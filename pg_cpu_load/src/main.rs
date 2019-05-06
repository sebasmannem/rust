extern crate postgres;
extern crate args;
extern crate getopts;

use postgres::{Connection, TlsMode};
use std::{env, process};
use getopts::Occur;
use args::Args;
use std::time::SystemTime;
use std::thread;
use std::sync::mpsc;
use std::str::FromStr;

const PROGRAM_DESC: &'static str = "generate cpu load on a Postgres cluster, and output the TPS.";
const PROGRAM_NAME: &'static str = "pg_cpu_load";

fn postgres_param(argument: &Result<String, args::ArgsError>, env_var_key: &String, default: &String) -> String {
    let mut return_val: String;
    match env::var(env_var_key) {
        Ok(val) => return_val = val,
        Err(_err) => return_val = default.to_string(),
    }
    if return_val.is_empty() {
        return_val = default.to_string()
    }
    match argument {
        Ok(val) => return_val = val.to_string(),
        Err(_err) => (),
    }
    return_val
}

fn postgres_connect_string(args: args::Args) -> String {
    let mut connect_string: String;
    let pgport = postgres_param(&args.value_of("port"), &"PGPORT".to_string(), &"5432".to_string());
    let pguser = postgres_param(&args.value_of("user"), &"PGUSER".to_string(), &"postgres".to_string());
    let pghost = postgres_param(&args.value_of("host"), &"PGHOST".to_string(), &"localhost".to_string());
    let pgpassword = postgres_param(&args.value_of("password"), &"PGPASSWORD".to_string(), &"".to_string());
    let pgdatabase = postgres_param(&args.value_of("dbname"), &"PGDATABASE".to_string(), &pguser);
//  postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]
    connect_string = "postgres://".to_string();
    if ! pguser.is_empty() {
        connect_string.push_str(&pguser);
        if ! pgpassword.is_empty() {
            connect_string.push_str(":");
            connect_string.push_str(&pgpassword);
        }
        connect_string.push_str("@");
    }
    connect_string.push_str(&pghost);
    if ! pgport.is_empty() {
        connect_string.push_str(":");
        connect_string.push_str(&pgport);
    }
    if ! pgdatabase.is_empty() {
        connect_string.push_str("/");
        connect_string.push_str(&pgdatabase);
    }
    connect_string
}

fn parse_args() -> Result<args::Args, args::ArgsError> {
    let input: Vec<String> = env::args().collect();
    let mut args = Args::new(PROGRAM_NAME, PROGRAM_DESC);
    args.flag("?", "help", "Print the usage menu");
    args.option("d",
        "dbname",
        "The database to connect to",
        "PGDATABASE",
        Occur::Optional,
        None);
    args.option("h",
        "host",
        "The hostname to connect to",
        "PGHOST",
        Occur::Optional,
        None);
    args.option("p",
        "port",
        "Postgres port to connect to",
        "PGPORT",
        Occur::Optional,
        None);
    args.option("P",
        "parallel",
        "How much threads to use",
        "THREADS",
        Occur::Optional,
        Some("10".to_string()));
    args.option("U",
        "user",
        "The user to use for the connection",
        "PGUSER",
        Occur::Optional,
        None);
    args.option("t",
        "query_type",
        "The type of query to run: empty, simple, read, write",
        "QTYPE",
        Occur::Optional,
        Some("simple".to_string()));
    args.option("s",
        "statement_type",
        "The type of statwement prep to use: direct, prepared, transactional, prepared_transactional",
        "STYPE",
        Occur::Optional,
        Some("direct".to_string()));
    args.parse(input)?;

    Ok(args)
}

fn thread(thread_id: u32, tx: mpsc::Sender<f32>) -> Result<(), Box<std::error::Error>>{
    println!("Thread {} started", thread_id);
    let args = parse_args()?;

    let qtype: String = args.value_of("query_type").unwrap();
    let query: String;
    match qtype.as_ref() {
        "empty" => query = "".to_string(),
        "simple" => query = "SELECT $1".to_string(),
        "read" => query = "SELECT ID from my_temp_table WHERE ID = $1".to_string(),
        "write" => query = "UPDATE my_temp_table set ID = $1 WHERE ID = $1".to_string(),
        _ => panic!("Option QTYPE should be one of empty, simple, read, write (not {}).", qtype),
    }

    let stype: String = args.value_of("statement_type")?;
    if stype != "prepared" && stype != "prepared_transactional" && stype != "transactional" && stype != "direct" {
        panic!("Option STYPE should be one of direct, prepared, transactional, prepared_transactional (not {}).", stype);
    }
    if qtype == "empty" && stype != "prepared_transactional" && stype != "transactional" {
        panic!("Option QTYPE-empty only works with transactions.");
    }

    let connect_string = postgres_connect_string(args);
    if thread_id == 0 {
        println!("Connectstring: {}", connect_string);
        println!("Query: {}", query);
        println!("SType: {}", stype);
    }
    let conn = Connection::connect(connect_string, TlsMode::None)?;
    let mut tps: u64 = 1000;

    if qtype == "read" || qtype == "write" {
        conn.execute("create temporary table my_temp_table (id oid)", &[])?;
        conn.execute("insert into my_temp_table values($1)", &[&thread_id])?;
    }

    loop {
        let start = SystemTime::now();    
        for _x in (1..tps).rev() {
            if stype == "prepared" {
                let prep = conn.prepare_cached(&query)?;
                let _row = prep.query(&[&thread_id]);
            } else if stype == "transactional" {
                let trans = conn.transaction()?;
                if qtype != "empty" {
                    let _row = trans.query(&query, &[&thread_id]);
                }
                let _res = trans.commit()?;
            } else if stype == "prepared_transactional" {
                let trans = conn.transaction()?;
                if qtype != "empty" {
                    let prep = trans.prepare_cached(&query)?;
                    let _row = prep.query(&[&thread_id]);
                }
                let _res = trans.commit()?;
            } else if qtype != "empty" {
                let _row = &conn.query(&query, &[&thread_id]);
            }
        }
        let end = SystemTime::now();
        let duration_nanos = end.duration_since(start)
            .expect("Time went backwards").as_nanos();
        let calc_tps = 10.0_f32.powi(9) * tps as f32 / duration_nanos as f32;
        tx.send(calc_tps)?;
        tps = calc_tps as u64;
    }
    //Ok(())
}

fn main() -> Result<(), Box<std::error::Error>> {
    let mut sum_tps: f32;
    let mut avg_tps: f32;
    let args = parse_args()?;
    let help = args.value_of("help")?;
    if help {
        println!("{}", args.full_usage());
        process::exit(0);
    }

    let num_threads: String = args.value_of("parallel")?;
    let num_threads = u32::from_str(&num_threads)?;

    let (tx, rx) = mpsc::channel();
    for thread_id in (0..num_threads).rev() {
        let thread_tx = tx.clone();
        thread::spawn(move || {
            thread(thread_id, thread_tx).unwrap();
        });
    }

    loop {
        sum_tps = 0_f32;
        for _thread_id in (0..num_threads).rev() {
             sum_tps += rx.recv()?;
        }
        avg_tps = sum_tps / num_threads as f32;
        println!("Average tps: {}", avg_tps);
        println!("Total tps: {}", sum_tps);
    }
    //Ok(())
}
