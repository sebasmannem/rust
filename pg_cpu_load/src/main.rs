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

    args.parse(input)?;

    Ok(args)
}

fn thread(thread_id: u32, connect_string: String, tx: mpsc::Sender<f32>) -> Result<(), args::ArgsError>{
    println!("Thread {} started", thread_id);
    let conn = Connection::connect(connect_string, TlsMode::None).unwrap();
    let mut tps: u64 = 1000;
    loop {
        let start = SystemTime::now();    
        for _x in (1..tps).rev() {
            let _row = &conn.query("SELECT 1", &[]).unwrap();
        }
        let end = SystemTime::now();    
        let duration_nanos = end.duration_since(start)
            .expect("Time went backwards").as_nanos();
        let calc_tps = 10.0_f32.powi(9) * tps as f32 / duration_nanos as f32;
        tx.send(calc_tps).unwrap();
        tps = calc_tps as u64;
    }
    //Ok(())
}

fn main() -> Result<(), args::ArgsError>{
    let mut sum_tps: f32;
    let mut avg_tps: f32;
    let args = parse_args()?;
    let help = args.value_of("help")?;
    if help {
        println!("{}", args.full_usage());
        process::exit(0);
    }

    let num_threads: String = args.value_of("parallel").unwrap();
    let num_threads = u32::from_str(&num_threads).unwrap();

    let connect_string = postgres_connect_string(args);
    println!("Connectstring: {}", connect_string);

    let (tx, rx) = mpsc::channel();
    for thread_id in (0..num_threads).rev() {
        let thread_tx = tx.clone();
        let thread_connstr = connect_string.clone();
        thread::spawn(move || {
            thread(thread_id, thread_connstr, thread_tx).unwrap();
        });
    }

    loop {
        sum_tps = 0_f32;
        for _thread_id in (0..num_threads).rev() {
             sum_tps += rx.recv().unwrap();
        }
        avg_tps = sum_tps / num_threads as f32;
        println!("Average tps: {}", avg_tps);
    }
    //Ok(())
}
