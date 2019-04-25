extern crate postgres;
extern crate args;
extern crate getopts;

use postgres::{Connection, TlsMode};
use std::env;

use getopts::Occur;
use args::Args;
use std::process;

const PROGRAM_DESC: &'static str = "generate cpu load on a Postgres cluster, and output the TPS.";
const PROGRAM_NAME: &'static str = "pg_cpu_load";

struct Person {
    id: i32,
    name: String,
    data: Option<Vec<u8>>,
}

fn postgres_param(argument: &Result<String, args::ArgsError>, env_var_key: &String, default: &String) -> String {
    let argval: String;
    match argument {
        Ok(val) => argval = val.to_string(),
        Err(_err) => argval = "".to_string(),
    }
    if ! argval.is_empty() {
        return argval;
    } else {
        match env::var(env_var_key) {
            Ok(val) => return val,
            Err(_err) => return default.to_string(),
        }
    }
}

fn postgres_connect_string(args: args::Args) -> String {
    let pgport = postgres_param(&args.value_of("PGPORT"), &"PGPORT".to_string(), &"5432".to_string());
    let pguser = postgres_param(&args.value_of("PGUSER"), &"PGUSER".to_string(), &"postgres".to_string());
    let pghost = postgres_param(&args.value_of("PGHOST"), &"PGHOST".to_string(), &"localhost".to_string());
    let pgpassword = postgres_param(&args.value_of("PGPASSWORD"), &"PGPASSWORD".to_string(), &"".to_string());
    let pgdatabase = postgres_param(&args.value_of("PGDATABASE"), &"PGDATABASE".to_string(), &pguser);
    return format!("postgres://{user}:{password}@{host}:{port}/{database}", user=pguser, host=pghost, port=pgport, password=pgpassword, database=pgdatabase);
}

fn parse_args() -> Result<args::Args, args::ArgsError> {
    let input: Vec<String> = env::args().collect();
    let mut args = Args::new(PROGRAM_NAME, PROGRAM_DESC);
    args.flag("?", "help", "Print the usage menu");
    args.option("p",
        "port",
        "Postgres port to connect to",
        "PGPORT",
        Occur::Optional,
        None);
    args.option("h",
        "host",
        "The hostname to connect to",
        "PGHOST",
        Occur::Optional,
        None);
    args.option("U",
        "user",
        "The user to use for the connection",
        "PGUSER",
        Occur::Optional,
        None);
    args.option("d",
        "dbname",
        "The database to connect to",
        "PGDATABASE",
        Occur::Optional,
        None);

    args.parse(input)?;

    Ok(args)
}

fn main() -> Result<(), args::ArgsError>{
    let args = parse_args()?;
    let help = args.value_of("help")?;
    if help {
        args.full_usage();
        process::exit(0);
    }

    let connect_string = postgres_connect_string(args);
    println!("Connectstring: {}", connect_string);
    let conn = Connection::connect(connect_string, TlsMode::None).unwrap();
    conn.execute("CREATE TABLE person (
                    id              SERIAL PRIMARY KEY,
                    name            VARCHAR NOT NULL,
                    data            BYTEA
                  )", &[]).unwrap();
    let me = Person {
        id: 0,
        name: "Steven".to_string(),
        data: None,
    };
    conn.execute("INSERT INTO person (name, data) VALUES ($1, $2)",
                 &[&me.name, &me.data]).unwrap();
    for row in &conn.query("SELECT id, name, data FROM person", &[]).unwrap() {
        let person = Person {
            id: row.get(0),
            name: row.get(1),
            data: row.get(2),
        };
        println!("Found person {}", person.name);
    }
    Ok(())
}
