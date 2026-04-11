use anyhow::{Context, Result, bail};

use narrowdb::bench::run_benchmark;
use narrowdb::{DbOptions, NarrowDb};

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 2 {
        print_usage();
        return Ok(());
    }

    match args[1].as_str() {
        "exec" => exec_command(&args),
        "bench" => bench_command(&args),
        _ => {
            print_usage();
            Ok(())
        }
    }
}

fn exec_command(args: &[String]) -> Result<()> {
    if args.len() < 4 {
        bail!("usage: narrowdb exec <db-file> <sql>");
    }
    let db = NarrowDb::open(&args[2], DbOptions::default())?;
    let results = db.execute_sql(&args[3])?;
    for result in results {
        if result.columns.is_empty() {
            continue;
        }
        println!("{}", result.columns.join(" | "));
        for row in result.rows {
            println!(
                "{}",
                row.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" | ")
            );
        }
    }
    Ok(())
}

fn bench_command(args: &[String]) -> Result<()> {
    if args.len() < 3 {
        bail!("usage: narrowdb bench <db-file> [rows]");
    }
    let rows = args
        .get(3)
        .map(|value| value.parse::<usize>().context("rows must be an integer"))
        .transpose()?
        .unwrap_or(1_000_000);
    run_benchmark(&args[2], rows)
}

fn print_usage() {
    println!("narrowdb");
    println!("  exec  <db-file> <sql>");
    println!("  bench <db-file> [rows]");
}
