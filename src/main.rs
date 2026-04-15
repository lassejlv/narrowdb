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
        bail!("usage: narrowdb exec <db-file> <sql> [--query-threads N]");
    }
    let query_threads = parse_query_threads_arg(&args[4..])?;
    let db = NarrowDb::open(
        &args[2],
        DbOptions {
            query_threads,
            ..DbOptions::default()
        },
    )?;
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
        bail!("usage: narrowdb bench <db-file> [rows] [--query-threads N]");
    }
    let (rows, query_threads) = parse_bench_args(args)?;
    run_benchmark(&args[2], rows, query_threads)
}

fn print_usage() {
    println!("narrowdb");
    println!("  exec  <db-file> <sql> [--query-threads N]");
    println!("  bench <db-file> [rows] [--query-threads N]");
}

fn parse_bench_args(args: &[String]) -> Result<(usize, Option<usize>)> {
    let mut rows = None;
    let mut query_threads = None;
    let mut index = 3;

    while index < args.len() {
        match args[index].as_str() {
            "--query-threads" => {
                index += 1;
                query_threads = Some(parse_query_threads_value(
                    args.get(index)
                        .context("missing value for --query-threads")?,
                )?);
            }
            value if rows.is_none() => {
                rows = Some(value.parse::<usize>().context("rows must be an integer")?);
            }
            other => bail!("unknown argument: {other}"),
        }
        index += 1;
    }

    Ok((rows.unwrap_or(1_000_000), query_threads))
}

fn parse_query_threads_arg(args: &[String]) -> Result<Option<usize>> {
    if args.is_empty() {
        return Ok(None);
    }
    if args.len() != 2 || args[0] != "--query-threads" {
        bail!("usage: narrowdb exec <db-file> <sql> [--query-threads N]");
    }
    Ok(Some(parse_query_threads_value(&args[1])?))
}

fn parse_query_threads_value(value: &str) -> Result<usize> {
    let threads = value
        .parse::<usize>()
        .context("query threads must be an integer")?;
    if threads == 0 {
        bail!("query threads must be greater than zero");
    }
    Ok(threads)
}
