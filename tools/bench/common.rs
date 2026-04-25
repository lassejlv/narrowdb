use anyhow::{Context, Result, bail};
use std::time::{Duration, Instant};

pub const SERVICES: [&str; 8] = [
    "api", "worker", "auth", "billing", "search", "ingest", "cdn", "edge",
];
pub const HOSTS: [&str; 16] = [
    "host-a", "host-b", "host-c", "host-d", "host-e", "host-f", "host-g", "host-h", "host-i",
    "host-j", "host-k", "host-l", "host-m", "host-n", "host-o", "host-p",
];
pub const LEVELS: [&str; 4] = ["info", "warn", "error", "debug"];
pub const MESSAGES: [&str; 16] = [
    "request completed",
    "request retried",
    "cache miss",
    "cache warm",
    "upstream timeout",
    "upstream reset",
    "auth failed",
    "auth refreshed",
    "job queued",
    "job started",
    "job finished",
    "rate limited",
    "disk spill",
    "checkpoint written",
    "connection reused",
    "connection opened",
];

pub const QUERIES: [(&str, &str); 6] = [
    (
        "Errors by service (filter + group by)",
        "SELECT service, COUNT(*) AS errors FROM logs WHERE ts >= 1700500000 AND level = 'error' GROUP BY service ORDER BY errors DESC LIMIT 5;",
    ),
    (
        "Avg duration for 5xx (filter + group by + avg)",
        "SELECT host, AVG(duration) AS avg_ms FROM logs WHERE status >= 500 GROUP BY host ORDER BY avg_ms DESC LIMIT 10;",
    ),
    (
        "Slow request count (filter + count)",
        "SELECT COUNT(*) AS slow_requests FROM logs WHERE duration >= 700.0;",
    ),
    (
        "Multi-filter scan (projection-heavy)",
        "SELECT ts, service, host, status FROM logs WHERE level = 'error' AND status >= 503 AND duration >= 700.0 ORDER BY ts DESC LIMIT 25;",
    ),
    (
        "High-cardinality grouped count",
        "SELECT request_id, COUNT(*) AS total FROM logs WHERE status >= 500 GROUP BY request_id ORDER BY total DESC LIMIT 25;",
    ),
    (
        "Cross-row-group string group by",
        "SELECT message, COUNT(*) AS total FROM logs WHERE bytes >= 4096 GROUP BY message ORDER BY total DESC LIMIT 10;",
    ),
];

pub struct Row {
    pub ts: i64,
    pub level: &'static str,
    pub service: &'static str,
    pub host: &'static str,
    pub request_id: i64,
    pub status: i64,
    pub duration: f64,
    pub bytes: i64,
    pub message: &'static str,
}

pub fn generate_row(i: usize) -> Row {
    let level = if i.is_multiple_of(17) {
        LEVELS[2]
    } else if i.is_multiple_of(7) {
        LEVELS[1]
    } else {
        LEVELS[0]
    };
    let status = if level == "error" {
        500 + (i % 8) as i64
    } else {
        200 + (i % 5) as i64
    };
    Row {
        ts: 1_700_000_000 + i as i64,
        level,
        service: SERVICES[i % SERVICES.len()],
        host: HOSTS[(i / 11) % HOSTS.len()],
        request_id: i as i64,
        status,
        duration: 4.0 + ((i % 1000) as f64 * 1.37),
        bytes: 256 + ((i * 31) % 65_536) as i64,
        message: MESSAGES[(i / 97) % MESSAGES.len()],
    }
}

pub struct BenchmarkArgs {
    pub rows: usize,
    pub query_threads: Option<usize>,
    #[allow(dead_code)]
    pub repeat: usize,
}

pub fn benchmark_args() -> Result<BenchmarkArgs> {
    parse_benchmark_args(std::env::args().skip(1))
}

pub fn resolved_query_threads(query_threads: Option<usize>) -> usize {
    query_threads.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|threads| threads.get())
            .unwrap_or(1)
    })
}

pub fn timed<F: FnOnce() -> R, R>(f: F) -> (R, Duration) {
    let start = Instant::now();
    let result = f();
    (result, start.elapsed())
}

#[allow(dead_code)]
pub fn print_header(engine: &str, rows: usize) {
    println!("=== {engine} benchmark ({rows} rows) ===\n");
}

#[allow(dead_code)]
pub fn print_ingest(elapsed: Duration, rows: usize, file_size: u64) {
    println!("Ingest: {:?}", elapsed);
    println!(
        "  throughput: {:.0} rows/sec",
        rows as f64 / elapsed.as_secs_f64()
    );
    println!("  file size:  {:.2} MiB", file_size as f64 / 1_048_576.0);
    println!("  bytes/row:  {:.1}", file_size as f64 / rows as f64);
    println!();
}

#[allow(dead_code)]
pub fn print_query(label: &str, elapsed: Duration, result: &QueryResult) {
    println!("{label}: {:?}", elapsed);
    if !result.columns.is_empty() {
        println!("  {}", result.columns.join(" | "));
        for row in result.rows.iter().take(5) {
            let line = row
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(" | ");
            println!("  {line}");
        }
    }
    println!();
}

#[allow(dead_code)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

#[allow(dead_code)]
pub struct QueryMetric {
    pub label: &'static str,
    pub elapsed: Duration,
}

#[allow(dead_code)]
pub struct BenchmarkReport {
    pub engine: &'static str,
    pub rows: usize,
    pub query_threads: usize,
    pub ingest_elapsed: Duration,
    pub file_size: u64,
    pub queries: Vec<QueryMetric>,
}

#[allow(dead_code)]
impl BenchmarkReport {
    pub fn ingest_throughput(&self) -> f64 {
        self.rows as f64 / self.ingest_elapsed.as_secs_f64()
    }

    pub fn bytes_per_row(&self) -> f64 {
        self.file_size as f64 / self.rows as f64
    }
}

#[allow(dead_code)]
pub fn print_comparison(narrow: &BenchmarkReport, duckdb: &BenchmarkReport) {
    println!("{} vs {}", narrow.engine, duckdb.engine);
    println!(
        "rows={} | query_threads={}",
        narrow.rows, narrow.query_threads
    );
    println!();

    let summary_rows = vec![
        comparison_row(
            "Ingest throughput",
            format!("{:.0} rows/s", narrow.ingest_throughput()),
            format!("{:.0} rows/s", duckdb.ingest_throughput()),
            compare_higher_better(
                narrow.ingest_throughput(),
                duckdb.ingest_throughput(),
                "higher",
            ),
        ),
        comparison_row(
            "File size",
            format!("{:.2} MiB", narrow.file_size as f64 / 1_048_576.0),
            format!("{:.2} MiB", duckdb.file_size as f64 / 1_048_576.0),
            compare_lower_better(narrow.file_size as f64, duckdb.file_size as f64, "smaller"),
        ),
        comparison_row(
            "Bytes per row",
            format!("{:.2}", narrow.bytes_per_row()),
            format!("{:.2}", duckdb.bytes_per_row()),
            compare_lower_better(narrow.bytes_per_row(), duckdb.bytes_per_row(), "smaller"),
        ),
    ];
    println!(
        "{}",
        render_table(
            ["Metric", "NarrowDB", "DuckDB", "Winner", "Delta"],
            summary_rows
        )
    );
    println!();

    let query_rows = narrow
        .queries
        .iter()
        .zip(&duckdb.queries)
        .map(|(lhs, rhs)| {
            comparison_row(
                lhs.label,
                format_duration(lhs.elapsed),
                format_duration(rhs.elapsed),
                compare_lower_better(
                    lhs.elapsed.as_secs_f64(),
                    rhs.elapsed.as_secs_f64(),
                    "faster",
                ),
            )
        })
        .collect::<Vec<_>>();
    println!(
        "{}",
        render_table(
            ["Query", "NarrowDB", "DuckDB", "Winner", "Delta"],
            query_rows
        )
    );
}

#[allow(dead_code)]
pub fn median_report(reports: &[BenchmarkReport]) -> Result<BenchmarkReport> {
    let first = reports.first().context("no benchmark reports collected")?;
    let mut ingest_samples = reports
        .iter()
        .map(|report| report.ingest_elapsed)
        .collect::<Vec<_>>();
    ingest_samples.sort_unstable();

    let mut file_sizes = reports
        .iter()
        .map(|report| report.file_size)
        .collect::<Vec<_>>();
    file_sizes.sort_unstable();

    let mut query_elapsed = Vec::with_capacity(first.queries.len());
    for query_index in 0..first.queries.len() {
        let mut samples = reports
            .iter()
            .map(|report| report.queries[query_index].elapsed)
            .collect::<Vec<_>>();
        samples.sort_unstable();
        query_elapsed.push(QueryMetric {
            label: first.queries[query_index].label,
            elapsed: median_duration(&samples),
        });
    }

    Ok(BenchmarkReport {
        engine: first.engine,
        rows: first.rows,
        query_threads: first.query_threads,
        ingest_elapsed: median_duration(&ingest_samples),
        file_size: file_sizes[file_sizes.len() / 2],
        queries: query_elapsed,
    })
}

#[allow(dead_code)]
fn comparison_row(
    metric: impl Into<String>,
    narrow: impl Into<String>,
    duckdb: impl Into<String>,
    verdict: (String, String),
) -> Vec<String> {
    vec![
        metric.into(),
        narrow.into(),
        duckdb.into(),
        verdict.0,
        verdict.1,
    ]
}

#[allow(dead_code)]
fn compare_higher_better(lhs: f64, rhs: f64, suffix: &str) -> (String, String) {
    if approx_equal(lhs, rhs) {
        return ("Tie".to_string(), "-".to_string());
    }
    if lhs > rhs {
        (
            "NarrowDB".to_string(),
            format!("{:.2}x {suffix}", lhs / rhs),
        )
    } else {
        ("DuckDB".to_string(), format!("{:.2}x {suffix}", rhs / lhs))
    }
}

#[allow(dead_code)]
fn compare_lower_better(lhs: f64, rhs: f64, suffix: &str) -> (String, String) {
    if approx_equal(lhs, rhs) {
        return ("Tie".to_string(), "-".to_string());
    }
    if lhs < rhs {
        (
            "NarrowDB".to_string(),
            format!("{:.2}x {suffix}", rhs / lhs),
        )
    } else {
        ("DuckDB".to_string(), format!("{:.2}x {suffix}", lhs / rhs))
    }
}

#[allow(dead_code)]
fn approx_equal(lhs: f64, rhs: f64) -> bool {
    (lhs - rhs).abs() < 1e-9
}

#[allow(dead_code)]
fn format_duration(duration: Duration) -> String {
    if duration.as_secs() >= 1 {
        format!("{:.3}s", duration.as_secs_f64())
    } else if duration.as_millis() >= 1 {
        format!("{:.3}ms", duration.as_secs_f64() * 1_000.0)
    } else {
        format!("{:.3}us", duration.as_secs_f64() * 1_000_000.0)
    }
}

#[allow(dead_code)]
fn render_table<const N: usize>(headers: [&str; N], rows: Vec<Vec<String>>) -> String {
    let mut widths = headers.map(str::len);
    for row in &rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(cell.len());
        }
    }

    let separator = {
        let mut line = String::from("+");
        for width in widths {
            line.push_str(&"-".repeat(width + 2));
            line.push('+');
        }
        line
    };

    let mut out = String::new();
    out.push_str(&separator);
    out.push('\n');
    out.push_str(&render_row(
        &headers.map(|header| header.to_string()),
        &widths,
    ));
    out.push('\n');
    out.push_str(&separator);
    out.push('\n');
    for row in rows {
        out.push_str(&render_row(&row, &widths));
        out.push('\n');
    }
    out.push_str(&separator);
    out
}

#[allow(dead_code)]
fn render_row(row: &[String], widths: &[usize]) -> String {
    let mut line = String::from("|");
    for (cell, width) in row.iter().zip(widths.iter()) {
        line.push(' ');
        line.push_str(&format!("{cell:<width$}", width = *width));
        line.push(' ');
        line.push('|');
    }
    line
}

fn parse_benchmark_args<I>(args: I) -> Result<BenchmarkArgs>
where
    I: IntoIterator<Item = String>,
{
    let mut rows = None;
    let mut query_threads = None;
    let mut repeat = 1_usize;
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--query-threads" => {
                let value = args.next().context("missing value for --query-threads")?;
                let threads = value
                    .parse::<usize>()
                    .context("query threads must be an integer")?;
                if threads == 0 {
                    bail!("query threads must be greater than zero");
                }
                query_threads = Some(threads);
            }
            "--repeat" => {
                let value = args.next().context("missing value for --repeat")?;
                repeat = value
                    .parse::<usize>()
                    .context("repeat must be an integer")?;
                if repeat == 0 {
                    bail!("repeat must be greater than zero");
                }
            }
            value if rows.is_none() => {
                rows = Some(value.parse::<usize>().context("rows must be an integer")?);
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(BenchmarkArgs {
        rows: rows.unwrap_or(1_000_000),
        query_threads,
        repeat,
    })
}

#[allow(dead_code)]
fn median_duration(samples: &[Duration]) -> Duration {
    samples[samples.len() / 2]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_repeat_and_query_threads() {
        let args = parse_benchmark_args([
            "1000".to_string(),
            "--query-threads".to_string(),
            "8".to_string(),
            "--repeat".to_string(),
            "7".to_string(),
        ])
        .expect("benchmark args should parse");

        assert_eq!(args.rows, 1000);
        assert_eq!(args.query_threads, Some(8));
        assert_eq!(args.repeat, 7);
    }

    #[test]
    fn rejects_zero_repeat() {
        let result = parse_benchmark_args(["--repeat".to_string(), "0".to_string()]);
        assert!(result.is_err());
    }
}
