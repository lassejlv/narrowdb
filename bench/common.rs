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

pub const QUERIES: [(&str, &str); 3] = [
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
    let level = if i % 17 == 0 {
        LEVELS[2]
    } else if i % 7 == 0 {
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

pub fn row_count_from_args() -> usize {
    std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000)
}

pub fn timed<F: FnOnce() -> R, R>(f: F) -> (R, Duration) {
    let start = Instant::now();
    let result = f();
    (result, start.elapsed())
}

pub fn print_header(engine: &str, rows: usize) {
    println!("=== {engine} benchmark ({rows} rows) ===\n");
}

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

pub fn print_query(label: &str, elapsed: Duration, result: &QueryResult) {
    println!("{label}: {:?}", elapsed);
    if !result.columns.is_empty() {
        println!("  {}", result.columns.join(" | "));
        for row in result.rows.iter().take(5) {
            let line = row.iter().map(ToString::to_string).collect::<Vec<_>>().join(" | ");
            println!("  {line}");
        }
    }
    println!();
}

pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}
