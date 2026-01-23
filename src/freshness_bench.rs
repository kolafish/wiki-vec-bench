//! TiDB Freshness Benchmark - Write/Read lag with FTS

use clap::Parser;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::{MySql, Pool};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use chrono::Local;

const DEFAULT_DB_HOST: &str = "127.0.0.1";
const DEFAULT_DB_PORT: u16 = 4000;
const DEFAULT_DB_USER: &str = "root";
const DEFAULT_DB_NAME: &str = "test";

#[derive(Parser, Debug)]
#[command(author, version, about = "TiDB freshness benchmark using FULLTEXT on timestamp string")]
struct Args {
    /// Number of concurrent read workers
    #[arg(long, default_value_t = 8)]
    read_concurrency: usize,

    /// Number of concurrent write workers
    #[arg(long, default_value_t = 1)]
    write_concurrency: usize,

    /// Benchmark duration in seconds
    #[arg(short, long, default_value_t = 60)]
    duration: u64,

    /// Write interval in milliseconds
    #[arg(long, default_value_t = 10)]
    write_interval_ms: u64,

    /// Enable verbose logging
    #[arg(long, default_value_t = false)]
    verbose: bool,

    /// Database host
    #[arg(long, default_value = DEFAULT_DB_HOST)]
    db_host: String,

    /// Database port
    #[arg(long, default_value_t = DEFAULT_DB_PORT)]
    db_port: u16,

    /// Database name
    #[arg(long, default_value = DEFAULT_DB_NAME)]
    db_name: String,
}

#[derive(Clone)]
struct WriteRecord {
    ts_ms: i64,
    ts_text: String,
}

struct ReaderStats {
    lags_ms: Vec<u64>,
    read_lat_ms: Vec<u64>,
    errors: u64,
    misses: u64,
}

impl ReaderStats {
    fn new() -> Self {
        Self {
            lags_ms: Vec::with_capacity(4096),
            read_lat_ms: Vec::with_capacity(4096),
            errors: 0,
            misses: 0,
        }
    }

    fn merge(&mut self, other: ReaderStats) {
        self.lags_ms.extend(other.lags_ms);
        self.read_lat_ms.extend(other.read_lat_ms);
        self.errors += other.errors;
        self.misses += other.misses;
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as i64
}

fn escape_sql_string(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\'', "''")
        .replace('"', "\\\"")
        .replace('\0', "\\0")
}

fn build_read_query(table_name: &str, ts_text: &str) -> String {
    let escaped = escape_sql_string(ts_text);
    format!(
        "SELECT write_ts FROM `{}` WHERE fts_match_word('{}', write_ts_text) LIMIT 1",
        table_name, escaped
    )
}

async fn create_table_with_index(pool: &Pool<MySql>) -> Result<String, sqlx::Error> {
    let ts = Local::now().format("%Y%m%d%H%M%S").to_string();
    let table_name = format!("freshness_{}", ts);
    let create_sql = format!(
        r#"
        CREATE TABLE IF NOT EXISTS `{table}` (
          id1           BIGINT      NOT NULL,
          id2           INT         NOT NULL,
          write_ts      BIGINT      NOT NULL,
          write_ts_text VARCHAR(32) NOT NULL,
          PRIMARY KEY (id1, id2)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        "#,
        table = table_name
    );
    sqlx::query(&create_sql).execute(pool).await?;

    let index_sql = format!(
        r#"
        ALTER TABLE `{table}`
          ADD FULLTEXT INDEX ft_ts (write_ts_text) WITH PARSER standard;
        "#,
        table = table_name
    );
    if let Err(e) = sqlx::query(&index_sql).execute(pool).await {
        eprintln!("warning: failed to add FULLTEXT index: {}", e);
    }

    Ok(table_name)
}

async fn run_writer(
    pool: Pool<MySql>,
    table_name: String,
    queue: Arc<Mutex<VecDeque<WriteRecord>>>,
    id_counter: Arc<AtomicU64>,
    write_counter: Arc<AtomicU64>,
    start_time: Instant,
    duration: Duration,
    interval: Duration,
    verbose: bool,
) {
    let mut rng = StdRng::from_entropy();
    while start_time.elapsed() < duration {
        let id1 = id_counter.fetch_add(1, Ordering::Relaxed) as i64;
        let id2: i32 = rng.gen_range(1..1_000_000);
        let ts_ms = now_ms();
        let ts_text = ts_ms.to_string();

        let sql = format!(
            "INSERT INTO `{}` (id1, id2, write_ts, write_ts_text) VALUES (?, ?, ?, ?)",
            table_name
        );
        let result = sqlx::query(&sql)
            .bind(id1)
            .bind(id2)
            .bind(ts_ms)
            .bind(&ts_text)
            .execute(&pool)
            .await;

        match result {
            Ok(_) => {
                let record = WriteRecord { ts_ms, ts_text };
                let mut guard = queue.lock().await;
                guard.push_back(record);
                write_counter.fetch_add(1, Ordering::Relaxed);
                if verbose {
                    println!("write id1={} id2={} ts={}", id1, id2, ts_ms);
                }
            }
            Err(e) => {
                eprintln!("write error: {}", e);
            }
        }

        tokio::time::sleep(interval).await;
    }
}

async fn run_reader(
    pool: Pool<MySql>,
    table_name: String,
    queue: Arc<Mutex<VecDeque<WriteRecord>>>,
    start_time: Instant,
    duration: Duration,
    verbose: bool,
) -> ReaderStats {
    let mut stats = ReaderStats::new();
    while start_time.elapsed() < duration {
        let record = {
            let mut guard = queue.lock().await;
            guard.pop_front()
        };
        let Some(record) = record else {
            tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        };

        loop {
            if start_time.elapsed() >= duration {
                stats.misses += 1;
                break;
            }
            let query = build_read_query(&table_name, &record.ts_text);
            let read_start = Instant::now();
            let result: Result<Option<i64>, sqlx::Error> =
                sqlx::query_scalar(&query).fetch_optional(&pool).await;
            let read_elapsed = read_start.elapsed();
            match result {
                Ok(Some(write_ts)) => {
                    let lag = now_ms().saturating_sub(write_ts) as u64;
                    stats.lags_ms.push(lag);
                    stats.read_lat_ms.push(read_elapsed.as_millis() as u64);
                    if verbose {
                        println!(
                            "read ts={} lag={}ms read={}ms",
                            write_ts,
                            lag,
                            read_elapsed.as_millis()
                        );
                    }
                    break;
                }
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(e) => {
                    eprintln!("read error: {}", e);
                    stats.errors += 1;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }
    stats
}

fn percentile(values: &mut [u64], pct: f64) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let idx = ((values.len() as f64) * pct).ceil() as usize - 1;
    values.get(idx).copied()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("--- TiDB freshness benchmark (write/read lag) ---");
    println!("read_concurrency : {}", args.read_concurrency);
    println!("write_concurrency: {}", args.write_concurrency);
    println!("duration         : {}s", args.duration);
    println!("write_interval_ms: {}", args.write_interval_ms);

    let opts = MySqlConnectOptions::new()
        .host(&args.db_host)
        .port(args.db_port)
        .username(DEFAULT_DB_USER)
        .database(&args.db_name)
        .charset("utf8mb4");

    let pool = MySqlPoolOptions::new()
        .max_connections((args.read_concurrency + args.write_concurrency) as u32 + 8)
        .connect_with(opts)
        .await?;

    let table_name = create_table_with_index(&pool).await?;
    println!("using table: {}", table_name);

    let queue: Arc<Mutex<VecDeque<WriteRecord>>> = Arc::new(Mutex::new(VecDeque::new()));
    let id_counter = Arc::new(AtomicU64::new(1));
    let write_counter = Arc::new(AtomicU64::new(0));
    let start_time = Instant::now();
    let duration = Duration::from_secs(args.duration);
    let interval = Duration::from_millis(args.write_interval_ms);

    let mut writer_handles = Vec::with_capacity(args.write_concurrency);
    for _ in 0..args.write_concurrency {
        let pool = pool.clone();
        let table_name = table_name.clone();
        let queue = queue.clone();
        let id_counter = id_counter.clone();
        let write_counter = write_counter.clone();
        let verbose = args.verbose;
        let handle = tokio::spawn(async move {
            run_writer(
                pool,
                table_name,
                queue,
                id_counter,
                write_counter,
                start_time,
                duration,
                interval,
                verbose,
            )
            .await
        });
        writer_handles.push(handle);
    }

    let mut reader_handles = Vec::with_capacity(args.read_concurrency);
    for _ in 0..args.read_concurrency {
        let pool = pool.clone();
        let table_name = table_name.clone();
        let queue = queue.clone();
        let verbose = args.verbose;
        let handle = tokio::spawn(async move {
            run_reader(pool, table_name, queue, start_time, duration, verbose).await
        });
        reader_handles.push(handle);
    }

    for handle in writer_handles {
        let _ = handle.await;
    }

    let mut combined = ReaderStats::new();
    for handle in reader_handles {
        combined.merge(handle.await?);
    }

    let elapsed = start_time.elapsed();
    let total_reads = combined.lags_ms.len();
    let freshness_p95 = percentile(&mut combined.lags_ms, 0.95);
    let read_p95 = percentile(&mut combined.read_lat_ms, 0.95);
    let read_p99 = percentile(&mut combined.read_lat_ms, 0.99);
    let write_qps = write_counter.load(Ordering::Relaxed) as f64 / elapsed.as_secs_f64();

    println!();
    println!("=== Summary ===");
    println!("Elapsed      : {:.2?}", elapsed);
    println!("Write QPS    : {:.2}", write_qps);
    println!("Reads        : {}", total_reads);
    println!("Read errors  : {}", combined.errors);
    println!("Read misses  : {}", combined.misses);
    match read_p95 {
        Some(v) => println!("Read p95     : {} ms", v),
        None => println!("Read p95     : n/a"),
    }
    match read_p99 {
        Some(v) => println!("Read p99     : {} ms", v),
        None => println!("Read p99     : n/a"),
    }
    match freshness_p95 {
        Some(v) => println!("Freshness p95: {} ms", v),
        None => println!("Freshness p95: n/a"),
    }

    Ok(())
}
