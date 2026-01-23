//! TiDB Read Benchmark - Vector FTS Match

use clap::Parser;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rand::rngs::StdRng;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::{MySql, Pool};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const DEFAULT_DB_HOST: &str = "127.0.0.1";
const DEFAULT_DB_PORT: u16 = 4000;
const DEFAULT_DB_USER: &str = "root";
const DEFAULT_DB_NAME: &str = "test";

#[derive(Parser, Debug)]
#[command(author, version, about = "TiDB read benchmark for vector FTS using fts_match_word")]
struct Args {
    /// Number of concurrent workers
    #[arg(short, long, default_value_t = 16)]
    concurrency: usize,

    /// Benchmark duration in seconds
    #[arg(short, long, default_value_t = 60)]
    duration: u64,

    /// Sample size for vector queries
    #[arg(long, default_value_t = 2000)]
    sample_size: usize,

    /// Enable verbose logging (print individual SQLs)
    #[arg(long, default_value_t = false)]
    verbose: bool,

    /// Output file path for SQL queries log
    #[arg(long, default_value = "read_vector_bench_queries.sql")]
    output_file: String,

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

#[derive(Clone, Debug, sqlx::FromRow)]
struct SampleVector {
    vector: String,
}

struct ThreadStats {
    latencies: Vec<u128>,
    errors: usize,
}

impl ThreadStats {
    fn new() -> Self {
        Self {
            latencies: Vec::with_capacity(4096),
            errors: 0,
        }
    }

    fn merge(&mut self, other: ThreadStats) {
        self.latencies.extend(other.latencies);
        self.errors += other.errors;
    }
}

struct QueryLogger {
    writer: Arc<Mutex<BufWriter<File>>>,
}

impl QueryLogger {
    fn new(path: &str, table_name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        writeln!(writer, "-- TiDB Vector Read Benchmark SQL Queries")?;
        writeln!(writer, "-- Table: {}", table_name)?;
        writeln!(writer, "-- Generated at: {:?}", std::time::SystemTime::now())?;
        writeln!(writer, "")?;
        writer.flush()?;
        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    fn log_query(&self, sql: &str, duration: Duration) {
        if let Ok(mut writer) = self.writer.lock() {
            let _ = writeln!(
                writer,
                "-- TiDB Query (Execution time: {:.2}ms)",
                duration.as_secs_f64() * 1000.0
            );
            let _ = writeln!(writer, "{}\n", sql.trim());
        }
    }

    fn flush(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Ok(mut writer) = self.writer.lock() {
            writer.flush()?;
        }
        Ok(())
    }
}

fn escape_sql_string(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\'', "''")
        .replace('"', "\\\"")
        .replace('\0', "\\0")
}

fn build_vector_query(table_name: &str, vector_query: &str) -> String {
    let escaped = escape_sql_string(vector_query);
    format!(
        "SELECT id FROM `{}` WHERE fts_match_word('{}', vector)",
        table_name, escaped
    )
}

async fn find_latest_table(pool: &Pool<MySql>) -> Result<String, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT table_name FROM information_schema.tables
         WHERE table_schema = ? AND table_name LIKE 'wiki_paragraphs_embeddings_%'
         ORDER BY table_name DESC LIMIT 1",
    )
    .bind(DEFAULT_DB_NAME)
    .fetch_optional(pool)
    .await
    .map(|opt| opt.unwrap_or_default())
}

async fn fetch_sample_vectors(
    pool: &Pool<MySql>,
    table_name: &str,
    limit: usize,
) -> Result<Vec<SampleVector>, sqlx::Error> {
    let query = format!("SELECT vector FROM `{}` LIMIT {}", table_name, limit);
    sqlx::query_as(&query).fetch_all(pool).await
}

async fn run_worker(
    pool: Pool<MySql>,
    table_name: String,
    samples: Arc<Vec<SampleVector>>,
    logger: Arc<QueryLogger>,
    start_time: Instant,
    duration: Duration,
    verbose: bool,
) -> ThreadStats {
    let mut rng = StdRng::from_entropy();
    let mut stats = ThreadStats::new();

    while start_time.elapsed() < duration {
        let Some(sample) = samples.choose(&mut rng) else {
            continue;
        };

        let query = build_vector_query(&table_name, &sample.vector);
        let start = Instant::now();
        let result: Result<Vec<i64>, sqlx::Error> = sqlx::query_scalar(&query).fetch_all(&pool).await;
        let elapsed = start.elapsed();
        logger.log_query(&query, elapsed);

        match result {
            Ok(ids) => {
                stats.latencies.push(elapsed.as_micros());
                if verbose {
                    println!(
                        "Time: {:?} | Rows: {} | Vector size: {}",
                        elapsed.as_millis(),
                        ids.len(),
                        sample.vector.len()
                    );
                    println!("SQL: {}", query);
                }
            }
            Err(e) => {
                eprintln!("TiDB query error: {}", e);
                stats.errors += 1;
            }
        }
    }

    stats
}

fn print_percentiles(latencies: &mut [u128], elapsed: Duration) {
    if latencies.is_empty() {
        println!("vector: no samples");
        return;
    }
    latencies.sort_unstable();
    let len = latencies.len() as f64;
    let p50 = latencies[(len * 0.50) as usize] as f64 / 1000.0;
    let p95 = latencies[((len * 0.95) as usize).min(latencies.len() - 1)] as f64 / 1000.0;
    let p99 = latencies[((len * 0.99) as usize).min(latencies.len() - 1)] as f64 / 1000.0;
    let max = *latencies.last().unwrap() as f64 / 1000.0;
    let qps = latencies.len() as f64 / elapsed.as_secs_f64();
    println!(
        "vector qps={:.2} p50={:.2}ms p95={:.2}ms p99={:.2}ms max={:.2}ms",
        qps, p50, p95, p99, max
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("--- TiDB vector read benchmark (fts_match_word) ---");
    println!("concurrency    : {}", args.concurrency);
    println!("duration       : {}s", args.duration);
    println!("sample_size    : {}", args.sample_size);
    println!("output_file    : {}", args.output_file);

    let opts = MySqlConnectOptions::new()
        .host(&args.db_host)
        .port(args.db_port)
        .username(DEFAULT_DB_USER)
        .database(&args.db_name)
        .charset("utf8mb4");

    let pool = MySqlPoolOptions::new()
        .max_connections(args.concurrency as u32 + 8)
        .connect_with(opts)
        .await?;

    let table_name = find_latest_table(&pool).await?;
    if table_name.is_empty() {
        return Err("No table found matching pattern wiki_paragraphs_embeddings_*".into());
    }
    println!("using table: {}\n", table_name);

    println!("> Sampling {} vectors from database...", args.sample_size);
    let samples = fetch_sample_vectors(&pool, &table_name, args.sample_size).await?;
    if samples.is_empty() {
        return Err("Table is empty".into());
    }
    println!("> Successfully loaded {} samples\n", samples.len());

    let logger = Arc::new(QueryLogger::new(&args.output_file, &table_name)?);
    let samples = Arc::new(samples);

    println!("> Warm-up: running one vector query...");
    if let Some(sample) = samples.first() {
        let warmup_query = build_vector_query(&table_name, &sample.vector);
        let warmup_start = Instant::now();
        let warmup_result: Result<Vec<i64>, sqlx::Error> =
            sqlx::query_scalar(&warmup_query).fetch_all(&pool).await;
        let warmup_elapsed = warmup_start.elapsed();
        logger.log_query(&warmup_query, warmup_elapsed);

        match warmup_result {
            Ok(ids) => {
                println!(
                    "✓ Warm-up done: {:?} (rows={})",
                    warmup_elapsed, ids.len()
                );
            }
            Err(e) => {
                eprintln!("Warm-up query error: {}", e);
            }
        }
    }

    let start_time = Instant::now();
    let duration = Duration::from_secs(args.duration);

    let mut handles = Vec::with_capacity(args.concurrency);
    for _ in 0..args.concurrency {
        let pool = pool.clone();
        let table_name = table_name.clone();
        let samples = samples.clone();
        let logger = logger.clone();
        let verbose = args.verbose;

        let handle = tokio::spawn(async move {
            run_worker(pool, table_name, samples, logger, start_time, duration, verbose).await
        });
        handles.push(handle);
    }

    let mut combined = ThreadStats::new();
    for handle in handles {
        combined.merge(handle.await?);
    }
    let elapsed = start_time.elapsed();

    println!("\n=== Summary ===");
    println!("Elapsed        : {:.2?}", elapsed);
    println!("Total queries  : {}", combined.latencies.len());
    println!("Total errors   : {}", combined.errors);
    print_percentiles(&mut combined.latencies, elapsed);

    logger.flush()?;
    println!("\n✓ SQL queries have been saved to: {}", args.output_file);
    Ok(())
}
