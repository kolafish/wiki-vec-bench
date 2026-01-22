//! TiDB Read Benchmark - Comparing TiDB vs TiFlash Performance
//!
//! This refactored version improves code organization, reduces duplication,
//! and enhances maintainability.

use clap::Parser;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use sqlx::mysql::{MySqlPoolOptions, MySqlConnectOptions};
use sqlx::{Pool, MySql};
use std::fs::File;
use std::io::{self, Write, BufWriter};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_DB_HOST: &str = "127.0.0.1";
const DEFAULT_DB_PORT: u16 = 4000;
const DEFAULT_DB_USER: &str = "root";
const DEFAULT_DB_NAME: &str = "test";
const TIFLASH_SYNC_TIMEOUT_SECS: u64 = 300;
const TIFLASH_CHECK_INTERVAL_SECS: u64 = 2;

// ============================================================================
// CLI Arguments
// ============================================================================

#[derive(Parser, Debug)]
#[command(author, version, about = "TiDB read benchmark for wiki_paragraphs_embeddings using fts_match_word")]
struct Args {
    /// Number of concurrent workers
    #[arg(short, long, default_value_t = 16)]
    concurrency: usize,

    /// Benchmark duration in seconds
    #[arg(short, long, default_value_t = 60)]
    duration: u64,

    /// Sample size for test data
    #[arg(long, default_value_t = 2000)]
    sample_size: usize,

    /// Enable verbose logging (print individual SQLs)
    #[arg(long, default_value_t = false)]
    verbose: bool,

    /// Output file path for SQL queries log
    #[arg(long, default_value = "read_bench_queries.sql")]
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

// ============================================================================
// Data Structures
// ============================================================================

struct ThreadStats {
    tidb_latencies: Vec<u128>,
    tiflash_latencies: Vec<u128>,
    errors: usize,
}

impl ThreadStats {
    fn new() -> Self {
        Self {
            tidb_latencies: Vec::with_capacity(4096),
            tiflash_latencies: Vec::with_capacity(4096),
            errors: 0,
        }
    }

    fn merge(&mut self, other: ThreadStats) {
        self.tidb_latencies.extend(other.tidb_latencies);
        self.tiflash_latencies.extend(other.tiflash_latencies);
        self.errors += other.errors;
    }
}

#[derive(Clone, Debug, sqlx::FromRow)]
struct SampleData {
    title: String,
    text: String,
}

#[derive(Clone, Copy, Debug)]
enum QueryEngine {
    TiDB,
    TiFlash,
}

impl QueryEngine {
    fn name(&self) -> &str {
        match self {
            QueryEngine::TiDB => "TiDB",
            QueryEngine::TiFlash => "TiFlash",
        }
    }
}

// ============================================================================
// Query Logger (Buffered writes to reduce lock contention)
// ============================================================================

struct QueryLogger {
    writer: Arc<Mutex<BufWriter<File>>>,
}

impl QueryLogger {
    fn new(path: &str, table_name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        
        // Write header
        writeln!(writer, "-- TiDB Read Benchmark SQL Queries")?;
        writeln!(writer, "-- Table: {}", table_name)?;
        writeln!(writer, "-- Generated at: {:?}", std::time::SystemTime::now())?;
        writeln!(writer, "")?;
        writer.flush()?;
        
        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    fn log_query(&self, engine: QueryEngine, sql: &str) {
        if let Ok(mut writer) = self.writer.lock() {
            let _ = writeln!(writer, "-- {} Query", engine.name());
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

// ============================================================================
// SQL Query Builder
// ============================================================================

struct QueryBuilder {
    table_name: String,
}

impl QueryBuilder {
    fn new(table_name: String) -> Self {
        Self { table_name }
    }

    /// Build FTS query for specific engine
    fn build_fts_query(&self, search_word: &str, engine: QueryEngine) -> String {
        let escaped_word = Self::escape_sql_string(search_word);
        
        let hint = match engine {
            QueryEngine::TiDB => String::new(),
            QueryEngine::TiFlash => format!("/*+ READ_FROM_STORAGE(TIFLASH[`{}`]) */ ", self.table_name),
        };

        format!(
            "SELECT {}count(*) FROM `{}` WHERE fts_match_word('{}', text) OR fts_match_word('{}', title)",
            hint, self.table_name, escaped_word, escaped_word
        )
    }

    /// Escape SQL string to prevent injection
    fn escape_sql_string(s: &str) -> String {
        s.replace('\\', "\\\\")
         .replace('\'', "''")
         .replace('"', "\\\"")
         .replace('\0', "\\0")
    }
}

// ============================================================================
// Query Executor
// ============================================================================

struct QueryExecutor {
    pool: Pool<MySql>,
    query_builder: QueryBuilder,
    logger: Arc<QueryLogger>,
}

impl QueryExecutor {
    fn new(pool: Pool<MySql>, table_name: String, logger: Arc<QueryLogger>) -> Self {
        Self {
            pool,
            query_builder: QueryBuilder::new(table_name),
            logger,
        }
    }

    async fn execute_query(
        &self,
        search_word: &str,
        engine: QueryEngine,
        verbose: bool,
    ) -> Result<Duration, sqlx::Error> {
        let query = self.query_builder.build_fts_query(search_word, engine);
        
        // Log to file
        self.logger.log_query(engine, &query);
        
        let start = Instant::now();
        let count: i64 = sqlx::query_scalar(&query)
            .fetch_one(&self.pool)
            .await?;
        let duration = start.elapsed();

        if verbose {
            println!("---------------------------------------------------");
            println!("[{}] Time: {:?} | Count: {} | Word: '{}'", 
                engine.name(), duration.as_millis(), count, search_word);
            println!("SQL: {}", query);
        }

        Ok(duration)
    }
}

// ============================================================================
// TiFlash Replica Manager
// ============================================================================

#[derive(sqlx::FromRow)]
struct ReplicaInfo {
    replica_count: Option<i64>,
    available: Option<bool>,
    progress: Option<f64>,
}

impl ReplicaInfo {
    fn is_ready(&self) -> bool {
        self.replica_count.map_or(false, |c| c > 0)
            && self.available.unwrap_or(false)
            && self.progress.map_or(false, |p| p >= 1.0)
    }

    fn needs_wait(&self) -> bool {
        self.replica_count.map_or(false, |c| c > 0) && !self.is_ready()
    }
}

struct TiFlashReplicaManager {
    pool: Pool<MySql>,
}

impl TiFlashReplicaManager {
    fn new(pool: Pool<MySql>) -> Self {
        Self { pool }
    }

    async fn ensure_replica(&self, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let info = self.get_replica_info(table_name).await?;

        match info {
            Some(info) if info.is_ready() => {
                println!("✓ TiFlash replica exists and is synced for table: {} (progress: {:.2}%)",
                    table_name, info.progress.unwrap_or(1.0) * 100.0);
                Ok(())
            }
            Some(info) if info.needs_wait() => {
                println!("⏳ TiFlash replica exists but not fully synced (progress: {:.2}%), waiting...",
                    info.progress.unwrap_or(0.0) * 100.0);
                self.wait_for_sync(table_name).await
            }
            _ => {
                self.create_and_wait(table_name).await
            }
        }
    }

    async fn get_replica_info(&self, table_name: &str) -> Result<Option<ReplicaInfo>, sqlx::Error> {
        sqlx::query_as(
            "SELECT replica_count, available, progress
             FROM information_schema.tiflash_replica
             WHERE table_schema = ? AND table_name = ?"
        )
        .bind(DEFAULT_DB_NAME)
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await
    }

    async fn create_and_wait(&self, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        println!("> Creating TiFlash replica for table: {}", table_name);
        
        let alter_sql = format!("ALTER TABLE `{}` SET TIFLASH REPLICA 1", table_name);
        sqlx::query(&alter_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("Failed to create TiFlash replica: {}", e))?;
        
        println!("✓ TiFlash replica creation command executed successfully");
        println!("> Waiting for TiFlash replica to sync...");
        
        self.wait_for_sync(table_name).await
    }

    async fn wait_for_sync(&self, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();
        let timeout = Duration::from_secs(TIFLASH_SYNC_TIMEOUT_SECS);
        let interval = Duration::from_secs(TIFLASH_CHECK_INTERVAL_SECS);

        loop {
            if start.elapsed() > timeout {
                return Err(format!(
                    "Timeout: TiFlash replica for '{}' did not sync within {} seconds",
                    table_name, TIFLASH_SYNC_TIMEOUT_SECS
                ).into());
            }

            let info = self.get_replica_info(table_name).await?;
            
            if let Some(info) = info {
                if info.is_ready() {
                    println!("\n✓ TiFlash replica is fully synced (progress: {:.2}%)",
                        info.progress.unwrap_or(1.0) * 100.0);
                    return Ok(());
                }
                
                if let Some(progress) = info.progress {
                    print!("\r⏳ Syncing TiFlash replica... {:.1}%", progress * 100.0);
                } else {
                    print!("\r⏳ Waiting for TiFlash replica...");
                }
            } else {
                print!("\r⏳ Waiting for TiFlash replica to be created...");
            }
            
            io::stdout().flush().ok();
            tokio::time::sleep(interval).await;
        }
    }

    async fn verify_query(&self, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Set isolation engines
        sqlx::query("SET SESSION tidb_isolation_read_engines = 'tikv,tiflash'")
            .execute(&self.pool)
            .await?;

        // Test query
        let explain_query = format!(
            "EXPLAIN SELECT /*+ READ_FROM_STORAGE(TIFLASH[`{}`]) */ count(*)
             FROM `{}`
             WHERE fts_match_word('test', title) OR fts_match_word('test', text)",
            table_name, table_name
        );

        #[derive(sqlx::FromRow)]
        struct ExplainRow {
            task: String,
        }

        let rows: Vec<ExplainRow> = sqlx::query_as(&explain_query)
            .fetch_all(&self.pool)
            .await?;

        let found_tiflash = rows.iter().any(|r| r.task.contains("tiflash"));

        if found_tiflash {
            println!("✓ Query plan verification: TiFlash will be used");
        } else {
            eprintln!("⚠ Warning: EXPLAIN plan does not show TiFlash usage");
        }

        Ok(())
    }
}

// ============================================================================
// Statistics Utilities
// ============================================================================

fn calculate_percentiles(latencies: &mut [u128]) -> (f64, f64, f64, f64) {
    if latencies.is_empty() {
        return (0.0, 0.0, 0.0, 0.0);
    }
    
    latencies.sort_unstable();
    let len = latencies.len();
    
    let p50 = latencies[len / 2] as f64 / 1000.0;
    let p95 = latencies[(len * 95 / 100).min(len - 1)] as f64 / 1000.0;
    let p99 = latencies[(len * 99 / 100).min(len - 1)] as f64 / 1000.0;
    let max = latencies[len - 1] as f64 / 1000.0;
    
    (p50, p95, p99, max)
}

fn print_statistics(name: &str, latencies: &mut [u128], elapsed: Duration) {
    if latencies.is_empty() {
        println!("{}: no samples", name);
        return;
    }

    let qps = latencies.len() as f64 / elapsed.as_secs_f64();
    let (p50, p95, p99, max) = calculate_percentiles(latencies);

    println!("--- {} Statistics ---", name);
    println!("Total queries  : {}", latencies.len());
    println!("QPS            : {:.2}", qps);
    println!("{:<10} qps={:.2} p50={:.2}ms p95={:.2}ms p99={:.2}ms max={:.2}ms",
        name, qps, p50, p95, p99, max);
}

// ============================================================================
// Utility Functions
// ============================================================================

async fn find_latest_table(pool: &Pool<MySql>) -> Result<String, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT table_name FROM information_schema.tables
         WHERE table_schema = ? AND table_name LIKE 'wiki_paragraphs_embeddings_%'
         ORDER BY table_name DESC LIMIT 1"
    )
    .bind(DEFAULT_DB_NAME)
    .fetch_optional(pool)
    .await
    .map(|opt| opt.unwrap_or_default())
}

async fn fetch_sample_data(
    pool: &Pool<MySql>,
    table_name: &str,
    limit: usize,
) -> Result<Vec<SampleData>, sqlx::Error> {
    let query = format!("SELECT title, text FROM `{}` LIMIT {}", table_name, limit);
    sqlx::query_as(&query).fetch_all(pool).await
}

fn extract_search_word(rng: &mut impl Rng, sample_data: &[SampleData]) -> Option<String> {
    let sample = sample_data.choose(rng)?;
    let words: Vec<&str> = sample.text.split_whitespace().collect();
    if words.is_empty() {
        return None;
    }
    Some(words[rng.gen_range(0..words.len())].to_string())
}

// ============================================================================
// Benchmark Runner
// ============================================================================

async fn run_benchmark_worker(
    executor: Arc<QueryExecutor>,
    sample_data: Arc<Vec<SampleData>>,
    start_time: Instant,
    duration: Duration,
    verbose: bool,
) -> ThreadStats {
    let mut rng = StdRng::from_entropy();
    let mut stats = ThreadStats::new();

    while start_time.elapsed() < duration {
        let Some(search_word) = extract_search_word(&mut rng, &sample_data) else {
            continue;
        };

        // Run on TiDB
        match executor.execute_query(&search_word, QueryEngine::TiDB, verbose).await {
            Ok(dur) => stats.tidb_latencies.push(dur.as_micros()),
            Err(e) => {
                eprintln!("TiDB query error: {}", e);
                stats.errors += 1;
            }
        }

        // Run on TiFlash
        match executor.execute_query(&search_word, QueryEngine::TiFlash, verbose).await {
            Ok(dur) => stats.tiflash_latencies.push(dur.as_micros()),
            Err(e) => {
                eprintln!("TiFlash query error: {}", e);
                stats.errors += 1;
            }
        }
    }

    stats
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("--- TiDB read benchmark for wiki_paragraphs_embeddings ---");
    println!("concurrency    : {}", args.concurrency);
    println!("duration       : {}s", args.duration);
    println!("sample_size    : {}", args.sample_size);
    println!("output_file    : {}", args.output_file);

    // Connect to database
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

    // Find table
    let table_name = find_latest_table(&pool).await?;
    if table_name.is_empty() {
        return Err("No table found matching pattern wiki_paragraphs_embeddings_*".into());
    }
    println!("using table: {}\n", table_name);

    // Initialize TiFlash replica manager
    let replica_mgr = TiFlashReplicaManager::new(pool.clone());
    replica_mgr.ensure_replica(&table_name).await?;
    replica_mgr.verify_query(&table_name).await?;

    // Load sample data
    println!("> Sampling {} rows from database...", args.sample_size);
    let sample_data = fetch_sample_data(&pool, &table_name, args.sample_size).await?;
    if sample_data.is_empty() {
        return Err("Table is empty".into());
    }
    println!("> Successfully loaded {} samples\n", sample_data.len());

    // Initialize query logger and executor
    let logger = Arc::new(QueryLogger::new(&args.output_file, &table_name)?);
    let executor = Arc::new(QueryExecutor::new(pool.clone(), table_name.clone(), logger.clone()));
    let sample_data = Arc::new(sample_data);

    // Run benchmark
    let start_time = Instant::now();
    let duration = Duration::from_secs(args.duration);
    let mut handles = Vec::with_capacity(args.concurrency);

    for _ in 0..args.concurrency {
        let executor = executor.clone();
        let sample_data = sample_data.clone();
        let verbose = args.verbose;

        let handle = tokio::spawn(async move {
            run_benchmark_worker(executor, sample_data, start_time, duration, verbose).await
        });

        handles.push(handle);
    }

    // Collect results
    let mut combined_stats = ThreadStats::new();
    for handle in handles {
        combined_stats.merge(handle.await?);
    }

    let elapsed = start_time.elapsed();

    // Print results
    println!("\n=== Summary ===");
    println!("Elapsed        : {:.2?}", elapsed);
    println!("Total errors   : {}\n", combined_stats.errors);

    print_statistics("TiDB", &mut combined_stats.tidb_latencies, elapsed);
    println!();
    print_statistics("TiFlash", &mut combined_stats.tiflash_latencies, elapsed);

    // Print comparison
    if !combined_stats.tidb_latencies.is_empty() && !combined_stats.tiflash_latencies.is_empty() {
        let tidb_qps = combined_stats.tidb_latencies.len() as f64 / elapsed.as_secs_f64();
        let tiflash_qps = combined_stats.tiflash_latencies.len() as f64 / elapsed.as_secs_f64();
        println!("\n--- Comparison ---");
        println!("QPS ratio (TiFlash/TiDB): {:.2}x", tiflash_qps / tidb_qps);
    }

    logger.flush()?;
    println!("\n✓ SQL queries have been saved to: {}", args.output_file);

    Ok(())
}
