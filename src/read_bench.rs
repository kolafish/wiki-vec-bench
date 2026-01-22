//! TiDB Read Benchmark - Comparing TiDB vs TiFlash Performance
//!
//! ## How TiFlash Query Execution Works
//!
//! ### 1. TiFlash Architecture
//! - **Columnar Storage**: TiFlash stores data in columnar format (vs TiKV's row format)
//! - **MPP (Massively Parallel Processing)**: Queries are distributed across multiple TiFlash nodes
//! - **Async Replication**: TiFlash replicates data from TiKV asynchronously
//!
//! ### 2. How We Ensure Queries Run on TiFlash
//!
//! **Method 1: SQL Hint (Current Implementation)**
//! ```sql
//! SELECT /*+ READ_FROM_STORAGE(TIFLASH[table_name]) */ ...
//! ```
//! - Forces TiDB optimizer to read from TiFlash replica
//! - Requires: TiFlash replica exists AND engine isolation allows TiFlash
//!
//! **Method 2: Session Variable (Alternative)**
//! ```sql
//! SET SESSION tidb_isolation_read_engines = 'tiflash';
//! ```
//! - Forces all queries in the session to use TiFlash (if available)
//!
//! ### 3. Verification Steps
//!
//! 1. **Check Replica Exists**: Query `information_schema.tiflash_replica`
//! 2. **Verify with EXPLAIN**: Check if `task` column shows `cop[tiflash]` or `batchCop[tiflash]`
//! 3. **Check Engine Isolation**: Ensure `tidb_isolation_read_engines` includes 'tiflash'
//!
//! ### 4. When TiFlash Hint May Be Ignored
//!
//! - No TiFlash replica for the table
//! - Engine isolation doesn't include TiFlash
//! - Unsupported operations (some functions/operators)
//! - Write operations (INSERT/UPDATE/DELETE)
//!
//! ### 5. TiFlash Query Execution Flow
//!
//! 1. TiDB receives query with hint
//! 2. Optimizer checks if TiFlash replica exists
//! 3. If yes, generates MPP plan and sends to TiFlash nodes
//! 4. TiFlash nodes execute query in parallel (columnar processing)
//! 5. Results are aggregated and returned to TiDB
//! 6. TiDB returns final result to client

use clap::Parser;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use sqlx::mysql::{MySqlPoolOptions, MySqlConnectOptions};
use sqlx::{Pool, MySql};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
}

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
}

#[derive(Clone, Debug, sqlx::FromRow)]
struct SampleData {
    title: String,
    text: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("--- TiDB read benchmark for wiki_paragraphs_embeddings ---");
    println!("concurrency    : {}", args.concurrency);
    println!("duration       : {}s", args.duration);
    println!("sample_size    : {}", args.sample_size);

    // Global TiDB connection settings
    let opts = MySqlConnectOptions::new()
        .host("127.0.0.1")
        .port(4000)
        .username("root")
        .database("test")
        .charset("utf8mb4");

    let pool = MySqlPoolOptions::new()
        .max_connections(args.concurrency as u32 + 8)
        .connect_with(opts)
        .await?;

    // Find the table with the latest timestamp
    let table_name = find_latest_table(&pool).await?;
    if table_name.is_empty() {
        eprintln!("❌ Error: No table found matching pattern wiki_paragraphs_embeddings_*");
        return Ok(());
    }
    println!("using table: {}", table_name);

    // Check if TiFlash replica exists and verify query execution
    check_tiflash_replica(&pool, &table_name).await?;
    verify_tiflash_query(&pool, &table_name).await?;

    // Sample test data from the table
    println!("> Sampling {} rows (title, text) from database...", args.sample_size);
    let sampled_data = fetch_sample_data(&pool, &table_name, args.sample_size).await?;
    
    if sampled_data.is_empty() {
        eprintln!("❌ Error: Table seems empty.");
        return Ok(());
    }
    println!("> Successfully loaded {} unique sample points.", sampled_data.len());

    let shared_data = Arc::new(sampled_data);
    let start_time = Instant::now();
    let run_duration = Duration::from_secs(args.duration);
    let mut handles = Vec::with_capacity(args.concurrency);

    for _ in 0..args.concurrency {
        let pool = pool.clone();
        let data_pool = shared_data.clone();
        let table_name = table_name.clone();
        let verbose = args.verbose;

        let handle = tokio::spawn(async move {
            let mut rng = StdRng::from_entropy();
            let mut stats = ThreadStats::new();

            while start_time.elapsed() < run_duration {
                // Get a sample and extract search word once for fair comparison
                let sample = get_test_sample(&mut rng, &data_pool);
                let words: Vec<&str> = sample.text.split_whitespace().collect();
                if words.is_empty() {
                    continue;
                }
                let search_word = words[rng.gen_range(0..words.len())].to_string();

                // Run query on TiDB
                match run_query_tidb(&pool, &table_name, &search_word, verbose).await {
                    Ok(duration) => {
                        let micros = duration.as_micros();
                        stats.tidb_latencies.push(micros);
                    }
                    Err(e) => {
                        eprintln!("TiDB query error: {}", e);
                        stats.errors += 1;
                    }
                }

                // Run the same query on TiFlash
                match run_query_tiflash(&pool, &table_name, &search_word, verbose).await {
                    Ok(duration) => {
                        let micros = duration.as_micros();
                        stats.tiflash_latencies.push(micros);
                    }
                    Err(e) => {
                        eprintln!("TiFlash query error: {}", e);
                        stats.errors += 1;
                    }
                }
            }

            stats
        });

        handles.push(handle);
    }

    let mut total_errors = 0;
    let mut all_tidb_lat = Vec::new();
    let mut all_tiflash_lat = Vec::new();

    for handle in handles {
        let stats = handle.await?;
        total_errors += stats.errors;
        all_tidb_lat.extend(stats.tidb_latencies);
        all_tiflash_lat.extend(stats.tiflash_latencies);
    }

    let elapsed = start_time.elapsed();
    let tidb_total = all_tidb_lat.len() as u64;
    let tiflash_total = all_tiflash_lat.len() as u64;
    let tidb_qps = tidb_total as f64 / elapsed.as_secs_f64();
    let tiflash_qps = tiflash_total as f64 / elapsed.as_secs_f64();

    println!();
    println!("=== Summary ===");
    println!("Elapsed        : {:.2?}", elapsed);
    println!("Total errors   : {}", total_errors);
    println!();

    println!("--- TiDB Statistics ---");
    println!("Total queries  : {}", tidb_total);
    println!("QPS            : {:.2}", tidb_qps);
    print_percentiles("TiDB", &mut all_tidb_lat, tidb_qps);

    println!();
    println!("--- TiFlash Statistics ---");
    println!("Total queries  : {}", tiflash_total);
    println!("QPS            : {:.2}", tiflash_qps);
    print_percentiles("TiFlash", &mut all_tiflash_lat, tiflash_qps);

    println!();
    println!("--- Comparison ---");
    if tidb_qps > 0.0 && tiflash_qps > 0.0 {
        let qps_ratio = tiflash_qps / tidb_qps;
        println!("QPS ratio (TiFlash/TiDB): {:.2}x", qps_ratio);
    }

    Ok(())
}

fn print_percentiles(name: &str, latencies: &mut Vec<u128>, qps: f64) {
    if latencies.is_empty() {
        println!("{}: no samples", name);
        return;
    }
    latencies.sort_unstable();
    let len = latencies.len() as f64;
    let p50 = latencies[(len * 0.50) as usize];
    let p95 = latencies[((len * 0.95) as usize).min(latencies.len() - 1)];
    let p99 = latencies[((len * 0.99) as usize).min(latencies.len() - 1)];
    let max = *latencies.last().unwrap();

    let to_ms = |us: u128| us as f64 / 1000.0;
    println!(
        "{:<10} qps={:.2} p50={:.2}ms p95={:.2}ms p99={:.2}ms max={:.2}ms",
        name,
        qps,
        to_ms(p50),
        to_ms(p95),
        to_ms(p99),
        to_ms(max)
    );
}

async fn find_latest_table(pool: &Pool<MySql>) -> Result<String, sqlx::Error> {
    let query = r#"
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'test' 
        AND table_name LIKE 'wiki_paragraphs_embeddings_%'
        ORDER BY table_name DESC 
        LIMIT 1
    "#;
    
    let result: Option<String> = sqlx::query_scalar(query)
        .fetch_optional(pool)
        .await?;
    
    Ok(result.unwrap_or_default())
}

async fn fetch_sample_data(
    pool: &Pool<MySql>,
    table_name: &str,
    limit: usize,
) -> Result<Vec<SampleData>, sqlx::Error> {
    let query = format!(
        "SELECT title, text FROM `{}` LIMIT {}",
        table_name, limit
    );
    let rows: Vec<SampleData> = sqlx::query_as(&query).fetch_all(pool).await?;
    Ok(rows)
}

fn get_test_sample<'a>(rng: &mut impl Rng, pool: &'a [SampleData]) -> &'a SampleData {
    pool.choose(rng).expect("Pool empty")
}

async fn run_query_tidb(
    pool: &Pool<MySql>,
    table_name: &str,
    search_word: &str,
    verbose: bool,
) -> Result<Duration, sqlx::Error> {
    let start = Instant::now();
    let query = format!(
        r#"
        SELECT count(*) 
        FROM `{}` 
        WHERE fts_match_word(title, ?) OR fts_match_word(text, ?)
        "#,
        table_name
    );
    
    let count: i64 = sqlx::query_scalar(&query)
        .bind(search_word)
        .bind(search_word)
        .fetch_one(pool)
        .await?;
    
    let duration = start.elapsed();

    if verbose {
        println!("---------------------------------------------------");
        println!("[TiDB] Time: {:?} | Count: {} | Word: '{}'", duration.as_millis(), count, search_word);
        println!("SQL: SELECT count(*) FROM `{}` WHERE fts_match_word(title, '{}') OR fts_match_word(text, '{}');",
            table_name, search_word, search_word
        );
    }
    
    Ok(duration)
}

async fn run_query_tiflash(
    pool: &Pool<MySql>,
    table_name: &str,
    search_word: &str,
    verbose: bool,
) -> Result<Duration, sqlx::Error> {
    let start = Instant::now();
    // Use TiFlash by SQL hint: READ_FROM_STORAGE(TIFLASH[table])
    // This hint forces TiDB optimizer to read from TiFlash replica
    // TiFlash uses columnar storage and MPP (Massively Parallel Processing) architecture
    let query = format!(
        r#"
        SELECT /*+ READ_FROM_STORAGE(TIFLASH[`{}`]) */ count(*) 
        FROM `{}` 
        WHERE fts_match_word(title, ?) OR fts_match_word(text, ?)
        "#,
        table_name, table_name
    );
    
    let count: i64 = sqlx::query_scalar(&query)
        .bind(search_word)
        .bind(search_word)
        .fetch_one(pool)
        .await?;
    
    let duration = start.elapsed();

    if verbose {
        println!("---------------------------------------------------");
        println!("[TiFlash] Time: {:?} | Count: {} | Word: '{}'", duration.as_millis(), count, search_word);
        println!("SQL: SELECT /*+ READ_FROM_STORAGE(TIFLASH[`{}`]) */ count(*) FROM `{}` WHERE fts_match_word(title, '{}') OR fts_match_word(text, '{}');",
            table_name, table_name, search_word, search_word
        );
    }
    
    Ok(duration)
}

/// Check if TiFlash replica exists for the table
async fn check_tiflash_replica(pool: &Pool<MySql>, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let query = r#"
        SELECT 
            table_schema,
            table_name,
            is_tiflash_replica
        FROM information_schema.tiflash_replica
        WHERE table_schema = 'test' AND table_name = ?
    "#;
    
    #[derive(sqlx::FromRow)]
    struct ReplicaInfo {
        table_schema: String,
        table_name: String,
        is_tiflash_replica: u8,
    }
    
    let result: Option<ReplicaInfo> = sqlx::query_as(query)
        .bind(table_name)
        .fetch_optional(pool)
        .await?;
    
    match result {
        Some(info) => {
            if info.is_tiflash_replica == 1 {
                println!("✓ TiFlash replica exists for table: {}", table_name);
            } else {
                eprintln!("⚠ Warning: TiFlash replica not available for table: {}", table_name);
                eprintln!("  Query hint may be ignored. Please create TiFlash replica first:");
                eprintln!("  ALTER TABLE `{}` SET TIFLASH REPLICA 1;", table_name);
            }
        }
        None => {
            eprintln!("⚠ Warning: No TiFlash replica found for table: {}", table_name);
            eprintln!("  Query hint may be ignored. Please create TiFlash replica first:");
            eprintln!("  ALTER TABLE `{}` SET TIFLASH REPLICA 1;", table_name);
        }
    }
    
    Ok(())
}

/// Verify that query actually executes on TiFlash by checking EXPLAIN plan
async fn verify_tiflash_query(pool: &Pool<MySql>, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    // First, ensure TiFlash is in isolation read engines
    sqlx::query("SET SESSION tidb_isolation_read_engines = 'tikv,tiflash'")
        .execute(pool)
        .await?;
    
    // Create a test query with hint
    let explain_query = format!(
        r#"
        EXPLAIN SELECT /*+ READ_FROM_STORAGE(TIFLASH[`{}`]) */ count(*) 
        FROM `{}` 
        WHERE fts_match_word(title, 'test') OR fts_match_word(text, 'test')
        "#,
        table_name, table_name
    );
    
    #[derive(sqlx::FromRow)]
    struct ExplainRow {
        id: String,
        est_rows: String,
        task: String,
        access_object: Option<String>,
        operator_info: Option<String>,
    }
    
    let rows: Vec<ExplainRow> = sqlx::query_as(&explain_query)
        .fetch_all(pool)
        .await?;
    
    let mut found_tiflash = false;
    for row in &rows {
        if row.task.contains("tiflash") || row.task.contains("batchCop[tiflash]") || row.task.contains("cop[tiflash]") {
            found_tiflash = true;
            println!("✓ Query plan verification: TiFlash will be used (task: {})", row.task);
            break;
        }
    }
    
    if !found_tiflash {
        eprintln!("⚠ Warning: EXPLAIN plan does not show TiFlash usage");
        eprintln!("  This may mean:");
        eprintln!("  1. TiFlash replica is not available");
        eprintln!("  2. Query operations are not supported by TiFlash");
        eprintln!("  3. Engine isolation settings prevent TiFlash usage");
        eprintln!("  Plan details:");
        for row in &rows {
            println!("    task: {}, operator: {:?}", row.task, row.operator_info);
        }
    }
    
    // Note: Warnings from EXPLAIN are automatically shown in the query result
    // If hint is ignored, TiDB will typically use TiKV instead
    
    Ok(())
}
