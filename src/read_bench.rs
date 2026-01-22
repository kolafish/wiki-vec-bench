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
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
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

    /// Output file path for SQL queries log
    #[arg(long, default_value = "read_bench_queries.sql")]
    output_file: String,
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
    println!("output_file    : {}", args.output_file);

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

    // Print table statistics
    print_table_statistics(&pool, &table_name).await?;

    // Check if TiFlash replica exists and verify query execution
    check_tiflash_replica(&pool, &table_name).await?;
    verify_tiflash_query(&pool, &table_name).await?;

    // Create output file for SQL queries
    let output_file = Arc::new(Mutex::new(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&args.output_file)?,
    ));
    
    // Write header to output file
    {
        let mut file = output_file.lock().unwrap();
        writeln!(file, "-- TiDB Read Benchmark SQL Queries")?;
        writeln!(file, "-- Table: {}", table_name)?;
        writeln!(file, "-- Generated at: {:?}", std::time::SystemTime::now())?;
        writeln!(file, "")?;
    }

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
        let output_file = output_file.clone();

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
                match run_query_tidb(&pool, &table_name, &search_word, verbose, &output_file).await {
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
                match run_query_tiflash(&pool, &table_name, &search_word, verbose, &output_file).await {
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

    println!();
    println!("✓ SQL queries have been saved to: {}", args.output_file);

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

/// Print table statistics including row count, table size, etc.
async fn print_table_statistics(pool: &Pool<MySql>, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!();
    println!("--- Table Statistics ---");
    
    // Get row count
    let count_query = format!("SELECT COUNT(*) FROM `{}`", table_name);
    let row_count: i64 = sqlx::query_scalar(&count_query)
        .fetch_one(pool)
        .await?;
    println!("Total rows      : {}", row_count);
    
    // Get table size information
    let size_query = r#"
        SELECT 
            table_rows,
            data_length,
            index_length,
            (data_length + index_length) as total_size
        FROM information_schema.tables
        WHERE table_schema = 'test' AND table_name = ?
    "#;
    
    #[derive(sqlx::FromRow)]
    struct TableSize {
        table_rows: Option<u64>,
        data_length: Option<u64>,
        index_length: Option<u64>,
        total_size: Option<u64>,
    }
    
    if let Ok(Some(size_info)) = sqlx::query_as::<_, TableSize>(size_query)
        .bind(table_name)
        .fetch_optional(pool)
        .await
    {
        if let Some(rows) = size_info.table_rows {
            println!("Table rows (est) : {}", rows);
        }
        
        let format_bytes = |bytes: Option<u64>| -> String {
            bytes.map_or("N/A".to_string(), |b| {
                if b < 1024 {
                    format!("{} B", b)
                } else if b < 1024 * 1024 {
                    format!("{:.2} KB", b as f64 / 1024.0)
                } else if b < 1024 * 1024 * 1024 {
                    format!("{:.2} MB", b as f64 / (1024.0 * 1024.0))
                } else {
                    format!("{:.2} GB", b as f64 / (1024.0 * 1024.0 * 1024.0))
                }
            })
        };
        
        println!("Data size       : {}", format_bytes(size_info.data_length));
        println!("Index size      : {}", format_bytes(size_info.index_length));
        println!("Total size      : {}", format_bytes(size_info.total_size));
    }
    
    // Get column information
    let col_query = r#"
        SELECT 
            column_name,
            data_type,
            character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = 'test' AND table_name = ?
        ORDER BY ordinal_position
    "#;
    
    #[derive(sqlx::FromRow)]
    struct ColumnInfo {
        column_name: String,
        data_type: String,
        character_maximum_length: Option<u64>,
    }
    
    if let Ok(columns) = sqlx::query_as::<_, ColumnInfo>(col_query)
        .bind(table_name)
        .fetch_all(pool)
        .await
    {
        println!("Columns         : {}", columns.len());
        if columns.len() <= 10 {
            for col in &columns {
                let col_type = if let Some(max_len) = col.character_maximum_length {
                    format!("{}({})", col.data_type, max_len)
                } else {
                    col.data_type.clone()
                };
                println!("  - {} : {}", col.column_name, col_type);
            }
        } else {
            for col in columns.iter().take(5) {
                let col_type = if let Some(max_len) = col.character_maximum_length {
                    format!("{}({})", col.data_type, max_len)
                } else {
                    col.data_type.clone()
                };
                println!("  - {} : {}", col.column_name, col_type);
            }
            println!("  ... and {} more columns", columns.len() - 5);
        }
    }
    
    println!();
    
    Ok(())
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
    output_file: &Arc<Mutex<std::fs::File>>,
) -> Result<Duration, sqlx::Error> {
    let start = Instant::now();
    
    // Escape single quotes in search_word to prevent SQL injection
    let escaped_word = search_word.replace("'", "''");
    
    // Build query with literal string (not placeholder) because TiDB fts_match_word requires constant
    let query = format!(
        r#"
        SELECT count(*) 
        FROM `{}` 
        WHERE fts_match_word('{}', text) OR fts_match_word('{}', title)
        "#,
        table_name, escaped_word, escaped_word
    );
    
    // Log SQL query to file
    if let Ok(mut file) = output_file.lock() {
        let _ = writeln!(file, "-- TiDB Query");
        let _ = writeln!(file, "{}", query.trim());
    }
    
    let count: i64 = sqlx::query_scalar(&query)
        .fetch_one(pool)
        .await?;
    
    let duration = start.elapsed();

    if verbose {
        println!("---------------------------------------------------");
        println!("[TiDB] Time: {:?} | Count: {} | Word: '{}'", duration.as_millis(), count, search_word);
        println!("SQL: {}", query.trim());
    }
    
    Ok(duration)
}

async fn run_query_tiflash(
    pool: &Pool<MySql>,
    table_name: &str,
    search_word: &str,
    verbose: bool,
    output_file: &Arc<Mutex<std::fs::File>>,
) -> Result<Duration, sqlx::Error> {
    let start = Instant::now();
    
    // Escape for SQL and LIKE wildcards
    let escaped_word = search_word
        .replace("\\", "\\\\")
        .replace("'", "''")
        .replace("%", "\\%")
        .replace("_", "\\_");
    
    // Use TiFlash by SQL hint: READ_FROM_STORAGE(TIFLASH[table])
    // This hint forces TiDB optimizer to read from TiFlash replica
    // TiFlash uses columnar storage and MPP (Massively Parallel Processing) architecture
    // Note: TiFlash doesn't support fts_match_word, so we use LIKE for string matching
    let query = format!(
        r#"
        SELECT /*+ READ_FROM_STORAGE(TIFLASH[`{}`]) */ count(*) 
        FROM `{}` 
        WHERE text LIKE '%{}%' OR title LIKE '%{}%'
        "#,
        table_name, table_name, escaped_word, escaped_word
    );
    
    // Log SQL query to file
    if let Ok(mut file) = output_file.lock() {
        let _ = writeln!(file, "-- TiFlash Query (using LIKE)");
        let _ = writeln!(file, "{}", query.trim());
    }
    
    let count: i64 = sqlx::query_scalar(&query)
        .fetch_one(pool)
        .await?;
    
    let duration = start.elapsed();

    if verbose {
        println!("---------------------------------------------------");
        println!("[TiFlash] Time: {:?} | Count: {} | Word: '{}'", duration.as_millis(), count, search_word);
        println!("SQL: {}", query.trim());
    }
    
    Ok(duration)
}

/// Check if TiFlash replica exists for the table, create if missing and wait for sync
async fn check_tiflash_replica(pool: &Pool<MySql>, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let query = r#"
        SELECT 
            table_schema,
            table_name,
            replica_count,
            available,
            progress
        FROM information_schema.tiflash_replica
        WHERE table_schema = 'test' AND table_name = ?
    "#;
    
    #[derive(sqlx::FromRow)]
    struct ReplicaInfo {
        table_schema: String,
        table_name: String,
        replica_count: Option<i64>,
        available: Option<bool>,
        progress: Option<f64>,
    }
    
    let result: Option<ReplicaInfo> = sqlx::query_as(query)
        .bind(table_name)
        .fetch_optional(pool)
        .await?;
    
    match result {
        Some(info) => {
            // Check if replica exists (replica_count > 0) and is available
            if let Some(replica_count) = info.replica_count {
                if replica_count > 0 {
                    if let Some(available) = info.available {
                        if available {
                            if let Some(progress) = info.progress {
                                if progress >= 1.0 {
                                    println!("✓ TiFlash replica exists and is synced for table: {} (progress: {:.2}%)", 
                                        table_name, progress * 100.0);
                                    return Ok(());
                                } else {
                                    println!("⏳ TiFlash replica exists but not fully synced (progress: {:.2}%), waiting...", 
                                        progress * 100.0);
                                    // Wait for sync to complete
                                    wait_for_tiflash_sync(pool, table_name).await?;
                                    return Ok(());
                                }
                            } else {
                                println!("✓ TiFlash replica exists and is available for table: {}", table_name);
                                return Ok(());
                            }
                        } else {
                            println!("⏳ TiFlash replica exists but not yet available, waiting...");
                            // Wait for replica to become available and synced
                            wait_for_tiflash_sync(pool, table_name).await?;
                            return Ok(());
                        }
                    } else {
                        println!("✓ TiFlash replica exists for table: {} (replica_count: {})", table_name, replica_count);
                        // Wait for sync to complete
                        wait_for_tiflash_sync(pool, table_name).await?;
                        return Ok(());
                    }
                }
            }
        }
        None => {
            // No replica found, need to create
        }
    }
    
    // TiFlash replica doesn't exist or not available, create it
    println!("> Creating TiFlash replica for table: {}", table_name);
    let alter_sql = format!("ALTER TABLE `{}` SET TIFLASH REPLICA 1", table_name);
    
    match sqlx::query(&alter_sql).execute(pool).await {
        Ok(_) => {
            println!("✓ TiFlash replica creation command executed successfully");
        }
        Err(e) => {
            return Err(format!("❌ Failed to create TiFlash replica: {}. Please ensure TiFlash cluster is running and accessible.", e).into());
        }
    }
    
    // Wait for replica to be created and synced
    println!("> Waiting for TiFlash replica to sync...");
    wait_for_tiflash_sync(pool, table_name).await?;
    
    Ok(())
}

/// Wait for TiFlash replica to be fully synced
async fn wait_for_tiflash_sync(pool: &Pool<MySql>, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let max_wait_seconds = 300; // Maximum 5 minutes
    let check_interval = Duration::from_secs(2);
    let start = Instant::now();
    
    loop {
        let query = r#"
            SELECT 
                replica_count,
                available,
                progress
            FROM information_schema.tiflash_replica
            WHERE table_schema = 'test' AND table_name = ?
        "#;
        
        #[derive(sqlx::FromRow)]
        struct SyncInfo {
            replica_count: Option<i64>,
            available: Option<bool>,
            progress: Option<f64>,
        }
        
        let result: Option<SyncInfo> = sqlx::query_as(query)
            .bind(table_name)
            .fetch_optional(pool)
            .await?;
        
        match result {
            Some(info) => {
                if let Some(replica_count) = info.replica_count {
                    if replica_count > 0 {
                        if let Some(available) = info.available {
                            if available {
                                if let Some(progress) = info.progress {
                                    if progress >= 1.0 {
                                        println!("\n✓ TiFlash replica is fully synced (progress: {:.2}%)", progress * 100.0);
                                        return Ok(());
                                    } else {
                                        print!("\r⏳ Syncing TiFlash replica... {:.1}%", progress * 100.0);
                                        io::stdout().flush().ok();
                                    }
                                } else {
                                    // Progress not available, but replica is available, assume it's ready
                                    println!("\n✓ TiFlash replica is available");
                                    return Ok(());
                                }
                            } else {
                                // Replica exists but not available yet
                                print!("\r⏳ Waiting for TiFlash replica to become available...");
                                io::stdout().flush().ok();
                            }
                        } else {
                            // Replica exists but availability unknown, check progress
                            if let Some(progress) = info.progress {
                                if progress >= 1.0 {
                                    println!("\n✓ TiFlash replica is fully synced (progress: {:.2}%)", progress * 100.0);
                                    return Ok(());
                                } else {
                                    print!("\r⏳ Syncing TiFlash replica... {:.1}%", progress * 100.0);
                                    io::stdout().flush().ok();
                                }
                            } else {
                                print!("\r⏳ Waiting for TiFlash replica to sync...");
                                io::stdout().flush().ok();
                            }
                        }
                    } else {
                        // Replica count is 0, not created yet
                        print!("\r⏳ Waiting for TiFlash replica to be created...");
                        io::stdout().flush().ok();
                    }
                } else {
                    // Replica count is None or 0
                    print!("\r⏳ Waiting for TiFlash replica to be created...");
                    io::stdout().flush().ok();
                }
            }
            None => {
                // Replica not found yet
                print!("\r⏳ Waiting for TiFlash replica to be created...");
                io::stdout().flush().ok();
            }
        }
        
        if start.elapsed().as_secs() > max_wait_seconds {
            return Err(format!(
                "❌ Timeout: TiFlash replica for table '{}' did not sync within {} seconds. Please check TiFlash cluster status.",
                table_name, max_wait_seconds
            ).into());
        }
        
        tokio::time::sleep(check_interval).await;
    }
}

/// Verify that query actually executes on TiFlash by checking EXPLAIN plan
async fn verify_tiflash_query(pool: &Pool<MySql>, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    // First, ensure TiFlash is in isolation read engines
    sqlx::query("SET SESSION tidb_isolation_read_engines = 'tikv,tiflash'")
        .execute(pool)
        .await?;
    
    // Create a test query with hint (using LIKE since TiFlash doesn't support fts_match_word)
    let explain_query = format!(
        r#"
        EXPLAIN SELECT /*+ READ_FROM_STORAGE(TIFLASH[`{}`]) */ count(*) 
        FROM `{}` 
        WHERE text LIKE '%test%' OR title LIKE '%test%'
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
