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
use std::collections::HashMap;
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

    /// Enable complex query benchmark
    #[arg(long, default_value_t = false)]
    complex_queries: bool,
}

// ============================================================================
// Data Structures
// ============================================================================

struct ThreadStats {
    tidb_latencies: Vec<u128>,
    tikv_latencies: Vec<u128>,
    tiflash_latencies: Vec<u128>,
    tidb_complex_latencies: Vec<u128>,
    tikv_complex_latencies: Vec<u128>,
    tiflash_complex_latencies: Vec<u128>,
    errors: usize,
}

impl ThreadStats {
    fn new() -> Self {
        Self {
            tidb_latencies: Vec::with_capacity(4096),
            tikv_latencies: Vec::with_capacity(4096),
            tiflash_latencies: Vec::with_capacity(4096),
            tidb_complex_latencies: Vec::with_capacity(4096),
            tikv_complex_latencies: Vec::with_capacity(4096),
            tiflash_complex_latencies: Vec::with_capacity(4096),
            errors: 0,
        }
    }

    fn merge(&mut self, other: ThreadStats) {
        self.tidb_latencies.extend(other.tidb_latencies);
        self.tikv_latencies.extend(other.tikv_latencies);
        self.tiflash_latencies.extend(other.tiflash_latencies);
        self.tidb_complex_latencies.extend(other.tidb_complex_latencies);
        self.tikv_complex_latencies.extend(other.tikv_complex_latencies);
        self.tiflash_complex_latencies.extend(other.tiflash_complex_latencies);
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
    TiKV,
    TiFlash,
}

impl QueryEngine {
    fn name(&self) -> &str {
        match self {
            QueryEngine::TiDB => "TiDB",
            QueryEngine::TiKV => "TiKV",
            QueryEngine::TiFlash => "TiFlash",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum FieldType {
    Title,
    Text,
}

impl FieldType {
    fn name(&self) -> &str {
        match self {
            FieldType::Title => "title",
            FieldType::Text => "text",
        }
    }
}

struct SearchQuery {
    word: String,
    field: FieldType,
}

struct RareWordPool {
    title_words: Vec<String>,
    text_words: Vec<String>,
}

impl RareWordPool {
    fn pick_word(&self, field: FieldType, rng: &mut impl Rng) -> Option<String> {
        let pool = match field {
            FieldType::Title => &self.title_words,
            FieldType::Text => &self.text_words,
        };
        if pool.is_empty() {
            None
        } else {
            Some(pool[rng.gen_range(0..pool.len())].clone())
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ComplexQueryVariant {
    WithMetadataFilter,  // Filter by views and langs
    GroupByLangs,        // Aggregate by langs
    TopNByViews,         // TOP-N ordered by views
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

    fn log_query(&self, engine: QueryEngine, sql: &str, duration: Duration) {
        if let Ok(mut writer) = self.writer.lock() {
            let _ = writeln!(writer, "-- {} Query (Execution time: {:.2}ms)", engine.name(), duration.as_secs_f64() * 1000.0);
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
    /// TiDB uses fts_match_word (full-text index)
    /// TiKV uses LIKE (string matching without hint)
    /// TiFlash uses LIKE (string matching with TIFLASH hint, as fts_match_word is not supported)
    fn build_fts_query(&self, search_word: &str, field: FieldType, engine: QueryEngine) -> String {
        let escaped_word = Self::escape_sql_string(search_word);
        let field_name = field.name();
        
        match engine {
            QueryEngine::TiDB => {
                // TiDB: Use fts_match_word with FULLTEXT index on specific field
                format!(
                    "SELECT count(*) FROM `{}` WHERE fts_match_word('{}', {})",
                    self.table_name, escaped_word, field_name
                )
            }
            QueryEngine::TiKV => {
                // TiKV: Use LIKE for string matching on specific field (without hint, defaults to TiKV)
                format!(
                    "SELECT /*+ READ_FROM_STORAGE(TIKV[`{}`]) */ count(*) FROM `{}` WHERE {} LIKE '%{}%'",
                    self.table_name, self.table_name, field_name, escaped_word
                )
            }
            QueryEngine::TiFlash => {
                // TiFlash: Use LIKE for string matching on specific field (fts_match_word not supported)
                format!(
                    "SELECT /*+ READ_FROM_STORAGE(TIFLASH[`{}`]) */ count(*) FROM `{}` WHERE {} LIKE '%{}%'",
                    self.table_name, self.table_name, field_name, escaped_word
                )
            }
        }
    }

    /// Build complex query with metadata filtering and aggregations
    fn build_complex_query(
        &self, 
        search_word: &str, 
        field: FieldType, 
        engine: QueryEngine,
        variant: ComplexQueryVariant,
    ) -> String {
        let escaped_word = Self::escape_sql_string(search_word);
        let field_name = field.name();
        let storage_hint = match engine {
            QueryEngine::TiDB => String::new(),
            QueryEngine::TiKV => format!("/*+ READ_FROM_STORAGE(TIKV[`{}`]) */", self.table_name),
            QueryEngine::TiFlash => format!("/*+ READ_FROM_STORAGE(TIFLASH[`{}`]) */", self.table_name),
        };
        let storage_prefix = if storage_hint.is_empty() {
            String::new()
        } else {
            format!("{} ", storage_hint)
        };
        let match_predicate = match engine {
            QueryEngine::TiDB => format!("fts_match_word('{}', {})", escaped_word, field_name),
            _ => format!("{} LIKE '%{}%'", field_name, escaped_word),
        };
        
        match variant {
            ComplexQueryVariant::WithMetadataFilter => {
                // Filter by metadata while keeping aggregation on PK
                format!(
                    "SELECT {}COUNT(id) FROM `{}` WHERE {} AND views > 1000 AND langs >= 2",
                    storage_prefix, self.table_name, match_predicate
                )
            }
            ComplexQueryVariant::GroupByLangs => {
                // GROUP BY langs with PK-based aggregation
                format!(
                    "SELECT {} langs, COUNT(id) as cnt FROM `{}` WHERE {} GROUP BY langs ORDER BY cnt DESC LIMIT 10",
                    storage_prefix, self.table_name, match_predicate
                )
            }
            ComplexQueryVariant::TopNByViews => {
                // TOP-N query ordered by views, selecting only PK
                format!(
                    "SELECT {} id FROM `{}` WHERE {} ORDER BY views DESC LIMIT 10",
                    storage_prefix, self.table_name, match_predicate
                )
            }
        }
    }

    /// Escape SQL string to prevent injection
    /// Also escapes LIKE wildcards to prevent unintended pattern matching
    fn escape_sql_string(s: &str) -> String {
        s.replace('\\', "\\\\")
         .replace('\'', "''")
         .replace('"', "\\\"")
         .replace('\0', "\\0")
         .replace('%', "\\%")
         .replace('_', "\\_")
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
        field: FieldType,
        engine: QueryEngine,
        verbose: bool,
    ) -> Result<Duration, sqlx::Error> {
        let query = self.query_builder.build_fts_query(search_word, field, engine);
        
        let start = Instant::now();
        let count: i64 = sqlx::query_scalar(&query)
            .fetch_one(&self.pool)
            .await?;
        let duration = start.elapsed();

        // Log to file with execution time
        self.logger.log_query(engine, &query, duration);

        if verbose {
            println!("---------------------------------------------------");
            println!("[{}] Time: {:?} | Count: {} | Field: {} | Word: '{}'", 
                engine.name(), duration.as_millis(), count, field.name(), search_word);
            println!("SQL: {}", query);
        }

        Ok(duration)
    }

    async fn execute_complex_query(
        &self,
        search_word: &str,
        field: FieldType,
        engine: QueryEngine,
        variant: ComplexQueryVariant,
        verbose: bool,
    ) -> Result<Duration, sqlx::Error> {
        let query = self.query_builder.build_complex_query(search_word, field, engine, variant);
        
        let start = Instant::now();
        // Execute query but ignore results (we only care about execution time)
        let _rows = sqlx::query(&query)
            .fetch_all(&self.pool)
            .await?;
        let duration = start.elapsed();

        // Log to file with execution time
        self.logger.log_query(engine, &query, duration);

        if verbose {
            println!("---------------------------------------------------");
            println!("[{} COMPLEX] Time: {:?} | Field: {} | Word: '{}' | Variant: {:?}", 
                engine.name(), duration.as_millis(), field.name(), search_word, variant);
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

        // Test query using LIKE (not fts_match_word, as TiFlash doesn't support it)
        let explain_query = format!(
            "EXPLAIN SELECT /*+ READ_FROM_STORAGE(TIFLASH[`{}`]) */ count(*)
             FROM `{}`
             WHERE text LIKE '%test%' OR title LIKE '%test%'",
            table_name, table_name
        );

        #[derive(sqlx::FromRow)]
        struct ExplainRow {
            task: String,
        }

        match sqlx::query_as::<_, ExplainRow>(&explain_query).fetch_all(&self.pool).await {
            Ok(rows) => {
                let found_tiflash = rows.iter().any(|r| r.task.contains("tiflash"));
                if found_tiflash {
                    println!("✓ Query plan verification: TiFlash will be used (with LIKE pattern matching)");
                } else {
                    eprintln!("⚠ Warning: EXPLAIN plan does not show TiFlash usage");
                }
            }
            Err(e) => {
                eprintln!("⚠ Warning: Could not verify TiFlash query plan: {}", e);
                eprintln!("  Benchmark will continue with fallback behavior");
            }
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

fn normalize_word(token: &str) -> Option<String> {
    let trimmed = token.trim_matches(|c: char| !c.is_alphanumeric());
    if trimmed.len() < 3 {
        return None;
    }
    Some(trimmed.to_lowercase())
}

fn pick_word_from_text(rng: &mut impl Rng, text: &str) -> Option<String> {
    let words: Vec<String> = text
        .split_whitespace()
        .filter_map(normalize_word)
        .collect();
    if words.is_empty() {
        return None;
    }
    Some(words[rng.gen_range(0..words.len())].clone())
}

fn build_rare_word_list(freq: HashMap<String, usize>) -> Vec<String> {
    let mut items: Vec<(String, usize)> = freq.into_iter().collect();
    if items.is_empty() {
        return Vec::new();
    }
    items.sort_by_key(|(_, count)| *count);

    let mut rare_words: Vec<String> = items
        .iter()
        .take_while(|(_, count)| *count <= 2)
        .map(|(word, _)| word.clone())
        .collect();

    if rare_words.len() < 50 {
        rare_words = items
            .iter()
            .take(200.min(items.len()))
            .map(|(word, _)| word.clone())
            .collect();
    }

    rare_words
}

fn build_rare_word_pool(sample_data: &[SampleData]) -> RareWordPool {
    let mut title_freq: HashMap<String, usize> = HashMap::new();
    let mut text_freq: HashMap<String, usize> = HashMap::new();

    for sample in sample_data {
        for token in sample.title.split_whitespace() {
            if let Some(word) = normalize_word(token) {
                *title_freq.entry(word).or_insert(0) += 1;
            }
        }
        for token in sample.text.split_whitespace() {
            if let Some(word) = normalize_word(token) {
                *text_freq.entry(word).or_insert(0) += 1;
            }
        }
    }

    RareWordPool {
        title_words: build_rare_word_list(title_freq),
        text_words: build_rare_word_list(text_freq),
    }
}

fn extract_search_word(
    rng: &mut impl Rng,
    sample_data: &[SampleData],
    rare_words: Option<&RareWordPool>,
) -> Option<SearchQuery> {
    let sample = sample_data.choose(rng)?;
    
    // Randomly choose between title and text field (75% title, 25% text)
    let (field, text) = if rng.gen_bool(0.75) {
        (FieldType::Title, &sample.title)
    } else {
        (FieldType::Text, &sample.text)
    };

    if let Some(pool) = rare_words {
        if let Some(word) = pool.pick_word(field, rng) {
            return Some(SearchQuery { word, field });
        }
    }
    
    pick_word_from_text(rng, text).map(|word| SearchQuery { word, field })
}

// ============================================================================
// Benchmark Runner
// ============================================================================

async fn run_benchmark_worker(
    executor: Arc<QueryExecutor>,
    sample_data: Arc<Vec<SampleData>>,
    rare_words: Arc<RareWordPool>,
    start_time: Instant,
    duration: Duration,
    engine: QueryEngine,
    verbose: bool,
) -> ThreadStats {
    let mut rng = StdRng::from_entropy();
    let mut stats = ThreadStats::new();

    while start_time.elapsed() < duration {
        let Some(search_query) = extract_search_word(&mut rng, &sample_data, Some(&rare_words)) else {
            continue;
        };

        match engine {
            QueryEngine::TiDB => {
                match executor.execute_query(&search_query.word, search_query.field, QueryEngine::TiDB, verbose).await {
                    Ok(dur) => stats.tidb_latencies.push(dur.as_micros()),
                    Err(e) => {
                        eprintln!("TiDB query error: {}", e);
                        stats.errors += 1;
                    }
                }
            }
            QueryEngine::TiKV => {
                match executor.execute_query(&search_query.word, search_query.field, QueryEngine::TiKV, verbose).await {
                    Ok(dur) => stats.tikv_latencies.push(dur.as_micros()),
                    Err(e) => {
                        eprintln!("TiKV query error: {}", e);
                        stats.errors += 1;
                    }
                }
            }
            QueryEngine::TiFlash => {
                match executor.execute_query(&search_query.word, search_query.field, QueryEngine::TiFlash, verbose).await {
                    Ok(dur) => stats.tiflash_latencies.push(dur.as_micros()),
                    Err(e) => {
                        eprintln!("TiFlash query error: {}", e);
                        stats.errors += 1;
                    }
                }
            }
        }
    }

    stats
}

async fn run_complex_benchmark_worker(
    executor: Arc<QueryExecutor>,
    sample_data: Arc<Vec<SampleData>>,
    rare_words: Arc<RareWordPool>,
    start_time: Instant,
    duration: Duration,
    engine: QueryEngine,
    verbose: bool,
) -> ThreadStats {
    let mut rng = StdRng::from_entropy();
    let mut stats = ThreadStats::new();

    // Cycle through different complex query variants
    let variants = [
        ComplexQueryVariant::WithMetadataFilter,
        ComplexQueryVariant::GroupByLangs,
        ComplexQueryVariant::TopNByViews,
    ];

    while start_time.elapsed() < duration {
        let Some(search_query) = extract_search_word(&mut rng, &sample_data, Some(&rare_words)) else {
            continue;
        };

        // Randomly pick a complex query variant
        let variant = variants[rng.gen_range(0..variants.len())];

        match engine {
            QueryEngine::TiDB => {
                match executor.execute_complex_query(&search_query.word, search_query.field, QueryEngine::TiDB, variant, verbose).await {
                    Ok(dur) => stats.tidb_complex_latencies.push(dur.as_micros()),
                    Err(e) => {
                        eprintln!("TiDB complex query error: {}", e);
                        stats.errors += 1;
                    }
                }
            }
            QueryEngine::TiKV => {
                match executor.execute_complex_query(&search_query.word, search_query.field, QueryEngine::TiKV, variant, verbose).await {
                    Ok(dur) => stats.tikv_complex_latencies.push(dur.as_micros()),
                    Err(e) => {
                        eprintln!("TiKV complex query error: {}", e);
                        stats.errors += 1;
                    }
                }
            }
            QueryEngine::TiFlash => {
                match executor.execute_complex_query(&search_query.word, search_query.field, QueryEngine::TiFlash, variant, verbose).await {
                    Ok(dur) => stats.tiflash_complex_latencies.push(dur.as_micros()),
                    Err(e) => {
                        eprintln!("TiFlash complex query error: {}", e);
                        stats.errors += 1;
                    }
                }
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
    let rare_words = Arc::new(build_rare_word_pool(&sample_data));
    let sample_data = Arc::new(sample_data);
    let duration = Duration::from_secs(args.duration);

    if args.complex_queries {
        println!("\n========================================");
        println!("=== COMPLEX QUERIES BENCHMARK ===");
        println!("========================================\n");

        // Phase 1: Complex TiDB queries
        println!("=== Phase 1: Benchmarking TiDB (Complex Queries) ===");
        let tidb_complex_start = Instant::now();
        let mut tidb_complex_handles = Vec::with_capacity(args.concurrency);

        for _ in 0..args.concurrency {
            let executor = executor.clone();
            let sample_data = sample_data.clone();
            let rare_words = rare_words.clone();
            let verbose = args.verbose;

            let handle = tokio::spawn(async move {
                run_complex_benchmark_worker(executor, sample_data, rare_words, tidb_complex_start, duration, QueryEngine::TiDB, verbose).await
            });

            tidb_complex_handles.push(handle);
        }

        let mut tidb_complex_stats = ThreadStats::new();
        for handle in tidb_complex_handles {
            tidb_complex_stats.merge(handle.await?);
        }
        let tidb_complex_elapsed = tidb_complex_start.elapsed();
        println!("✓ TiDB complex queries completed in {:.2?}\n", tidb_complex_elapsed);

        // Phase 2: Complex TiKV queries
        println!("=== Phase 2: Benchmarking TiKV (Complex Queries) ===");
        let tikv_complex_start = Instant::now();
        let mut tikv_complex_handles = Vec::with_capacity(args.concurrency);

        for _ in 0..args.concurrency {
            let executor = executor.clone();
            let sample_data = sample_data.clone();
            let rare_words = rare_words.clone();
            let verbose = args.verbose;

            let handle = tokio::spawn(async move {
                run_complex_benchmark_worker(executor, sample_data, rare_words, tikv_complex_start, duration, QueryEngine::TiKV, verbose).await
            });

            tikv_complex_handles.push(handle);
        }

        let mut tikv_complex_stats = ThreadStats::new();
        for handle in tikv_complex_handles {
            tikv_complex_stats.merge(handle.await?);
        }
        let tikv_complex_elapsed = tikv_complex_start.elapsed();
        println!("✓ TiKV complex queries completed in {:.2?}\n", tikv_complex_elapsed);

        // Phase 3: Complex TiFlash queries
        println!("=== Phase 3: Benchmarking TiFlash (Complex Queries) ===");
        let tiflash_complex_start = Instant::now();
        let mut tiflash_complex_handles = Vec::with_capacity(args.concurrency);

        for _ in 0..args.concurrency {
            let executor = executor.clone();
            let sample_data = sample_data.clone();
            let rare_words = rare_words.clone();
            let verbose = args.verbose;

            let handle = tokio::spawn(async move {
                run_complex_benchmark_worker(executor, sample_data, rare_words, tiflash_complex_start, duration, QueryEngine::TiFlash, verbose).await
            });

            tiflash_complex_handles.push(handle);
        }

        let mut tiflash_complex_stats = ThreadStats::new();
        for handle in tiflash_complex_handles {
            tiflash_complex_stats.merge(handle.await?);
        }
        let tiflash_complex_elapsed = tiflash_complex_start.elapsed();
        println!("✓ TiFlash complex queries completed in {:.2?}\n", tiflash_complex_elapsed);

        // Summary (complex only)
        let total_elapsed = tidb_complex_elapsed + tikv_complex_elapsed + tiflash_complex_elapsed;
        let mut combined_stats = ThreadStats::new();
        combined_stats.tidb_complex_latencies = tidb_complex_stats.tidb_complex_latencies;
        combined_stats.tikv_complex_latencies = tikv_complex_stats.tikv_complex_latencies;
        combined_stats.tiflash_complex_latencies = tiflash_complex_stats.tiflash_complex_latencies;
        combined_stats.errors = tidb_complex_stats.errors + tikv_complex_stats.errors + tiflash_complex_stats.errors;

        println!("\n=== Summary ===");
        println!("Query Type     : Complex");
        println!("TiDB elapsed   : {:.2?}", tidb_complex_elapsed);
        println!("TiKV elapsed   : {:.2?}", tikv_complex_elapsed);
        println!("TiFlash elapsed: {:.2?}", tiflash_complex_elapsed);
        println!("Total elapsed  : {:.2?}", total_elapsed);
        println!("Total errors   : {}\n", combined_stats.errors);

        println!("--- Query Statistics ---");
        print_statistics("TiDB", &mut combined_stats.tidb_complex_latencies, tidb_complex_elapsed);
        println!();
        print_statistics("TiKV", &mut combined_stats.tikv_complex_latencies, tikv_complex_elapsed);
        println!();
        print_statistics("TiFlash", &mut combined_stats.tiflash_complex_latencies, tiflash_complex_elapsed);

        if !combined_stats.tidb_complex_latencies.is_empty()
            && !combined_stats.tikv_complex_latencies.is_empty()
            && !combined_stats.tiflash_complex_latencies.is_empty() {
            let tidb_complex_qps = combined_stats.tidb_complex_latencies.len() as f64 / tidb_complex_elapsed.as_secs_f64();
            let tikv_complex_qps = combined_stats.tikv_complex_latencies.len() as f64 / tikv_complex_elapsed.as_secs_f64();
            let tiflash_complex_qps = combined_stats.tiflash_complex_latencies.len() as f64 / tiflash_complex_elapsed.as_secs_f64();
            println!("\n--- Performance Comparison ---");
            println!("TiDB   QPS: {:.2}", tidb_complex_qps);
            println!("TiKV   QPS: {:.2}", tikv_complex_qps);
            println!("TiFlash QPS: {:.2}", tiflash_complex_qps);
            println!("QPS ratio (TiKV/TiDB): {:.2}x", tikv_complex_qps / tidb_complex_qps);
            println!("QPS ratio (TiFlash/TiDB): {:.2}x", tiflash_complex_qps / tidb_complex_qps);
            println!("QPS ratio (TiFlash/TiKV): {:.2}x", tiflash_complex_qps / tikv_complex_qps);
        }

        logger.flush()?;
        println!("\n✓ SQL queries have been saved to: {}", args.output_file);
        return Ok(());
    }

    println!("\n========================================");
    println!("=== SIMPLE QUERIES BENCHMARK ===");
    println!("========================================\n");

    // Phase 1: Benchmark TiDB
    println!("=== Phase 1: Benchmarking TiDB (Simple Queries) ===");
    let tidb_start = Instant::now();
    let mut tidb_handles = Vec::with_capacity(args.concurrency);

    for _ in 0..args.concurrency {
        let executor = executor.clone();
            let sample_data = sample_data.clone();
            let rare_words = rare_words.clone();
        let verbose = args.verbose;

        let handle = tokio::spawn(async move {
                run_benchmark_worker(executor, sample_data, rare_words, tidb_start, duration, QueryEngine::TiDB, verbose).await
        });

        tidb_handles.push(handle);
    }

    let mut tidb_stats = ThreadStats::new();
    for handle in tidb_handles {
        tidb_stats.merge(handle.await?);
    }
    let tidb_elapsed = tidb_start.elapsed();
    println!("✓ TiDB simple queries completed in {:.2?}\n", tidb_elapsed);

    // Phase 2: Benchmark TiKV
    println!("=== Phase 2: Benchmarking TiKV (Simple Queries) ===");
    let tikv_start = Instant::now();
    let mut tikv_handles = Vec::with_capacity(args.concurrency);

    for _ in 0..args.concurrency {
        let executor = executor.clone();
            let sample_data = sample_data.clone();
            let rare_words = rare_words.clone();
        let verbose = args.verbose;

        let handle = tokio::spawn(async move {
                run_benchmark_worker(executor, sample_data, rare_words, tikv_start, duration, QueryEngine::TiKV, verbose).await
        });

        tikv_handles.push(handle);
    }

    let mut tikv_stats = ThreadStats::new();
    for handle in tikv_handles {
        tikv_stats.merge(handle.await?);
    }
    let tikv_elapsed = tikv_start.elapsed();
    println!("✓ TiKV simple queries completed in {:.2?}\n", tikv_elapsed);

    // Phase 3: Benchmark TiFlash
    println!("=== Phase 3: Benchmarking TiFlash (Simple Queries) ===");
    let tiflash_start = Instant::now();
    let mut tiflash_handles = Vec::with_capacity(args.concurrency);

    for _ in 0..args.concurrency {
        let executor = executor.clone();
            let sample_data = sample_data.clone();
            let rare_words = rare_words.clone();
        let verbose = args.verbose;

        let handle = tokio::spawn(async move {
                run_benchmark_worker(executor, sample_data, rare_words, tiflash_start, duration, QueryEngine::TiFlash, verbose).await
        });

        tiflash_handles.push(handle);
    }

    let mut tiflash_stats = ThreadStats::new();
    for handle in tiflash_handles {
        tiflash_stats.merge(handle.await?);
    }
    let tiflash_elapsed = tiflash_start.elapsed();
    println!("✓ TiFlash simple queries completed in {:.2?}\n", tiflash_elapsed);

    // Summary (simple only)
    let total_elapsed = tidb_elapsed + tikv_elapsed + tiflash_elapsed;
    let mut combined_stats = ThreadStats::new();
    combined_stats.tidb_latencies = tidb_stats.tidb_latencies;
    combined_stats.tikv_latencies = tikv_stats.tikv_latencies;
    combined_stats.tiflash_latencies = tiflash_stats.tiflash_latencies;
    combined_stats.errors = tidb_stats.errors + tikv_stats.errors + tiflash_stats.errors;

    println!("\n=== Summary ===");
    println!("Query Type     : Simple");
    println!("TiDB elapsed   : {:.2?}", tidb_elapsed);
    println!("TiKV elapsed   : {:.2?}", tikv_elapsed);
    println!("TiFlash elapsed: {:.2?}", tiflash_elapsed);
    println!("Total elapsed  : {:.2?}", total_elapsed);
    println!("Total errors   : {}\n", combined_stats.errors);

    println!("--- Query Statistics ---");
    print_statistics("TiDB", &mut combined_stats.tidb_latencies, tidb_elapsed);
    println!();
    print_statistics("TiKV", &mut combined_stats.tikv_latencies, tikv_elapsed);
    println!();
    print_statistics("TiFlash", &mut combined_stats.tiflash_latencies, tiflash_elapsed);

    if !combined_stats.tidb_latencies.is_empty()
        && !combined_stats.tikv_latencies.is_empty()
        && !combined_stats.tiflash_latencies.is_empty() {
        let tidb_qps = combined_stats.tidb_latencies.len() as f64 / tidb_elapsed.as_secs_f64();
        let tikv_qps = combined_stats.tikv_latencies.len() as f64 / tikv_elapsed.as_secs_f64();
        let tiflash_qps = combined_stats.tiflash_latencies.len() as f64 / tiflash_elapsed.as_secs_f64();
        println!("\n--- Performance Comparison ---");
        println!("TiDB   QPS: {:.2}", tidb_qps);
        println!("TiKV   QPS: {:.2}", tikv_qps);
        println!("TiFlash QPS: {:.2}", tiflash_qps);
        println!("QPS ratio (TiKV/TiDB): {:.2}x", tikv_qps / tidb_qps);
        println!("QPS ratio (TiFlash/TiDB): {:.2}x", tiflash_qps / tidb_qps);
        println!("QPS ratio (TiFlash/TiKV): {:.2}x", tiflash_qps / tikv_qps);
    }

    logger.flush()?;
    println!("\n✓ SQL queries have been saved to: {}", args.output_file);

    Ok(())
}
