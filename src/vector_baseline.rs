//! TiDB Vector Baseline Benchmark - Vector Index on TiFlash

use clap::Parser;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::{MySql, Pool, Row, Column};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const DEFAULT_DB_HOST: &str = "127.0.0.1";
const DEFAULT_DB_PORT: u16 = 4000;
const DEFAULT_DB_USER: &str = "root";
const DEFAULT_DB_NAME: &str = "test";
const TIFLASH_SYNC_TIMEOUT_SECS: u64 = 300;
const TIFLASH_CHECK_INTERVAL_SECS: u64 = 2;
const INDEX_BUILD_TIMEOUT_SECS: u64 = 600;

#[derive(Parser, Debug)]
#[command(author, version, about = "TiDB vector baseline benchmark using TiFlash vector index")]
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

    /// Enable concurrent inserts during benchmark
    #[arg(long, default_value_t = false)]
    realtime_insert: bool,

    /// Output file path for SQL queries log
    #[arg(long, default_value = "vector_baseline_queries.sql")]
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
        writeln!(writer, "-- TiDB Vector Baseline SQL Queries")?;
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

fn build_baseline_query(table_name: &str, vector_query: &str) -> String {
    let escaped = escape_sql_string(vector_query);
    format!(
        "SELECT id FROM `{}` ORDER BY VEC_COSINE_DISTANCE(embedding, '{}') LIMIT 10",
        table_name, escaped
    )
}

async fn find_latest_table(pool: &Pool<MySql>, db_name: &str) -> Result<String, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT table_name FROM information_schema.tables
         WHERE table_schema = ? AND table_name LIKE 'wiki_paragraphs_embeddings_%'
         ORDER BY table_name DESC LIMIT 1",
    )
    .bind(db_name)
    .fetch_optional(pool)
    .await
    .map(|opt| opt.unwrap_or_default())
}

async fn fetch_sample_vectors(
    pool: &Pool<MySql>,
    table_name: &str,
    limit: usize,
) -> Result<Vec<SampleVector>, sqlx::Error> {
    let query = format!("SELECT vector FROM `{}` WHERE TRIM(vector) <> '' LIMIT {}", table_name, limit);
    sqlx::query_as(&query).fetch_all(pool).await
}

fn infer_vector_dim(vector: &str) -> usize {
    vector
        .split(',')
        .filter(|v| !v.trim().is_empty())
        .count()
}

fn wrap_vector_literal(vector: &str) -> String {
    format!("[{}]", vector)
}

fn build_random_vector_string(rng: &mut impl rand::Rng, dim: usize) -> String {
    let mut out = String::with_capacity(dim * 8);
    for i in 0..dim {
        let v: f32 = rng.gen_range(-0.2..0.2);
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format!("{:.6}", v));
    }
    out
}

async fn run_realtime_inserts(
    pool: Pool<MySql>,
    table_name: String,
    vector_dim: usize,
    start_time: Instant,
    duration: Duration,
) {
    let mut rng = StdRng::from_entropy();
    while start_time.elapsed() < duration {
        let id = rng.gen::<i64>().abs();
        let vector_text = build_random_vector_string(&mut rng, vector_dim);
        let sql = format!(
            "INSERT INTO `{}` (id, vector_text, embedding) VALUES (?, ?, VEC_FROM_TEXT(?))",
            table_name
        );
        let vector_literal = wrap_vector_literal(&vector_text);
        if let Err(e) = sqlx::query(&sql)
            .bind(id)
            .bind(&vector_text)
            .bind(&vector_literal)
            .execute(&pool)
            .await
        {
            eprintln!("realtime insert error: {}", e);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn extract_timestamp_suffix(table_name: &str) -> Option<&str> {
    table_name.rsplit_once('_').map(|(_, suffix)| suffix)
}

async fn create_baseline_table(
    pool: &Pool<MySql>,
    table_name: &str,
    vector_dim: usize,
) -> Result<(), sqlx::Error> {
    let create_sql = format!(
        r#"
        CREATE TABLE IF NOT EXISTS `{table}` (
          id           BIGINT NOT NULL,
          vector_text  TEXT   NOT NULL,
          embedding    VECTOR({dim}),
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        "#,
        table = table_name,
        dim = vector_dim
    );
    sqlx::query(&create_sql).execute(pool).await?;
    Ok(())
}

async fn insert_baseline_data(
    pool: &Pool<MySql>,
    src_table: &str,
    dst_table: &str,
) -> Result<u64, sqlx::Error> {
    const BATCH_SIZE: i64 = 5000;
    let mut total_rows = 0u64;
    let mut last_id: i64 = 0;

    loop {
        let insert_sql = format!(
            r#"
            INSERT INTO `{dst}` (id, vector_text, embedding)
            SELECT id, vector, VEC_FROM_TEXT(CONCAT('[', vector, ']'))
            FROM `{src}`
            WHERE vector IS NOT NULL AND TRIM(vector) <> '' AND id > ?
            ORDER BY id
            LIMIT ?;
            "#,
            src = src_table,
            dst = dst_table
        );

        let result = sqlx::query(&insert_sql)
            .bind(last_id)
            .bind(BATCH_SIZE)
            .execute(pool)
            .await?;
        let rows = result.rows_affected();
        if rows == 0 {
            break;
        }
        total_rows += rows;

        let max_id_sql = format!(
            r#"
            SELECT MAX(id) FROM `{dst}`
            "#,
            dst = dst_table
        );
        let max_id: Option<i64> = sqlx::query_scalar(&max_id_sql).fetch_one(pool).await?;
        if let Some(id) = max_id {
            last_id = id;
        } else {
            break;
        }

        println!("inserted rows so far: {}", total_rows);
    }

    Ok(total_rows)
}

async fn create_vector_index(pool: &Pool<MySql>, table_name: &str) -> Result<(), sqlx::Error> {
    let sql = format!(
        "ALTER TABLE `{}` ADD VECTOR INDEX idx_embedding ((VEC_COSINE_DISTANCE(embedding)))",
        table_name
    );
    if let Err(e) = sqlx::query(&sql).execute(pool).await {
        eprintln!("vector index creation failed: {}", sql);
        return Err(e);
    }
    Ok(())
}

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
    db_name: String,
}

impl TiFlashReplicaManager {
    fn new(pool: Pool<MySql>, db_name: String) -> Self {
        Self { pool, db_name }
    }

    async fn ensure_replica(&self, table_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let info = self.get_replica_info(table_name).await?;

        match info {
            Some(info) if info.is_ready() => {
                println!(
                    "✓ TiFlash replica exists and is synced for table: {} (progress: {:.2}%)",
                    table_name,
                    info.progress.unwrap_or(1.0) * 100.0
                );
                Ok(())
            }
            Some(info) if info.needs_wait() => {
                println!(
                    "⏳ TiFlash replica exists but not fully synced (progress: {:.2}%), waiting...",
                    info.progress.unwrap_or(0.0) * 100.0
                );
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
             WHERE table_schema = ? AND table_name = ?",
        )
        .bind(&self.db_name)
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
                )
                .into());
            }

            let info = self.get_replica_info(table_name).await?;
            if let Some(info) = info {
                if info.is_ready() {
                    println!(
                        "\n✓ TiFlash replica is fully synced (progress: {:.2}%)",
                        info.progress.unwrap_or(1.0) * 100.0
                    );
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

            std::io::stdout().flush().ok();
            tokio::time::sleep(interval).await;
        }
    }
}

#[derive(sqlx::FromRow)]
struct IndexProgress {
    rows_stable_indexed: Option<i64>,
    rows_stable_not_indexed: Option<i64>,
}

async fn table_exists(pool: &Pool<MySql>, db_name: &str, table_name: &str) -> Result<bool, sqlx::Error> {
    let exists: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
    )
    .bind(db_name)
    .bind(table_name)
    .fetch_optional(pool)
    .await?;
    Ok(exists.is_some())
}

async fn table_has_rows(pool: &Pool<MySql>, table_name: &str) -> Result<bool, sqlx::Error> {
    let query = format!("SELECT 1 FROM `{}` LIMIT 1", table_name);
    let exists: Option<i64> = sqlx::query_scalar(&query).fetch_optional(pool).await?;
    Ok(exists.is_some())
}

fn find_column<'a>(cols: &'a [sqlx::mysql::MySqlColumn], candidates: &[&str]) -> Option<&'a str> {
    for col in cols {
        let name = col.name();
        if candidates.iter().any(|c| name.eq_ignore_ascii_case(c)) {
            return Some(name);
        }
    }
    None
}

async fn get_index_progress(
    pool: &Pool<MySql>,
    db_name: &str,
    table_name: &str,
    index_name: &str,
) -> Result<Option<IndexProgress>, Box<dyn std::error::Error>> {
    let sql = "SELECT * FROM information_schema.tiflash_indexes";
    let rows = sqlx::query(sql)
        .fetch_all(pool)
        .await
        .map_err(|e| {
            eprintln!("index progress query failed: {}", sql);
            e
        })?;

    if rows.is_empty() {
        return Ok(None);
    }

    let cols = rows[0].columns();
    let db_col = find_column(cols, &["table_schema", "db_name", "database", "tidb_database"])
        .ok_or_else(|| {
            let names: Vec<&str> = cols.iter().map(|c| c.name()).collect();
            format!("missing database name column in tiflash_indexes: {:?}", names)
        })?;
    let table_col = find_column(cols, &["table_name", "table", "tbl_name", "tidb_table"])
        .ok_or_else(|| {
            let names: Vec<&str> = cols.iter().map(|c| c.name()).collect();
            format!("missing table name column in tiflash_indexes: {:?}", names)
        })?;
    let index_col = find_column(cols, &["index_name", "index", "idx_name"])
        .ok_or_else(|| {
            let names: Vec<&str> = cols.iter().map(|c| c.name()).collect();
            format!("missing index name column in tiflash_indexes: {:?}", names)
        })?;
    let indexed_col = find_column(cols, &["rows_stable_indexed"])
        .ok_or_else(|| {
            let names: Vec<&str> = cols.iter().map(|c| c.name()).collect();
            format!("missing rows_stable_indexed column in tiflash_indexes: {:?}", names)
        })?;
    let not_indexed_col = find_column(cols, &["rows_stable_not_indexed"])
        .ok_or_else(|| {
            let names: Vec<&str> = cols.iter().map(|c| c.name()).collect();
            format!("missing rows_stable_not_indexed column in tiflash_indexes: {:?}", names)
        })?;

    let db_col = db_col.to_string();
    let table_col = table_col.to_string();
    let index_col = index_col.to_string();
    let indexed_col = indexed_col.to_string();
    let not_indexed_col = not_indexed_col.to_string();

    for row in rows.iter() {
        let db_val: String = row.try_get(db_col.as_str())?;
        let table_val: String = row.try_get(table_col.as_str())?;
        let index_val: String = row.try_get(index_col.as_str())?;
        if db_val == db_name && table_val == table_name && index_val == index_name {
            let indexed: i64 = row.try_get(indexed_col.as_str())?;
            let not_indexed: i64 = row.try_get(not_indexed_col.as_str())?;
            return Ok(Some(IndexProgress {
                rows_stable_indexed: Some(indexed),
                rows_stable_not_indexed: Some(not_indexed),
            }));
        }
    }

    Ok(None)
}

async fn wait_for_index_build(
    pool: &Pool<MySql>,
    db_name: &str,
    table_name: &str,
    index_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let timeout = Duration::from_secs(INDEX_BUILD_TIMEOUT_SECS);
    let interval = Duration::from_secs(TIFLASH_CHECK_INTERVAL_SECS);

    loop {
        if start.elapsed() > timeout {
            return Err(format!(
                "Timeout: index '{}' for '{}' not built within {} seconds",
                index_name, table_name, INDEX_BUILD_TIMEOUT_SECS
            )
            .into());
        }

        let progress = get_index_progress(pool, db_name, table_name, index_name).await?;

        if let Some(p) = progress {
            let indexed = p.rows_stable_indexed.unwrap_or(0);
            let not_indexed = p.rows_stable_not_indexed.unwrap_or(0);
            if not_indexed == 0 {
                println!(
                    "\n✓ Vector index build completed (indexed={})",
                    indexed
                );
                return Ok(());
            }
            print!(
                "\r⏳ Building vector index... indexed={} not_indexed={}",
                indexed, not_indexed
            );
        } else {
            print!("\r⏳ Waiting for vector index metadata...");
        }

        std::io::stdout().flush().ok();
        tokio::time::sleep(interval).await;
    }
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
        let query_vector = wrap_vector_literal(&sample.vector);
        let query = build_baseline_query(&table_name, &query_vector);
        let start = Instant::now();
        let result: Result<Vec<sqlx::mysql::MySqlRow>, sqlx::Error> =
            sqlx::query(&query).fetch_all(&pool).await;
        let elapsed = start.elapsed();
        logger.log_query(&query, elapsed);

        match result {
            Ok(rows) => {
                stats.latencies.push(elapsed.as_micros());
                if verbose {
                    println!(
                        "Time: {:?} | Rows: {} | Vector size: {}",
                        elapsed.as_millis(),
                        rows.len(),
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

    println!("--- TiDB vector baseline benchmark (TiFlash vector index) ---");
    println!("concurrency    : {}", args.concurrency);
    println!("duration       : {}s", args.duration);
    println!("sample_size    : {}", args.sample_size);
    println!("output_file    : {}", args.output_file);
    println!("realtime_insert: {}", args.realtime_insert);

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

    let src_table = find_latest_table(&pool, &args.db_name).await?;
    if src_table.is_empty() {
        return Err("No table found matching pattern wiki_paragraphs_embeddings_*".into());
    }
    println!("source table: {}", src_table);

    println!("> Sampling {} vectors from source table...", args.sample_size);
    let samples = fetch_sample_vectors(&pool, &src_table, args.sample_size).await?;
    if samples.is_empty() {
        return Err("Source table has no valid vectors".into());
    }
    println!("> Successfully loaded {} samples\n", samples.len());

    let vector_dim = infer_vector_dim(&samples[0].vector);
    if vector_dim == 0 {
        return Err("Failed to infer vector dimension".into());
    }
    println!("inferred vector dimension: {}", vector_dim);

    let suffix = extract_timestamp_suffix(&src_table)
        .ok_or("Failed to extract timestamp suffix from source table")?;
    let baseline_table = format!("vector_baseline_{}", suffix);
    println!("baseline table: {}", baseline_table);

    if !table_exists(&pool, &args.db_name, &baseline_table).await? {
        create_baseline_table(&pool, &baseline_table, vector_dim).await?;
    } else {
        println!("baseline table already exists, reusing it");
    }

    if table_has_rows(&pool, &baseline_table).await? {
        println!("baseline table already has data, skipping insert");
    } else {
        let inserted = insert_baseline_data(&pool, &src_table, &baseline_table).await?;
        println!("inserted rows: {}", inserted);
    }

    let replica_mgr = TiFlashReplicaManager::new(pool.clone(), args.db_name.clone());
    replica_mgr.ensure_replica(&baseline_table).await?;

    if let Some(progress) = get_index_progress(&pool, &args.db_name, &baseline_table, "idx_embedding").await? {
        let not_indexed = progress.rows_stable_not_indexed.unwrap_or(0);
        if not_indexed == 0 {
            println!("vector index already exists and is fully built");
        } else {
            println!("vector index exists, waiting for build to complete...");
            wait_for_index_build(&pool, &args.db_name, &baseline_table, "idx_embedding").await?;
        }
    } else {
        println!("> Creating vector index on TiFlash...");
        create_vector_index(&pool, &baseline_table).await?;
        wait_for_index_build(&pool, &args.db_name, &baseline_table, "idx_embedding").await?;
    }

    let logger = Arc::new(QueryLogger::new(&args.output_file, &baseline_table)?);
    let samples = Arc::new(samples);

    println!("> Warm-up: running one vector query...");
    if let Some(sample) = samples.first() {
        let query_vector = wrap_vector_literal(&sample.vector);
        let warmup_query = build_baseline_query(&baseline_table, &query_vector);
        let warmup_start = Instant::now();
        let warmup_result: Result<Vec<sqlx::mysql::MySqlRow>, sqlx::Error> =
            sqlx::query(&warmup_query).fetch_all(&pool).await;
        let warmup_elapsed = warmup_start.elapsed();
        logger.log_query(&warmup_query, warmup_elapsed);
        match warmup_result {
            Ok(rows) => {
                println!(
                    "✓ Warm-up done: {:?} (rows={})",
                    warmup_elapsed, rows.len()
                );
            }
            Err(e) => {
                eprintln!("Warm-up query error: {}", e);
            }
        }
    }

    let start_time = Instant::now();
    let duration = Duration::from_secs(args.duration);
    if args.realtime_insert {
        let pool = pool.clone();
        let table_name = baseline_table.clone();
        tokio::spawn(async move {
            run_realtime_inserts(pool, table_name, vector_dim, start_time, duration).await
        });
    }
    let mut handles = Vec::with_capacity(args.concurrency);
    for _ in 0..args.concurrency {
        let pool = pool.clone();
        let table_name = baseline_table.clone();
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
