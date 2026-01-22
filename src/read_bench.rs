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
    // Use TiFlash by SQL hint
    let query = format!(
        r#"
        SELECT /*+ read_from_storage(tiflash[`{}`]) */ count(*) 
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
        println!("SQL: SELECT /*+ read_from_storage(tiflash[`{}`]) */ count(*) FROM `{}` WHERE fts_match_word(title, '{}') OR fts_match_word(text, '{}');",
            table_name, table_name, search_word, search_word
        );
    }
    
    Ok(duration)
}
