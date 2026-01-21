use clap::Parser;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::{MySql, Pool};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(author, version, about = "TiDB OLTP benchmark for wiki_paragraphs_embeddings")]
struct Args {
    /// TiDB connection URL, e.g. mysql://user:pwd@host:4000/db
    #[arg(short, long)]
    url: String,

    /// insert-only or update-mixed
    #[arg(long, value_parser = ["insert-only", "update-mixed"])]
    mode: String,

    /// Number of concurrent workers
    #[arg(short, long, default_value_t = 16)]
    concurrency: usize,

    /// Benchmark duration in seconds
    #[arg(short, long, default_value_t = 60)]
    duration: u64,

    /// Logical writer count label (0 / 1 / 2), only used for reporting
    #[arg(long, default_value_t = 0)]
    writers: u32,

    /// Enable verbose logging (print individual SQLs)
    #[arg(long, default_value_t = false)]
    verbose: bool,
}

struct ThreadStats {
    latencies: Vec<u128>,
    errors: u64,
    rows: u64,
}

impl ThreadStats {
    fn new() -> Self {
        Self {
            latencies: Vec::with_capacity(4096),
            errors: 0,
            rows: 0,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("--- TiDB wiki_paragraphs_embeddings OLTP benchmark ---");
    println!("mode        : {}", args.mode);
    println!("writers     : {}", args.writers);
    println!("concurrency : {}", args.concurrency);
    println!("duration    : {}s", args.duration);

    let opts = MySqlConnectOptions::from_str(&args.url)?;
    let pool = MySqlPoolOptions::new()
        .max_connections(args.concurrency as u32 + 8)
        .connect_timeout(Duration::from_secs(10))
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(opts)
        .await?;

    let id_counter = Arc::new(AtomicU64::new(1));
    let start_time = Instant::now();
    let run_duration = Duration::from_secs(args.duration);
    let mut handles = Vec::with_capacity(args.concurrency);

    for _ in 0..args.concurrency {
        let pool = pool.clone();
        let mode = args.mode.clone();
        let verbose = args.verbose;
        let id_counter = id_counter.clone();

        let handle = tokio::spawn(async move {
            let mut rng = StdRng::from_entropy();
            let mut stats = ThreadStats::new();

            while start_time.elapsed() < run_duration {
                let op_start = Instant::now();
                let result = if mode == "insert-only" {
                    run_insert(&pool, &mut rng, &id_counter, verbose).await
                } else {
                    run_update_mixed(&pool, &mut rng, &id_counter, verbose).await
                };

                match result {
                    Ok(rows) => {
                        let micros = op_start.elapsed().as_micros();
                        stats.latencies.push(micros);
                        stats.rows += rows as u64;
                    }
                    Err(e) => {
                        eprintln!("worker error: {}", e);
                        stats.errors += 1;
                    }
                }
            }

            stats
        });

        handles.push(handle);
    }

    let mut all_lat = Vec::new();
    let mut total_errors = 0u64;
    let mut total_rows = 0u64;

    for h in handles {
        let s = h.await?;
        all_lat.extend(s.latencies);
        total_errors += s.errors;
        total_rows += s.rows;
    }

    let elapsed = start_time.elapsed();
    let txn_count = all_lat.len() as u64;
    let tps = txn_count as f64 / elapsed.as_secs_f64();
    let index_rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();

    println!();
    println!("=== Summary ===");
    println!("Mode           : {}", args.mode);
    println!("Writers        : {}", args.writers);
    println!("Elapsed        : {:.2?}", elapsed);
    println!("Total txns     : {}", txn_count);
    println!("Total rows     : {}", total_rows);
    println!("Total errors   : {}", total_errors);
    println!("OLTP (txn/s)   : {:.2}", tps);
    println!("Index row/s    : {:.2}", index_rows_per_sec);

    print_percentiles("write", &mut all_lat);

    Ok(())
}

fn print_percentiles(name: &str, latencies: &mut Vec<u128>) {
    if latencies.is_empty() {
        println!("{}: no samples", name);
        return;
    }
    latencies.sort_unstable();
    let len = latencies.len() as f64;
    let p50 = latencies[(len * 0.50) as usize];
    let p95 = latencies[(len * 0.95) as usize.min(latencies.len() - 1)];
    let p99 = latencies[(len * 0.99) as usize.min(latencies.len() - 1)];
    let max = *latencies.last().unwrap();

    let to_ms = |us: u128| us as f64 / 1000.0;
    println!(
        "{:<10} count={} p50={:.2}ms p95={:.2}ms p99={:.2}ms max={:.2}ms",
        name,
        latencies.len(),
        to_ms(p50),
        to_ms(p95),
        to_ms(p99),
        to_ms(max)
    );
}

async fn run_insert(
    pool: &Pool<MySql>,
    rng: &mut impl Rng,
    id_counter: &AtomicU64,
    verbose: bool,
) -> Result<u32, sqlx::Error> {
    let id = id_counter.fetch_add(1, Ordering::Relaxed) as i64;
    let wiki_id = id;
    let paragraph_id = (id % 1000) as i32;

    let title = format!("Sample title {}", id);
    let text = generate_text(rng, 512);
    let url = format!("https://example.com/wiki/{}", wiki_id);
    let views: f64 = rng.gen_range(0.0..1_000_000.0);
    let langs: i32 = rng.gen_range(1..10);
    let vector = generate_vector_string(rng);

    let sql = r#"
        INSERT INTO wiki_paragraphs_embeddings
            (id, wiki_id, paragraph_id, title, text, url, views, langs, vector)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    "#;

    if verbose {
        println!("INSERT id={}", id);
    }

    sqlx::query(sql)
        .bind(id)
        .bind(wiki_id)
        .bind(paragraph_id)
        .bind(title)
        .bind(text)
        .bind(url)
        .bind(views)
        .bind(langs)
        .bind(vector)
        .execute(pool)
        .await?;

    Ok(1)
}

async fn run_update_mixed(
    pool: &Pool<MySql>,
    rng: &mut impl Rng,
    id_counter: &AtomicU64,
    verbose: bool,
) -> Result<u32, sqlx::Error> {
    let current_max = id_counter.load(Ordering::Relaxed);
    // If there is no data yet, fall back to insert
    if current_max <= 1 || rng.gen_bool(0.5) {
        return run_insert(pool, rng, id_counter, verbose).await;
    }

    let target_id = rng.gen_range(1..current_max) as i64;
    let extra_views: f64 = rng.gen_range(0.0..100.0);
    let new_text = generate_text(rng, 256);
    let new_vector = generate_vector_string(rng);

    let sql = r#"
        UPDATE wiki_paragraphs_embeddings
        SET views = COALESCE(views, 0) + ?, text = ?, vector = ?
        WHERE id = ?
    "#;

    if verbose {
        println!("UPDATE id={}", target_id);
    }

    let result = sqlx::query(sql)
        .bind(extra_views)
        .bind(new_text)
        .bind(new_vector)
        .bind(target_id)
        .execute(pool)
        .await?;

    Ok(result.rows_affected() as u32)
}

fn generate_text(rng: &mut impl Rng, target_len: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz ";
    let mut s = String::with_capacity(target_len);
    for _ in 0..target_len {
        let ch = ALPHABET[rng.gen_range(0..ALPHABET.len())] as char;
        s.push(ch);
    }
    s
}

fn generate_vector_string(rng: &mut impl Rng) -> String {
    // 384-dim float vector, serialized to comma-separated string
    const DIM: usize = 384;
    let mut out = String::with_capacity(DIM * 8);
    for i in 0..DIM {
        let v: f32 = rng.gen_range(-0.2..0.2);
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format!("{:.6}", v));
    }
    out
}

