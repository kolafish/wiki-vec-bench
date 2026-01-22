use clap::Parser;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySql, Pool};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use chrono::Local;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::process::Command;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow_array::{ArrayRef, StringArray, ListArray, Float32Array};

#[derive(Parser, Debug)]
#[command(author, version, about = "TiDB OLTP benchmark for wiki_paragraphs_embeddings")]
struct Args {
    /// insert-only or update-mixed
    #[arg(long, value_parser = ["insert-only", "update-mixed"])]
    mode: String,

    /// Number of concurrent workers
    #[arg(short, long, default_value_t = 16)]
    concurrency: usize,

    /// Benchmark duration in seconds
    #[arg(short, long, default_value_t = 60)]
    duration: u64,

    /// Whether to create FULLTEXT index on (title, text)
    #[arg(long, default_value_t = false)]
    build_index: bool,

    /// Enable verbose logging (print individual SQLs)
    #[arg(long, default_value_t = false)]
    verbose: bool,

    /// Use randomly generated in-memory samples instead of parquet data
    #[arg(long, default_value_t = false)]
    use_random_data: bool,
}

struct ThreadStats {
    latencies: Vec<u128>,
    errors: u64,
    rows: u64,
    inserts: u64,
    updates: u64,
}

impl ThreadStats {
    fn new() -> Self {
        Self {
            latencies: Vec::with_capacity(4096),
            errors: 0,
            rows: 0,
            inserts: 0,
            updates: 0,
        }
    }
}

#[derive(Clone)]
struct SampleRow {
    title: String,
    text: String,
    vector: String,
}

#[derive(Copy, Clone)]
enum OpKind {
    Insert,
    Update,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("--- TiDB wiki_paragraphs_embeddings OLTP benchmark ---");
    println!("mode           : {}", args.mode);
    println!("concurrency    : {}", args.concurrency);
    println!("duration       : {}s", args.duration);
    println!("build_index    : {}", args.build_index);
    println!("use_random_data: {}", args.use_random_data);

    // Global TiDB connection settings
    // Equivalent to: mysql --comments --host 127.0.0.1 --port 4000 -u root test
    let opts = sqlx::mysql::MySqlConnectOptions::new()
        .host("127.0.0.1")
        .port(4000)
        .username("root")
        .database("test")
        .charset("utf8mb4");

    let pool = MySqlPoolOptions::new()
        .max_connections(args.concurrency as u32 + 8)
        .connect_with(opts)
        .await?;

    // Ensure local data samples exist (download from HF when missing) and load them,
    // or generate random samples in memory when requested.
    let samples = if args.use_random_data {
        let generated = generate_random_samples(200_000);
        println!("generated {} random samples in memory", generated.len());
        Arc::new(generated)
    } else {
        let loaded = ensure_samples()?;
        println!("loaded {} samples from ./data", loaded.len());
        Arc::new(loaded)
    };

    // Create table with timestamp suffix and optional FULLTEXT index automatically
    let table_name = Arc::new(create_table_with_index(&pool, args.build_index).await?);
    println!("using table: {}", table_name);

    let id_counter = Arc::new(AtomicU64::new(1));
    let start_time = Instant::now();
    let run_duration = Duration::from_secs(args.duration);
    let mut handles = Vec::with_capacity(args.concurrency);

    for _ in 0..args.concurrency {
        let pool = pool.clone();
        let mode = args.mode.clone();
        let verbose = args.verbose;
        let id_counter = id_counter.clone();
        let table_name = table_name.clone();
        let samples = samples.clone();

        let handle = tokio::spawn(async move {
            let mut rng = StdRng::from_entropy();
            let mut stats = ThreadStats::new();

            while start_time.elapsed() < run_duration {
                let op_start = Instant::now();
                let result = if mode == "insert-only" {
                    run_insert(&pool, &table_name, &samples, &mut rng, &id_counter, verbose).await
                } else {
                    run_update_mixed(&pool, &table_name, &samples, &mut rng, &id_counter, verbose).await
                };

                match result {
                    Ok((rows, kind)) => {
                        let micros = op_start.elapsed().as_micros();
                        stats.latencies.push(micros);
                        stats.rows += rows as u64;
                        match kind {
                            OpKind::Insert => stats.inserts += 1,
                            OpKind::Update => stats.updates += 1,
                        }
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
    let mut total_inserts = 0u64;
    let mut total_updates = 0u64;

    for h in handles {
        let s = h.await?;
        all_lat.extend(s.latencies);
        total_errors += s.errors;
        total_rows += s.rows;
        total_inserts += s.inserts;
        total_updates += s.updates;
    }

    let elapsed = start_time.elapsed();
    let txn_count = all_lat.len() as u64;
    let tps = txn_count as f64 / elapsed.as_secs_f64();
    let index_rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    let mix_total = total_inserts + total_updates;
    let (insert_pct, update_pct) = if mix_total > 0 {
        (
            (total_inserts as f64 / mix_total as f64) * 100.0,
            (total_updates as f64 / mix_total as f64) * 100.0,
        )
    } else {
        (0.0, 0.0)
    };

    println!();
    println!("=== Summary ===");
    println!("Mode           : {}", args.mode);
    println!("Elapsed        : {:.2?}", elapsed);
    println!("Total txns     : {}", txn_count);
    println!("Total rows     : {}", total_rows);
    println!("Total errors   : {}", total_errors);
    println!("OLTP (txn/s)   : {:.2}", tps);
    println!("Index row/s    : {:.2}", index_rows_per_sec);
    println!(
        "Mix ratio      : inserts={} ({:.1}%), updates={} ({:.1}%)",
        total_inserts, insert_pct, total_updates, update_pct
    );

    print_percentiles("write", &mut all_lat, tps);

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

async fn run_insert(
    pool: &Pool<MySql>,
    table_name: &str,
    samples: &Arc<Vec<SampleRow>>,
    rng: &mut impl Rng,
    id_counter: &AtomicU64,
    verbose: bool,
) -> Result<(u32, OpKind), sqlx::Error> {
    let id = id_counter.fetch_add(1, Ordering::Relaxed) as i64;
    let wiki_id = id;
    let paragraph_id = (id % 1000) as i32;

    let sample = pick_sample(rng, samples);
    let title = sample.title.clone();
    let text = sample.text.clone();
    let url = format!("https://example.com/wiki/{}", wiki_id);
    let views: f64 = rng.gen_range(0.0..1_000_000.0);
    let langs: i32 = rng.gen_range(1..10);
    let vector = sample.vector.clone();

    let sql = format!(
        r#"
        INSERT INTO `{table}`
            (id, wiki_id, paragraph_id, title, text, url, views, langs, vector)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    "#,
        table = table_name
    );

    if verbose {
        println!("INSERT id={}", id);
    }

    sqlx::query(&sql)
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

    Ok((1, OpKind::Insert))
}

async fn run_update_mixed(
    pool: &Pool<MySql>,
    table_name: &str,
    samples: &Arc<Vec<SampleRow>>,
    rng: &mut impl Rng,
    id_counter: &AtomicU64,
    verbose: bool,
) -> Result<(u32, OpKind), sqlx::Error> {
    let current_max = id_counter.load(Ordering::Relaxed);
    // If there is no data yet, fall back to insert
    if current_max <= 1 || rng.gen_bool(0.5) {
        return run_insert(pool, table_name, samples, rng, id_counter, verbose).await;
    }

    let target_id = rng.gen_range(1..current_max) as i64;
    let extra_views: f64 = rng.gen_range(0.0..100.0);
    let sample = pick_sample(rng, samples);
    let new_text = sample.text.clone();
    let new_vector = sample.vector.clone();

    let sql = format!(
        r#"
        UPDATE `{table}`
        SET views = COALESCE(views, 0) + ?, text = ?, vector = ?
        WHERE id = ?
    "#,
        table = table_name
    );

    if verbose {
        println!("UPDATE id={}", target_id);
    }

    let result = sqlx::query(&sql)
        .bind(extra_views)
        .bind(new_text)
        .bind(new_vector)
        .bind(target_id)
        .execute(pool)
        .await?;

    Ok((result.rows_affected() as u32, OpKind::Update))
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

fn generate_random_samples(count: usize) -> Vec<SampleRow> {
    let mut rng = StdRng::seed_from_u64(42);
    let mut samples = Vec::with_capacity(count);

    for _ in 0..count {
        let title = generate_text(&mut rng, 32);
        let text = generate_text(&mut rng, 256);
        let vector = generate_vector_string(&mut rng);
        samples.push(SampleRow { title, text, vector });
    }

    samples
}

/// Create a new table with timestamp suffix and optionally add FULLTEXT index on (title, text).
async fn create_table_with_index(pool: &Pool<MySql>, build_index: bool) -> Result<String, sqlx::Error> {
    let ts = Local::now().format("%Y%m%d%H%M%S").to_string();
    let table_name = format!("wiki_paragraphs_embeddings_{}", ts);

    let create_sql = format!(
        r#"
        CREATE TABLE IF NOT EXISTS `{table}` (
          id            BIGINT       NOT NULL,
          wiki_id       BIGINT       NOT NULL,
          paragraph_id  INT          NOT NULL,
          title         VARCHAR(512) NOT NULL,
          text          TEXT         NOT NULL,
          url           VARCHAR(512) NOT NULL,
          views         DOUBLE       NULL,
          langs         INT          NULL,
          vector        TEXT         NOT NULL,
          PRIMARY KEY (id),
          KEY idx_wiki_para (wiki_id, paragraph_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        "#,
        table = table_name
    );

    // Create table
    sqlx::query(&create_sql).execute(pool).await?;

    if build_index {
        let alter_sql = format!(
            r#"
            ALTER TABLE `{table}`
              ADD FULLTEXT INDEX ft_index (title, text) WITH PARSER standard;
            "#,
            table = table_name
        );

        // Try to add FULLTEXT index; ignore error if it already exists
        if let Err(e) = sqlx::query(&alter_sql).execute(pool).await {
            eprintln!("warning: failed to add FULLTEXT index: {}", e);
        }
    }

    Ok(table_name)
}

fn ensure_samples() -> Result<Vec<SampleRow>, Box<dyn Error>> {
    let data_dir = Path::new("data");
    let raw_dir = data_dir.join("raw");

    // Ensure directory exists
    std::fs::create_dir_all(&raw_dir)?;

    // Check if there is at least one parquet file; if not, download.
    let parquet_files: Vec<_> = std::fs::read_dir(&raw_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();

    if parquet_files.is_empty() {
        println!("no parquet files under ./data/raw, downloading dataset via Python script...");
        let status = Command::new("python3")
            .arg("scripts/download_wiki_embeddings.py")
            .status()?;
        if !status.success() {
            return Err("python scripts/download_wiki_embeddings.py failed".into());
        }
    }

    // Try to load samples from existing parquet files
    match load_samples_from_parquet(&raw_dir, 200_000) {
        Ok(samples) if !samples.is_empty() => {
            Ok(samples)
        },
        result => {
            // Either Ok with empty samples or Err
            if let Err(ref e) = result {
                eprintln!("warning: failed to load samples from existing parquet files: {:?}", e);
            } else {
                eprintln!("warning: loaded empty samples from existing parquet files");
            }
            eprintln!("checking and re-downloading missing or corrupted shards...");
            
            // Run the download script, which will intelligently check existing files
            // and only download missing/corrupted shards
            let status = Command::new("python3")
                .arg("scripts/download_wiki_embeddings.py")
                .status()?;
            if !status.success() {
                return Err("python scripts/download_wiki_embeddings.py failed during re-download".into());
            }
            
            // Try loading again after re-download
            let samples = load_samples_from_parquet(&raw_dir, 200_000)?;
            if samples.is_empty() {
                return Err("no samples loaded after re-download".into());
            }
            println!("successfully loaded {} samples after re-download", samples.len());
            Ok(samples)
        }
    }
}

fn pick_sample<'a>(rng: &mut impl Rng, samples: &'a [SampleRow]) -> &'a SampleRow {
    let idx = rng.gen_range(0..samples.len());
    &samples[idx]
}

fn load_samples_from_parquet(
    raw_dir: &Path,
    max_rows: usize,
) -> Result<Vec<SampleRow>, Box<dyn Error>> {
    let mut samples = Vec::new();
    let mut corrupted_files = Vec::new();

    let mut entries: Vec<_> = std::fs::read_dir(raw_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();

    if entries.is_empty() {
        return Err("no parquet files found under ./data/raw".into());
    }

    // Deterministic order
    entries.sort();
    let total_files = entries.len();
    let mut loaded_files = 0;

    for path in entries {
        if samples.len() >= max_rows {
            break;
        }
        
        let file_name = path.file_name().unwrap_or_default().to_string_lossy().to_string();
        println!("loading samples from {:?}", path);
        
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("warning: failed to open {:?}: {}, skipping", path, e);
                corrupted_files.push(file_name);
                continue;
            }
        };
        
        let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("warning: failed to read parquet file {:?}: {}, skipping", path, e);
                corrupted_files.push(file_name);
                continue;
            }
        };
        
        let mut reader = match builder.with_batch_size(1024).build() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("warning: failed to build parquet reader for {:?}: {}, skipping", path, e);
                corrupted_files.push(file_name);
                continue;
            }
        };

        let mut file_loaded = false;
        while let Some(batch_result) = reader.next() {
            let batch = match batch_result {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("warning: failed to read batch from {:?}: {}, skipping remaining batches", path, e);
                    if !file_loaded {
                        corrupted_files.push(file_name.clone());
                    }
                    break;
                }
            };
            
            // Use match to handle schema errors gracefully
            let (title_idx, text_idx, emb_idx) = match (
                batch.schema().index_of("title"),
                batch.schema().index_of("text"),
                batch.schema().index_of("emb"),
            ) {
                (Ok(t), Ok(txt), Ok(e)) => (t, txt, e),
                _ => {
                    eprintln!("warning: parquet file {:?} has invalid schema, skipping", path);
                    if !file_loaded {
                        corrupted_files.push(file_name.clone());
                    }
                    break;
                }
            };

            let title_arr = match batch.column(title_idx).as_any().downcast_ref::<StringArray>() {
                Some(arr) => arr,
                None => {
                    eprintln!("warning: title column in {:?} is not StringArray, skipping file", path);
                    if !file_loaded {
                        corrupted_files.push(file_name.clone());
                    }
                    break;
                }
            };
            
            let text_arr = match batch.column(text_idx).as_any().downcast_ref::<StringArray>() {
                Some(arr) => arr,
                None => {
                    eprintln!("warning: text column in {:?} is not StringArray, skipping file", path);
                    if !file_loaded {
                        corrupted_files.push(file_name.clone());
                    }
                    break;
                }
            };
            
            let emb_arr = match batch.column(emb_idx).as_any().downcast_ref::<ListArray>() {
                Some(arr) => arr,
                None => {
                    eprintln!("warning: emb column in {:?} is not ListArray, skipping file", path);
                    if !file_loaded {
                        corrupted_files.push(file_name.clone());
                    }
                    break;
                }
            };

            for row in 0..batch.num_rows() {
                if samples.len() >= max_rows {
                    break;
                }

                let title = title_arr.value(row).to_string();
                let text = text_arr.value(row).to_string();

                let values: ArrayRef = emb_arr.value(row);
                let float_arr = match values.as_any().downcast_ref::<Float32Array>() {
                    Some(arr) => arr,
                    None => {
                        eprintln!("warning: emb values in {:?} are not Float32Array, skipping row", path);
                        continue;
                    }
                };

                let mut vector = String::new();
                for i in 0..float_arr.len() {
                    let v = float_arr.value(i);
                    if i > 0 {
                        vector.push(',');
                    }
                    vector.push_str(&format!("{:.6}", v));
                }

                samples.push(SampleRow { title, text, vector });
                file_loaded = true;
            }
        }
        
        if file_loaded {
            loaded_files += 1;
        }
    }

    if !corrupted_files.is_empty() {
        eprintln!("warning: {} of {} parquet files were corrupted or unreadable:", 
                  corrupted_files.len(), total_files);
        for file in &corrupted_files {
            eprintln!("  - {}", file);
        }
    }

    if samples.is_empty() {
        return Err("no samples loaded from parquet files".into());
    }

    println!("successfully loaded {} samples from {}/{} parquet files", 
             samples.len(), loaded_files, total_files);

    Ok(samples)
}



