use arrow_array::{Array, Float32Array, Int32Array, Int64Array, ListArray, StringArray};
use chrono::Local;
use clap::{Parser, ValueEnum};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rand::prelude::SliceRandom;
use rand::rngs::StdRng;
use rand::SeedableRng;
use serde::Serialize;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use sqlx::{MySql, Pool};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

const DEFAULT_DB_HOST: &str = "127.0.0.1";
const DEFAULT_DB_PORT: u16 = 4000;
const DEFAULT_DB_USER: &str = "root";
const DEFAULT_DB_PASSWORD: &str = "";
const DEFAULT_DB_NAME: &str = "test";
const DEFAULT_VECTOR_DIM: usize = 384;
const MAX_TITLE_KW_CHARS: usize = 256;
const HYBRID_INDEX_NAME: &str = "idx_hybrid";
const TIFLASH_CHECK_INTERVAL_SECS: u64 = 2;
const INDEX_BUILD_TIMEOUT_SECS: u64 = 7200;
const QUERY_READINESS_SAMPLE_LIMIT: usize = 128;
const QUERY_READINESS_SOAK_CONCURRENCY: usize = 4;
const QUERY_READINESS_SOAK_DURATION_SECS: u64 = 5;
const QUERY_READINESS_SUCCESS_ROUNDS: usize = 2;
const QUERY_READINESS_MAX_ATTEMPTS: usize = 3;
const DEFAULT_QUERY_TIMEOUT_SECS: u64 = 30;

#[derive(Parser, Debug)]
#[command(author, version, about = "AWS hybrid benchmark for TiDB + TiCI")]
struct Args {
    #[arg(long, default_value = DEFAULT_DB_HOST)]
    db_host: String,

    #[arg(long, default_value_t = DEFAULT_DB_PORT)]
    db_port: u16,

    #[arg(long, default_value = DEFAULT_DB_NAME)]
    db_name: String,

    #[arg(long, default_value = DEFAULT_DB_USER)]
    db_user: String,

    #[arg(long, default_value = DEFAULT_DB_PASSWORD)]
    db_password: String,

    #[arg(long, default_value = "data/raw")]
    dataset_dir: String,

    #[arg(long)]
    scale_rows: usize,

    #[arg(long)]
    existing_table: Option<String>,

    #[arg(long, default_value_t = 500)]
    batch_size: usize,

    #[arg(long, default_value_t = 16)]
    query_concurrency: usize,

    #[arg(long, default_value_t = 120)]
    query_duration: u64,

    #[arg(long, default_value_t = DEFAULT_QUERY_TIMEOUT_SECS)]
    query_timeout_secs: u64,

    #[arg(long, default_value_t = 5000)]
    sample_size: usize,

    #[arg(long, default_value = "results")]
    output_dir: String,

    #[arg(long, default_value_t = false)]
    keep_tables: bool,

    #[arg(long, default_value_t = false, alias = "skip-fts")]
    skip_inverted: bool,

    #[arg(long, default_value_t = false)]
    skip_vector: bool,

    #[arg(long, default_value_t = 16)]
    hnsw_m: usize,

    #[arg(long, default_value_t = 200)]
    hnsw_ef_construction: usize,

    #[arg(long, default_value_t = 10)]
    vector_topk: usize,

    #[arg(long, value_enum, default_value_t = InvertedOperator::Eq)]
    inverted_operator: InvertedOperator,
}

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum InvertedOperator {
    Eq,
    Ne,
}

impl InvertedOperator {
    fn sql_operator(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Ne => "<>",
        }
    }
}

#[derive(Clone, Debug)]
struct HybridInputRow {
    id: i64,
    title_kw: String,
    payload: String,
    vector: String,
}

#[derive(Clone, Debug, sqlx::FromRow)]
struct SampleRow {
    title_kw: String,
    vector_text: String,
}

#[derive(Default)]
struct LoadAccumulator {
    total_rows: usize,
    batch_latencies_ms: Vec<f64>,
    vector_dim: usize,
}

#[derive(Default)]
struct QueryAccumulator {
    latencies_us: Vec<u128>,
    errors: usize,
}

#[derive(Serialize)]
struct LatencySummary {
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

#[derive(Serialize)]
struct LatencyBucket {
    upper_bound_ms: Option<f64>,
    label: String,
    count: usize,
    percentage: f64,
}

#[derive(Serialize)]
struct LoadSummary {
    elapsed_secs: f64,
    rows: usize,
    rows_per_sec: f64,
    batches: usize,
    batch_latency_ms: LatencySummary,
}

#[derive(Serialize)]
struct IndexSummary {
    elapsed_secs: f64,
}

#[derive(Serialize)]
struct QuerySummary {
    elapsed_secs: f64,
    queries: usize,
    qps: f64,
    errors: usize,
    latency_ms: LatencySummary,
    latency_distribution: Vec<LatencyBucket>,
}

#[derive(Serialize)]
struct RunReport {
    scale_rows: usize,
    dataset_dir: String,
    db_name: String,
    hybrid_table: String,
    index_name: String,
    reused_existing_table: bool,
    inverted_operator: InvertedOperator,
    vector_topk: usize,
    started_at: String,
    completed_at: String,
    hybrid_load: Option<LoadSummary>,
    hybrid_index: Option<IndexSummary>,
    hybrid_catchup: Option<IndexSummary>,
    inverted_query: Option<QuerySummary>,
    vector_query: Option<QuerySummary>,
}

#[derive(sqlx::FromRow)]
struct TiciShardProgress {
    shard_count: Option<i64>,
    ready_fragment_count: Option<i64>,
    ready_row_count: Option<i64>,
}

async fn get_tici_shard_progress(
    pool: &Pool<MySql>,
    db_name: &str,
    table_name: &str,
) -> Result<TiciShardProgress, sqlx::Error> {
    sqlx::query_as(
        r#"
        SELECT
          COUNT(*) AS shard_count,
          CAST(
            SUM(
              CASE
                WHEN JSON_EXTRACT(s.manifest, '$.fragments[0].f.property.count') IS NOT NULL THEN 1
                ELSE 0
              END
            ) AS SIGNED
          ) AS ready_fragment_count,
          CAST(
            SUM(
              CASE
                WHEN JSON_EXTRACT(s.manifest, '$.fragments[0].f.property.count') IS NOT NULL THEN
                  CAST(JSON_UNQUOTE(JSON_EXTRACT(s.manifest, '$.fragments[0].f.property.count')) AS SIGNED)
                ELSE 0
              END
            ) AS SIGNED
          ) AS ready_row_count
        FROM tici.tici_shard_meta s
        JOIN information_schema.tables t
          ON t.tidb_table_id = s.table_id
        WHERE t.table_schema = ? AND t.table_name = ?
        "#,
    )
    .bind(db_name)
    .bind(table_name)
    .fetch_one(pool)
    .await
}

async fn get_table_row_count(pool: &Pool<MySql>, table_name: &str) -> Result<i64, sqlx::Error> {
    let sql = format!("SELECT COUNT(*) FROM `{}`", table_name);
    sqlx::query_scalar(&sql).fetch_one(pool).await
}

async fn wait_for_tici_shards_ready(
    pool: &Pool<MySql>,
    db_name: &str,
    table_name: &str,
) -> Result<IndexSummary, Box<dyn std::error::Error>> {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(INDEX_BUILD_TIMEOUT_SECS) {
        let progress = get_tici_shard_progress(pool, db_name, table_name).await?;
        if progress.ready_fragment_count.unwrap_or_default() > 0
            && progress.ready_row_count.unwrap_or_default() > 0
        {
            return Ok(IndexSummary {
                elapsed_secs: start.elapsed().as_secs_f64(),
            });
        }
        print!(
            "\r⏳ Waiting for TiCI shards on {}: shards={} ready_fragments={} ready_rows={}",
            table_name,
            progress.shard_count.unwrap_or_default(),
            progress.ready_fragment_count.unwrap_or_default(),
            progress.ready_row_count.unwrap_or_default()
        );
        io::stdout().flush().ok();
        tokio::time::sleep(Duration::from_secs(TIFLASH_CHECK_INTERVAL_SECS)).await;
    }
    Err(format!("timeout waiting for TiCI shards on {}", table_name).into())
}

fn build_inverted_query_sql(
    table_name: &str,
    title_kw: &str,
    operator: InvertedOperator,
) -> String {
    format!(
        "SELECT count(*) FROM `{}` USE INDEX (`{}`) WHERE title_kw {} '{}'",
        table_name,
        HYBRID_INDEX_NAME,
        operator.sql_operator(),
        escape_sql_string(title_kw)
    )
}

fn build_vector_query_sql(table_name: &str, sample_vector: &str, topk: usize) -> String {
    format!(
        "SELECT id FROM `{}` ORDER BY VEC_COSINE_DISTANCE(embedding, '{}') LIMIT {}",
        table_name,
        escape_sql_string(&wrap_vector_literal(sample_vector)),
        topk
    )
}

fn select_readiness_samples<F>(
    sample_rows: &[SampleRow],
    limit: usize,
    predicate: F,
) -> Vec<SampleRow>
where
    F: Fn(&SampleRow) -> bool,
{
    let filtered: Vec<SampleRow> = sample_rows
        .iter()
        .filter(|row| predicate(row))
        .cloned()
        .collect();
    if filtered.len() <= limit {
        return filtered;
    }

    let step = filtered.len() as f64 / limit as f64;
    let mut selected = Vec::with_capacity(limit);
    for idx in 0..limit {
        let pos = ((idx as f64) * step).floor() as usize;
        selected.push(filtered[pos.min(filtered.len() - 1)].clone());
    }
    selected
}

async fn wait_for_inverted_query_stable(
    pool: Pool<MySql>,
    table_name: &str,
    sample_rows: &[SampleRow],
    query_concurrency: usize,
    operator: InvertedOperator,
) -> Result<(), Box<dyn std::error::Error>> {
    let readiness_samples =
        select_readiness_samples(sample_rows, QUERY_READINESS_SAMPLE_LIMIT, |row| {
            !row.title_kw.is_empty()
        });
    if readiness_samples.is_empty() {
        return Err("no non-empty title_kw samples available".into());
    }

    let soak_concurrency = query_concurrency.clamp(1, QUERY_READINESS_SOAK_CONCURRENCY);
    let soak_duration = Duration::from_secs(QUERY_READINESS_SOAK_DURATION_SECS);
    let deadline = Instant::now() + Duration::from_secs(INDEX_BUILD_TIMEOUT_SECS);
    let mut stable_rounds = 0usize;
    let mut attempts = 0usize;

    while Instant::now() < deadline {
        attempts += 1;
        let summary = run_inverted_query_benchmark(
            pool.clone(),
            table_name.to_string(),
            readiness_samples.clone(),
            soak_concurrency,
            soak_duration,
            Duration::from_secs(DEFAULT_QUERY_TIMEOUT_SECS),
            operator,
        )
        .await?;

        if summary.errors == 0 && summary.queries > 0 {
            stable_rounds += 1;
            if stable_rounds >= QUERY_READINESS_SUCCESS_ROUNDS {
                return Ok(());
            }
        } else {
            stable_rounds = 0;
        }

        print!(
            "\r⏳ Stabilizing inverted path on {}: queries={} errors={} stable_rounds={}/{}",
            table_name,
            summary.queries,
            summary.errors,
            stable_rounds,
            QUERY_READINESS_SUCCESS_ROUNDS
        );
        io::stdout().flush().ok();
        if attempts >= QUERY_READINESS_MAX_ATTEMPTS {
            println!(
                "\n! Continuing inverted benchmark on {} despite readiness errors after {} attempts",
                table_name, attempts
            );
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(TIFLASH_CHECK_INTERVAL_SECS)).await;
    }

    Err(format!("timeout stabilizing inverted query path on {}", table_name).into())
}

async fn wait_for_vector_query_stable(
    pool: Pool<MySql>,
    table_name: &str,
    sample_rows: &[SampleRow],
    query_concurrency: usize,
    topk: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let readiness_samples =
        select_readiness_samples(sample_rows, QUERY_READINESS_SAMPLE_LIMIT, |row| {
            !row.vector_text.is_empty()
        });
    if readiness_samples.is_empty() {
        return Err("no vector samples available".into());
    }

    let soak_concurrency = query_concurrency.clamp(1, QUERY_READINESS_SOAK_CONCURRENCY);
    let soak_duration = Duration::from_secs(QUERY_READINESS_SOAK_DURATION_SECS);
    let deadline = Instant::now() + Duration::from_secs(INDEX_BUILD_TIMEOUT_SECS);
    let mut stable_rounds = 0usize;
    let mut attempts = 0usize;

    while Instant::now() < deadline {
        attempts += 1;
        let summary = run_vector_query_benchmark(
            pool.clone(),
            table_name.to_string(),
            readiness_samples.clone(),
            soak_concurrency,
            soak_duration,
            Duration::from_secs(DEFAULT_QUERY_TIMEOUT_SECS),
            topk,
        )
        .await?;

        if summary.errors == 0 && summary.queries > 0 {
            stable_rounds += 1;
            if stable_rounds >= QUERY_READINESS_SUCCESS_ROUNDS {
                return Ok(());
            }
        } else {
            stable_rounds = 0;
        }

        print!(
            "\r⏳ Stabilizing vector path on {}: queries={} errors={} stable_rounds={}/{}",
            table_name,
            summary.queries,
            summary.errors,
            stable_rounds,
            QUERY_READINESS_SUCCESS_ROUNDS
        );
        io::stdout().flush().ok();
        if attempts >= QUERY_READINESS_MAX_ATTEMPTS {
            println!(
                "\n! Continuing vector benchmark on {} despite readiness errors after {} attempts",
                table_name, attempts
            );
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(TIFLASH_CHECK_INTERVAL_SECS)).await;
    }

    Err(format!("timeout stabilizing vector query path on {}", table_name).into())
}

fn escape_sql_string(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\'', "''")
        .replace('"', "\\\"")
        .replace('\0', "\\0")
}

fn wrap_vector_literal(vector: &str) -> String {
    format!("[{}]", vector)
}

fn normalize_title_kw(title: &str, id: i64) -> String {
    let compact = title.split_whitespace().collect::<Vec<_>>().join(" ");
    let trimmed = compact.trim();
    let candidate = if trimmed.is_empty() {
        format!("doc_{}", id)
    } else {
        trimmed.to_string()
    };
    candidate.chars().take(MAX_TITLE_KW_CHARS).collect()
}

fn build_latency_summary(latencies_ms: &mut [f64]) -> LatencySummary {
    if latencies_ms.is_empty() {
        return LatencySummary {
            p50_ms: 0.0,
            p95_ms: 0.0,
            p99_ms: 0.0,
            max_ms: 0.0,
        };
    }
    latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let len = latencies_ms.len();
    LatencySummary {
        p50_ms: latencies_ms[len / 2],
        p95_ms: latencies_ms[(len * 95 / 100).min(len - 1)],
        p99_ms: latencies_ms[(len * 99 / 100).min(len - 1)],
        max_ms: *latencies_ms.last().unwrap_or(&0.0),
    }
}

fn build_latency_distribution(latencies_ms: &[f64]) -> Vec<LatencyBucket> {
    const BUCKET_BOUNDS_MS: [f64; 10] =
        [1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0];

    let mut counts = vec![0usize; BUCKET_BOUNDS_MS.len() + 1];
    for &latency_ms in latencies_ms {
        let mut bucket_idx = BUCKET_BOUNDS_MS.len();
        for (idx, bound_ms) in BUCKET_BOUNDS_MS.iter().enumerate() {
            if latency_ms <= *bound_ms {
                bucket_idx = idx;
                break;
            }
        }
        counts[bucket_idx] += 1;
    }

    let total = latencies_ms.len() as f64;
    let mut buckets = Vec::with_capacity(counts.len());
    let mut lower_bound_ms = 0.0;
    for (idx, count) in counts.into_iter().enumerate() {
        let (upper_bound_ms, label) = if idx < BUCKET_BOUNDS_MS.len() {
            let upper = BUCKET_BOUNDS_MS[idx];
            let label = if idx == 0 {
                format!("<= {:.0}ms", upper)
            } else {
                format!("({:.0}, {:.0}]ms", lower_bound_ms, upper)
            };
            lower_bound_ms = upper;
            (Some(upper), label)
        } else {
            (None, format!("> {:.0}ms", lower_bound_ms))
        };
        buckets.push(LatencyBucket {
            upper_bound_ms,
            label,
            count,
            percentage: if total > 0.0 {
                count as f64 * 100.0 / total
            } else {
                0.0
            },
        });
    }
    buckets
}

fn create_hybrid_table_name(scale_rows: usize) -> String {
    format!(
        "wiki_bench_hybrid_{}_{}",
        scale_rows,
        Local::now().format("%Y%m%d%H%M%S")
    )
}

async fn create_hybrid_table(
    pool: &Pool<MySql>,
    table_name: &str,
    vector_dim: usize,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        r#"
        CREATE TABLE IF NOT EXISTS `{table}` (
          id          BIGINT       NOT NULL,
          title_kw    VARCHAR(512) NOT NULL,
          payload     TEXT         NOT NULL,
          vector_text TEXT         NOT NULL,
          embedding   VECTOR({dim}),
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        "#,
        table = table_name,
        dim = vector_dim
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

async fn create_hybrid_index(
    pool: &Pool<MySql>,
    table_name: &str,
    hnsw_m: usize,
    hnsw_ef_construction: usize,
) -> Result<IndexSummary, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let sql = format!(
        r#"
        CREATE HYBRID INDEX `{index_name}`
        ON `{table_name}`(title_kw, embedding)
        PARAMETER '{{
          "inverted": {{
            "columns": ["title_kw"]
          }},
          "vector": [{{
            "columns": ["embedding"],
            "index_info": {{
              "distance_metric": "COSINE",
              "hnsw_m": {hnsw_m},
              "hnsw_ef_construction": {hnsw_ef_construction}
            }}
          }}],
          "sharding_key": {{
            "columns": ["title_kw"]
          }}
        }}'
        "#,
        index_name = HYBRID_INDEX_NAME,
        table_name = table_name,
        hnsw_m = hnsw_m,
        hnsw_ef_construction = hnsw_ef_construction
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(IndexSummary {
        elapsed_secs: start.elapsed().as_secs_f64(),
    })
}

fn build_hybrid_insert_sql(table_name: &str, rows: &[HybridInputRow]) -> String {
    let mut sql = format!(
        "INSERT INTO `{}` (id, title_kw, payload, vector_text, embedding) VALUES ",
        table_name
    );
    for (idx, row) in rows.iter().enumerate() {
        if idx > 0 {
            sql.push(',');
        }
        let vector_literal = wrap_vector_literal(&row.vector);
        sql.push('(');
        sql.push_str(&format!(
            "{}, '{}', '{}', '{}', VEC_FROM_TEXT('{}')",
            row.id,
            escape_sql_string(&row.title_kw),
            escape_sql_string(&row.payload),
            escape_sql_string(&row.vector),
            escape_sql_string(&vector_literal)
        ));
        sql.push(')');
    }
    sql
}

async fn insert_hybrid_batch(
    pool: &Pool<MySql>,
    table_name: &str,
    rows: &[HybridInputRow],
) -> Result<(), sqlx::Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let sql = build_hybrid_insert_sql(table_name, rows);
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

fn extract_i64(
    row_idx: usize,
    int64: Option<&Int64Array>,
    int32: Option<&Int32Array>,
) -> Option<i64> {
    if let Some(arr) = int64 {
        if !arr.is_null(row_idx) {
            return Some(arr.value(row_idx));
        }
    }
    if let Some(arr) = int32 {
        if !arr.is_null(row_idx) {
            return Some(arr.value(row_idx) as i64);
        }
    }
    None
}

fn read_string(arr: &StringArray, row_idx: usize) -> String {
    if arr.is_null(row_idx) {
        String::new()
    } else {
        arr.value(row_idx).to_string()
    }
}

fn format_vector(arr: &Float32Array) -> String {
    let mut vector = String::with_capacity(arr.len() * 10);
    for idx in 0..arr.len() {
        if idx > 0 {
            vector.push(',');
        }
        vector.push_str(&format!("{:.6}", arr.value(idx)));
    }
    vector
}

async fn load_dataset_into_hybrid_table(
    pool: &Pool<MySql>,
    table_name: &str,
    dataset_dir: &Path,
    scale_rows: usize,
    batch_size: usize,
) -> Result<LoadSummary, Box<dyn std::error::Error>> {
    let mut entries: Vec<PathBuf> = fs::read_dir(dataset_dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("parquet"))
        .collect();
    entries.sort();
    if entries.is_empty() {
        return Err(format!("no parquet files found under {}", dataset_dir.display()).into());
    }

    let start = Instant::now();
    let mut acc = LoadAccumulator::default();
    let mut batch_rows = Vec::with_capacity(batch_size);

    'files: for path in entries {
        let file = File::open(&path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut reader = builder.with_batch_size(1024).build()?;

        while let Some(batch_result) = reader.next() {
            let batch = batch_result?;
            let schema = batch.schema();

            let id_idx = schema.index_of("id")?;
            let title_idx = schema.index_of("title")?;
            let text_idx = schema.index_of("text")?;
            let emb_idx = schema.index_of("emb")?;

            let id_i32 = batch.column(id_idx).as_any().downcast_ref::<Int32Array>();
            let id_i64 = batch.column(id_idx).as_any().downcast_ref::<Int64Array>();
            let title_arr = batch
                .column(title_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("title column is not StringArray")?;
            let text_arr = batch
                .column(text_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("text column is not StringArray")?;
            let emb_arr = batch
                .column(emb_idx)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or("emb column is not ListArray")?;

            for row_idx in 0..batch.num_rows() {
                if acc.total_rows >= scale_rows {
                    break 'files;
                }

                let id = extract_i64(row_idx, id_i64, id_i32).unwrap_or(acc.total_rows as i64 + 1);
                let title_kw = normalize_title_kw(&read_string(title_arr, row_idx), id);
                let payload = read_string(text_arr, row_idx);

                let values = emb_arr.value(row_idx);
                let float_arr = values
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or("emb values are not Float32Array")?;
                if acc.vector_dim == 0 {
                    acc.vector_dim = float_arr.len();
                }
                let vector = format_vector(float_arr);

                batch_rows.push(HybridInputRow {
                    id,
                    title_kw,
                    payload,
                    vector,
                });

                if batch_rows.len() >= batch_size {
                    let insert_start = Instant::now();
                    insert_hybrid_batch(pool, table_name, &batch_rows).await?;
                    acc.batch_latencies_ms
                        .push(insert_start.elapsed().as_secs_f64() * 1000.0);
                    acc.total_rows += batch_rows.len();
                    batch_rows.clear();
                    if acc.total_rows % (batch_size * 20) == 0 || acc.total_rows == scale_rows {
                        println!(
                            "loaded rows into {}: {} / {}",
                            table_name, acc.total_rows, scale_rows
                        );
                    }
                }
            }
        }
    }

    if !batch_rows.is_empty() {
        let insert_start = Instant::now();
        insert_hybrid_batch(pool, table_name, &batch_rows).await?;
        acc.batch_latencies_ms
            .push(insert_start.elapsed().as_secs_f64() * 1000.0);
        acc.total_rows += batch_rows.len();
    }

    if acc.total_rows < scale_rows {
        return Err(format!(
            "dataset rows are insufficient: requested {}, loaded {}",
            scale_rows, acc.total_rows
        )
        .into());
    }

    let elapsed = start.elapsed();
    let mut latencies = acc.batch_latencies_ms.clone();
    Ok(LoadSummary {
        elapsed_secs: elapsed.as_secs_f64(),
        rows: acc.total_rows,
        rows_per_sec: acc.total_rows as f64 / elapsed.as_secs_f64(),
        batches: latencies.len(),
        batch_latency_ms: build_latency_summary(&mut latencies),
    })
}

async fn fetch_samples(
    pool: &Pool<MySql>,
    table_name: &str,
    limit: usize,
) -> Result<Vec<SampleRow>, sqlx::Error> {
    let sql = format!(
        "SELECT title_kw, vector_text FROM `{}` LIMIT {}",
        table_name, limit
    );
    sqlx::query_as(&sql).fetch_all(pool).await
}

async fn run_inverted_query_benchmark(
    pool: Pool<MySql>,
    table_name: String,
    sample_rows: Vec<SampleRow>,
    concurrency: usize,
    duration: Duration,
    timeout: Duration,
    operator: InvertedOperator,
) -> Result<QuerySummary, Box<dyn std::error::Error>> {
    let sample_rows = Arc::new(sample_rows);
    let started = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let pool = pool.clone();
        let table_name = table_name.clone();
        let sample_rows = sample_rows.clone();
        handles.push(tokio::spawn(async move {
            let mut rng = StdRng::from_entropy();
            let mut acc = QueryAccumulator::default();
            while started.elapsed() < duration {
                let Some(sample) = sample_rows.choose(&mut rng) else {
                    continue;
                };
                let sql = build_inverted_query_sql(&table_name, &sample.title_kw, operator);
                let query_start = Instant::now();
                match tokio::time::timeout(
                    timeout,
                    sqlx::query_scalar::<_, i64>(&sql).fetch_one(&pool),
                )
                .await
                {
                    Ok(Ok(_)) => acc.latencies_us.push(query_start.elapsed().as_micros()),
                    Ok(Err(_)) | Err(_) => acc.errors += 1,
                }
            }
            acc
        }));
    }

    let mut all_latencies_ms = Vec::new();
    let mut errors = 0usize;
    for handle in handles {
        let acc = handle.await?;
        errors += acc.errors;
        all_latencies_ms.extend(
            acc.latencies_us
                .into_iter()
                .map(|value| value as f64 / 1000.0),
        );
    }

    let elapsed = started.elapsed();
    let queries = all_latencies_ms.len();
    let latency_distribution = build_latency_distribution(&all_latencies_ms);
    let latency_ms = build_latency_summary(&mut all_latencies_ms);
    Ok(QuerySummary {
        elapsed_secs: elapsed.as_secs_f64(),
        queries,
        qps: queries as f64 / elapsed.as_secs_f64(),
        errors,
        latency_ms,
        latency_distribution,
    })
}

async fn run_vector_query_benchmark(
    pool: Pool<MySql>,
    table_name: String,
    sample_rows: Vec<SampleRow>,
    concurrency: usize,
    duration: Duration,
    timeout: Duration,
    topk: usize,
) -> Result<QuerySummary, Box<dyn std::error::Error>> {
    let sample_rows = Arc::new(sample_rows);
    let started = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let pool = pool.clone();
        let table_name = table_name.clone();
        let sample_rows = sample_rows.clone();
        handles.push(tokio::spawn(async move {
            let mut rng = StdRng::from_entropy();
            let mut acc = QueryAccumulator::default();
            while started.elapsed() < duration {
                let Some(sample) = sample_rows.choose(&mut rng) else {
                    continue;
                };
                let sql = build_vector_query_sql(&table_name, &sample.vector_text, topk);
                let query_start = Instant::now();
                match tokio::time::timeout(timeout, sqlx::query(&sql).fetch_all(&pool)).await {
                    Ok(Ok(_)) => acc.latencies_us.push(query_start.elapsed().as_micros()),
                    Ok(Err(_)) | Err(_) => acc.errors += 1,
                }
            }
            acc
        }));
    }

    let mut all_latencies_ms = Vec::new();
    let mut errors = 0usize;
    for handle in handles {
        let acc = handle.await?;
        errors += acc.errors;
        all_latencies_ms.extend(
            acc.latencies_us
                .into_iter()
                .map(|value| value as f64 / 1000.0),
        );
    }

    let elapsed = started.elapsed();
    let queries = all_latencies_ms.len();
    let latency_distribution = build_latency_distribution(&all_latencies_ms);
    let latency_ms = build_latency_summary(&mut all_latencies_ms);
    Ok(QuerySummary {
        elapsed_secs: elapsed.as_secs_f64(),
        queries,
        qps: queries as f64 / elapsed.as_secs_f64(),
        errors,
        latency_ms,
        latency_distribution,
    })
}

fn write_csv(report: &RunReport, output_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let csv_path = output_dir.join("summary.csv");
    let mut file = File::create(csv_path)?;
    writeln!(
        file,
        "phase,scale_rows,elapsed_secs,rows,rows_per_sec,queries,qps,p50_ms,p95_ms,p99_ms,max_ms,errors,table"
    )?;
    if let Some(summary) = &report.hybrid_load {
        writeln!(
            file,
            "hybrid_load,{},{:.3},{},{:.3},,,,,,,{}",
            report.scale_rows,
            summary.elapsed_secs,
            summary.rows,
            summary.rows_per_sec,
            report.hybrid_table
        )?;
    }
    if let Some(summary) = &report.hybrid_index {
        writeln!(
            file,
            "hybrid_index,{},{:.3},,,,,,,,,,{}",
            report.scale_rows, summary.elapsed_secs, report.hybrid_table
        )?;
    }
    if let Some(summary) = &report.hybrid_catchup {
        writeln!(
            file,
            "hybrid_catchup,{},{:.3},,,,,,,,,,{}",
            report.scale_rows, summary.elapsed_secs, report.hybrid_table
        )?;
    }
    if let Some(summary) = &report.inverted_query {
        writeln!(
            file,
            "inverted_query,{},{:.3},,,{},{:.3},{:.3},{:.3},{:.3},{:.3},{},{}",
            report.scale_rows,
            summary.elapsed_secs,
            summary.queries,
            summary.qps,
            summary.latency_ms.p50_ms,
            summary.latency_ms.p95_ms,
            summary.latency_ms.p99_ms,
            summary.latency_ms.max_ms,
            summary.errors,
            report.hybrid_table
        )?;
    }
    if let Some(summary) = &report.vector_query {
        writeln!(
            file,
            "vector_query,{},{:.3},,,{},{:.3},{:.3},{:.3},{:.3},{:.3},{},{}",
            report.scale_rows,
            summary.elapsed_secs,
            summary.queries,
            summary.qps,
            summary.latency_ms.p50_ms,
            summary.latency_ms.p95_ms,
            summary.latency_ms.p99_ms,
            summary.latency_ms.max_ms,
            summary.errors,
            report.hybrid_table
        )?;
    }
    Ok(())
}

fn write_markdown(report: &RunReport, output_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let md_path = output_dir.join("summary.md");
    let mut file = File::create(md_path)?;
    writeln!(file, "# AWS Hybrid Benchmark")?;
    writeln!(file)?;
    writeln!(file, "- Scale rows: `{}`", report.scale_rows)?;
    writeln!(file, "- Dataset dir: `{}`", report.dataset_dir)?;
    writeln!(file, "- Database: `{}`", report.db_name)?;
    writeln!(file, "- Hybrid table: `{}`", report.hybrid_table)?;
    writeln!(file, "- Hybrid index: `{}`", report.index_name)?;
    writeln!(
        file,
        "- Reused existing table: `{}`",
        report.reused_existing_table
    )?;
    writeln!(
        file,
        "- Inverted operator: `{}`",
        report.inverted_operator.sql_operator()
    )?;
    writeln!(file, "- Vector topK: `{}`", report.vector_topk)?;

    if let Some(summary) = &report.hybrid_load {
        writeln!(file)?;
        writeln!(file, "## Hybrid Load")?;
        writeln!(
            file,
            "- Rows: `{}` in `{:.3}s`, throughput `{:.3} rows/s`",
            summary.rows, summary.elapsed_secs, summary.rows_per_sec
        )?;
        writeln!(
            file,
            "- Batch latency: p50 `{:.3}ms`, p95 `{:.3}ms`, p99 `{:.3}ms`, max `{:.3}ms`",
            summary.batch_latency_ms.p50_ms,
            summary.batch_latency_ms.p95_ms,
            summary.batch_latency_ms.p99_ms,
            summary.batch_latency_ms.max_ms
        )?;
    }

    if let Some(summary) = &report.hybrid_index {
        writeln!(file)?;
        writeln!(file, "## Hybrid Index")?;
        writeln!(
            file,
            "- DDL time on empty table: `{:.3}s`",
            summary.elapsed_secs
        )?;
    }

    if let Some(summary) = &report.hybrid_catchup {
        writeln!(file)?;
        writeln!(file, "## Hybrid Catch-up")?;
        writeln!(
            file,
            "- CDC catch-up time after inserts: `{:.3}s`",
            summary.elapsed_secs
        )?;
    }

    if let Some(summary) = &report.inverted_query {
        writeln!(file)?;
        writeln!(file, "## Inverted Query")?;
        writeln!(
            file,
            "- Queries: `{}` in `{:.3}s`, QPS `{:.3}`",
            summary.queries, summary.elapsed_secs, summary.qps
        )?;
        writeln!(
            file,
            "- Latency: p50 `{:.3}ms`, p95 `{:.3}ms`, p99 `{:.3}ms`, max `{:.3}ms`, errors `{}`",
            summary.latency_ms.p50_ms,
            summary.latency_ms.p95_ms,
            summary.latency_ms.p99_ms,
            summary.latency_ms.max_ms,
            summary.errors
        )?;
        writeln!(file, "- Distribution:")?;
        for bucket in &summary.latency_distribution {
            writeln!(
                file,
                "  - `{}`: `{}` queries (`{:.2}%`)",
                bucket.label, bucket.count, bucket.percentage
            )?;
        }
    }

    if let Some(summary) = &report.vector_query {
        writeln!(file)?;
        writeln!(file, "## Vector Query")?;
        writeln!(
            file,
            "- Queries: `{}` in `{:.3}s`, QPS `{:.3}`",
            summary.queries, summary.elapsed_secs, summary.qps
        )?;
        writeln!(
            file,
            "- Latency: p50 `{:.3}ms`, p95 `{:.3}ms`, p99 `{:.3}ms`, max `{:.3}ms`, errors `{}`",
            summary.latency_ms.p50_ms,
            summary.latency_ms.p95_ms,
            summary.latency_ms.p99_ms,
            summary.latency_ms.max_ms,
            summary.errors
        )?;
        writeln!(file, "- Distribution:")?;
        for bucket in &summary.latency_distribution {
            writeln!(
                file,
                "  - `{}`: `{}` queries (`{:.2}%`)",
                bucket.label, bucket.count, bucket.percentage
            )?;
        }
    }

    Ok(())
}

async fn drop_table_if_exists(pool: &Pool<MySql>, table_name: &str) -> Result<(), sqlx::Error> {
    let sql = format!("DROP TABLE IF EXISTS `{}`", table_name);
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let started_at = Local::now();

    let mut connect_options = MySqlConnectOptions::new()
        .host(&args.db_host)
        .port(args.db_port)
        .username(&args.db_user)
        .database(&args.db_name)
        .charset("utf8mb4");
    if !args.db_password.is_empty() {
        connect_options = connect_options.password(&args.db_password);
    }

    let pool = MySqlPoolOptions::new()
        .max_connections((args.query_concurrency as u32).saturating_add(16))
        .connect_with(connect_options)
        .await?;

    let (
        table_name,
        effective_scale_rows,
        reused_existing_table,
        hybrid_index,
        hybrid_load,
        hybrid_catchup,
    ) = if let Some(existing_table) = args.existing_table.clone() {
        let row_count = get_table_row_count(&pool, &existing_table).await?;
        if row_count <= 0 {
            return Err(format!("existing table {} is empty", existing_table).into());
        }
        println!(
            "> Reusing existing table {} with {} rows",
            existing_table, row_count
        );
        (existing_table, row_count as usize, true, None, None, None)
    } else {
        let dataset_dir = Path::new(&args.dataset_dir);
        let table_name = create_hybrid_table_name(args.scale_rows);

        println!("> Creating hybrid table {}", table_name);
        create_hybrid_table(&pool, &table_name, DEFAULT_VECTOR_DIM).await?;

        println!("> Creating hybrid index on empty table {}", table_name);
        let hybrid_index =
            create_hybrid_index(&pool, &table_name, args.hnsw_m, args.hnsw_ef_construction).await?;

        println!("> Loading {} rows into {}", args.scale_rows, table_name);
        let hybrid_load = load_dataset_into_hybrid_table(
            &pool,
            &table_name,
            dataset_dir,
            args.scale_rows,
            args.batch_size,
        )
        .await?;

        println!("> Waiting for TiCI shard catch-up on {}", table_name);
        let hybrid_catchup = wait_for_tici_shards_ready(&pool, &args.db_name, &table_name).await?;
        println!();

        (
            table_name,
            args.scale_rows,
            false,
            Some(hybrid_index),
            Some(hybrid_load),
            Some(hybrid_catchup),
        )
    };

    let output_dir =
        PathBuf::from(&args.output_dir).join(format!("scale_{}", effective_scale_rows));
    fs::create_dir_all(&output_dir)?;

    let sample_rows = if args.skip_inverted && args.skip_vector {
        Vec::new()
    } else {
        let rows = fetch_samples(&pool, &table_name, args.sample_size).await?;
        if rows.is_empty() {
            return Err("no samples available after load".into());
        }
        rows
    };

    let inverted_query = if args.skip_inverted {
        None
    } else {
        println!("> Stabilizing inverted query path on {}", table_name);
        wait_for_inverted_query_stable(
            pool.clone(),
            &table_name,
            &sample_rows,
            args.query_concurrency,
            args.inverted_operator,
        )
        .await?;
        println!();
        println!("> Running inverted query benchmark on {}", table_name);
        Some(
            run_inverted_query_benchmark(
                pool.clone(),
                table_name.clone(),
                sample_rows.clone(),
                args.query_concurrency,
                Duration::from_secs(args.query_duration),
                Duration::from_secs(args.query_timeout_secs),
                args.inverted_operator,
            )
            .await?,
        )
    };

    let vector_query = if args.skip_vector {
        None
    } else {
        println!("> Stabilizing vector query path on {}", table_name);
        wait_for_vector_query_stable(
            pool.clone(),
            &table_name,
            &sample_rows,
            args.query_concurrency,
            args.vector_topk,
        )
        .await?;
        println!();
        println!("> Running vector query benchmark on {}", table_name);
        Some(
            run_vector_query_benchmark(
                pool.clone(),
                table_name.clone(),
                sample_rows.clone(),
                args.query_concurrency,
                Duration::from_secs(args.query_duration),
                Duration::from_secs(args.query_timeout_secs),
                args.vector_topk,
            )
            .await?,
        )
    };

    let completed_at = Local::now();
    let report = RunReport {
        scale_rows: effective_scale_rows,
        dataset_dir: args.dataset_dir.clone(),
        db_name: args.db_name.clone(),
        hybrid_table: table_name.clone(),
        index_name: HYBRID_INDEX_NAME.to_string(),
        reused_existing_table,
        inverted_operator: args.inverted_operator,
        vector_topk: args.vector_topk,
        started_at: started_at.to_rfc3339(),
        completed_at: completed_at.to_rfc3339(),
        hybrid_load,
        hybrid_index,
        hybrid_catchup,
        inverted_query,
        vector_query,
    };

    let json_path = output_dir.join("summary.json");
    let json = serde_json::to_string_pretty(&report)?;
    fs::write(json_path, json)?;
    write_csv(&report, &output_dir)?;
    write_markdown(&report, &output_dir)?;

    if !reused_existing_table && !args.keep_tables {
        drop_table_if_exists(&pool, &table_name).await?;
    }

    println!("✓ Results written to {}", output_dir.display());
    Ok(())
}
