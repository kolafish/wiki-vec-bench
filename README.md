# wiki-vec-bench

Rust benchmark tool for running write and read workloads against a MySQL-compatible endpoint (e.g., TiDB).

- `wiki-vec-bench`: Write workload benchmark (insert-only / update-mixed)
- `read-bench`: Read workload benchmark using `fts_match_word` queries, comparing TiDB vs TiFlash performance

## Prerequisites

- Rust toolchain (`rustc`, `cargo`)
- A MySQL-compatible database (e.g., TiDB) with an existing database/schema (e.g., `test`)

## Build

```bash
rustc --version
cargo --version

cd wiki-vec-bench
cargo build --release
```

## Run

The benchmark connects to TiDB using the following fixed settings:

- host: `127.0.0.1`
- port: `4000`
- user: `root`
- database: `test`

On startup it will (by default):

- download the `maloyan/wikipedia-22-12-en-embeddings-all-MiniLM-L6-v2` dataset
  from Hugging Face (if there are no parquet files under `./data/raw` yet),
  and materialize multiple parquet shards under `./data/raw`;
- load sampled `title/text/vector` rows from these parquet files into memory;
- create a table named `wiki_paragraphs_embeddings_YYYYMMDDHHMMSS`;
- optionally add a FULLTEXT index on `(title, text)` if `--build-index` is provided.

Alternatively, you can skip the dataset download and use randomly generated
in-memory samples by passing `--use-random-data true`.

```bash
# Insert-only workload without FULLTEXT index (default)
./target/release/wiki-vec-bench \
  --mode insert-only \
  --concurrency 16 \
  --duration 60 \
  --use-random-data

# Insert-only workload with FULLTEXT index
./target/release/wiki-vec-bench \
  --mode insert-only \
  --concurrency 16 \
  --duration 60 \
  --build-index \
  --use-random-data

# Update-mixed workload without FULLTEXT index
./target/release/wiki-vec-bench \
  --mode update-mixed \
  --concurrency 16 \
  --duration 60 \
  --use-random-data

# Update-mixed workload with FULLTEXT index
./target/release/wiki-vec-bench \
  --mode update-mixed \
  --concurrency 16 \
  --duration 60 \
  --build-index \
  --use-random-data
```

## Read Benchmark (read-bench)

The read benchmark compares query performance between TiDB and TiFlash execution engines:
- **TiDB**: Uses `fts_match_word()` with FULLTEXT index for fast text search
- **TiFlash**: Uses `LIKE` pattern matching with columnar storage optimization

Features:
- Automatically finds the table with the latest timestamp (matching `wiki_paragraphs_embeddings_*`)
- Creates TiFlash replica if missing and waits for sync
- Samples test data (title, text) from the table
- Runs optimized queries on both engines:
  - TiDB: `fts_match_word()` with FULLTEXT index
  - TiFlash: `LIKE` pattern matching (columnar scan)
- Outputs performance statistics: QPS, p50, p95, p99 latencies
- Logs all SQL queries to file for analysis

```bash
# Run read benchmark with default settings
./target/release/read-bench \
  --concurrency 16 \
  --duration 60 \
  --sample-size 2000

# Run with verbose logging (prints individual SQL queries)
./target/release/read-bench \
  --concurrency 16 \
  --duration 60 \
  --sample-size 2000 \
  --verbose
```

The benchmark will output:
- TiDB statistics (QPS, p50, p95, p99, max latency)
- TiFlash statistics (QPS, p50, p95, p99, max latency)
- Comparison ratio (TiFlash/TiDB QPS)
