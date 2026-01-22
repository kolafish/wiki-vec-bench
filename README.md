# wiki-vec-bench

Comprehensive Rust benchmark tool for evaluating write and read performance on MySQL-compatible databases, with specialized support for TiDB's distributed architecture.

## Components

- **`wiki-vec-bench`**: Write workload benchmark (insert-only / update-mixed modes)
- **`read-bench`**: Read workload benchmark comparing TiDB, TiKV, and TiFlash query performance

## Overview

This benchmark suite is designed to test full-text search and analytical query performance using the Wikipedia embeddings dataset. It demonstrates the performance characteristics of:
- **TiDB**: Full-text search with `fts_match_word()` on FULLTEXT index
- **TiKV**: Row-based storage with `LIKE` pattern matching
- **TiFlash**: Columnar storage optimized for analytical queries

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

The read benchmark compares query performance across three execution engines:
- **TiDB**: Uses `fts_match_word()` with FULLTEXT index for fast text search
- **TiKV**: Uses `LIKE` pattern matching on row-based storage
- **TiFlash**: Uses `LIKE` pattern matching with columnar storage optimization

### Features

- **Multi-Engine Comparison**: Tests TiDB, TiKV, and TiFlash in separate benchmark phases
- **Automatic Table Discovery**: Finds the latest `wiki_paragraphs_embeddings_*` table
- **TiFlash Setup**: Creates TiFlash replica automatically and waits for synchronization
- **Smart Query Distribution**: 
  - 75% of queries target the `title` field
  - 25% of queries target the `text` field
- **Query Types**:
  - **Simple Queries**: Basic text search with count aggregation
  - **Complex Queries** (optional): Metadata filtering, GROUP BY aggregation, and TOP-N queries
- **Detailed Logging**: All SQL queries logged to file with execution time
- **Performance Metrics**: QPS, p50, p95, p99, max latency for each engine

### Benchmark Phases

The benchmark runs in sequential phases, each for the specified duration:

**Simple Queries Mode** (default):
1. **Phase 1**: TiDB queries using `fts_match_word()`
2. **Phase 2**: TiKV queries using `LIKE` with row storage
3. **Phase 3**: TiFlash queries using `LIKE` with columnar storage

**Complex Queries Mode** (with `--complex-queries` flag):
- Phases 1-3: Simple queries (as above)
- **Phase 4**: TiDB complex queries
- **Phase 5**: TiKV complex queries
- **Phase 6**: TiFlash complex queries

### Complex Query Types

When `--complex-queries` is enabled, the benchmark randomly cycles through:

1. **Metadata Filtering**: `WHERE field LIKE '%word%' AND views > 1000 AND langs >= 2`
2. **GROUP BY Aggregation**: `GROUP BY langs ORDER BY count(*) DESC LIMIT 10`
3. **TOP-N Query**: `ORDER BY views DESC LIMIT 10`

### Command Line Options

```bash
Options:
  -c, --concurrency <NUM>         Number of concurrent workers [default: 16]
  -d, --duration <SECONDS>        Benchmark duration per phase in seconds [default: 60]
      --sample-size <NUM>         Number of rows to sample for test data [default: 2000]
      --verbose                   Print individual SQL queries during execution
      --output-file <PATH>        SQL queries log file [default: read_bench_queries.sql]
      --db-host <HOST>            Database host [default: 127.0.0.1]
      --db-port <PORT>            Database port [default: 4000]
      --db-name <NAME>            Database name [default: test]
      --complex-queries           Enable complex query benchmark
  -h, --help                      Print help
  -V, --version                   Print version
```

### Usage Examples

```bash
# Basic benchmark: Simple queries only, default settings
./target/release/read-bench

# With custom concurrency and duration
./target/release/read-bench \
  --concurrency 32 \
  --duration 120

# Enable complex queries benchmark
./target/release/read-bench \
  --concurrency 16 \
  --duration 60 \
  --complex-queries

# Verbose mode: see each SQL query and result
./target/release/read-bench \
  --verbose \
  --concurrency 16 \
  --duration 30

# Full-featured benchmark with custom output
./target/release/read-bench \
  --concurrency 32 \
  --duration 120 \
  --sample-size 5000 \
  --complex-queries \
  --output-file benchmark_results.sql \
  --verbose

# Connect to remote TiDB cluster
./target/release/read-bench \
  --db-host 10.0.1.100 \
  --db-port 4000 \
  --db-name production_db \
  --concurrency 16 \
  --duration 60
```

### Output

The benchmark outputs detailed statistics for each engine:

**Simple Queries:**
```
=== Summary ===
TiDB elapsed   : 60.01s
TiKV elapsed   : 60.02s
TiFlash elapsed: 60.01s
Total elapsed  : 180.04s

--- Simple Queries ---
TiDB       qps=1234.56 p50=12.34ms p95=23.45ms p99=34.56ms max=45.67ms
TiKV       qps=567.89 p50=23.45ms p95=45.67ms p99=56.78ms max=67.89ms
TiFlash    qps=2345.67 p50=6.78ms p95=12.34ms p99=23.45ms max=34.56ms

--- Simple Query Comparison ---
TiDB   QPS: 1234.56
TiKV   QPS: 567.89
TiFlash QPS: 2345.67
QPS ratio (TiKV/TiDB): 0.46x
QPS ratio (TiFlash/TiDB): 1.90x
QPS ratio (TiFlash/TiKV): 4.13x
```

**Complex Queries** (if enabled):
```
--- Complex Queries ---
TiDB (Complex)    qps=123.45 p50=123.45ms p95=234.56ms p99=345.67ms max=456.78ms
TiKV (Complex)    qps=89.01 p50=178.90ms p95=345.67ms p99=456.78ms max=567.89ms
TiFlash (Complex) qps=456.78 p50=34.56ms p95=67.89ms p99=123.45ms max=234.56ms

--- Complex Query Comparison ---
TiDB (Complex)   QPS: 123.45
TiKV (Complex)   QPS: 89.01
TiFlash (Complex) QPS: 456.78
QPS ratio (TiKV/TiDB): 0.72x
QPS ratio (TiFlash/TiDB): 3.70x
QPS ratio (TiFlash/TiKV): 5.13x
```

### SQL Query Log

All executed queries are logged to the output file (default: `read_bench_queries.sql`) with execution time:

```sql
-- TiDB Query (Execution time: 12.34ms)
SELECT count(*) FROM `wiki_paragraphs_embeddings_20260122123456` WHERE fts_match_word('example', title)

-- TiKV Query (Execution time: 23.45ms)
SELECT /*+ READ_FROM_STORAGE(TIKV[`wiki_paragraphs_embeddings_20260122123456`]) */ count(*) FROM `wiki_paragraphs_embeddings_20260122123456` WHERE title LIKE '%example%'

-- TiFlash Query (Execution time: 6.78ms)
SELECT /*+ READ_FROM_STORAGE(TIFLASH[`wiki_paragraphs_embeddings_20260122123456`]) */ count(*) FROM `wiki_paragraphs_embeddings_20260122123456` WHERE title LIKE '%example%'
```

### Performance Tips

1. **Warm-up**: Run a short benchmark first to warm up caches
2. **Sample Size**: Larger sample sizes provide more diverse queries but use more memory
3. **Concurrency**: Adjust based on your hardware (CPU cores, network bandwidth)
4. **Duration**: Longer durations provide more stable statistics
5. **Complex Queries**: These are typically 5-10x slower than simple queries, use longer duration
