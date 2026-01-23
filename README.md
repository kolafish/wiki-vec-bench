# wiki-vec-bench

Rust benchmarks for TiDB write and read workloads on Wikipedia embeddings data.

## Build

```bash
rustc --version
cargo --version

cd wiki-vec-bench
cargo build --release
```

## Write benchmark (wiki-vec-bench)

```bash
# Insert-only, no FULLTEXT index
./target/release/wiki-vec-bench \
  --mode insert-only \
  --concurrency 16 \
  --duration 60 \
  --use-random-data

# Insert-only, FULLTEXT index on (title, text)
./target/release/wiki-vec-bench \
  --mode insert-only \
  --concurrency 16 \
  --duration 60 \
  --build-index \
  --use-random-data

# Insert-only, FULLTEXT index on (title, text, vector)
./target/release/wiki-vec-bench \
  --mode insert-only \
  --concurrency 16 \
  --duration 60 \
  --build-index \
  --index-with-vector \
  --use-random-data

# Update-mixed, no FULLTEXT index
./target/release/wiki-vec-bench \
  --mode update-mixed \
  --concurrency 16 \
  --duration 60 \
  --use-random-data

# Update-mixed, FULLTEXT index on (title, text)
./target/release/wiki-vec-bench \
  --mode update-mixed \
  --concurrency 16 \
  --duration 60 \
  --build-index \
  --use-random-data

# Update-mixed, FULLTEXT index on (title, text, vector)
./target/release/wiki-vec-bench \
  --mode update-mixed \
  --concurrency 16 \
  --duration 60 \
  --build-index \
  --index-with-vector \
  --use-random-data
```

## Read benchmark (read-bench)

```bash
./target/release/read-bench

./target/release/read-bench \
  --concurrency 32 \
  --duration 120

./target/release/read-bench \
  --concurrency 16 \
  --duration 60 \
  --complex-queries

./target/release/read-bench \
  --verbose \
  --concurrency 16 \
  --duration 30

./target/release/read-bench \
  --concurrency 32 \
  --duration 120 \
  --sample-size 5000 \
  --complex-queries \
  --output-file benchmark_results.sql \
  --verbose

./target/release/read-bench \
  --db-host 10.0.1.100 \
  --db-port 4000 \
  --db-name production_db \
  --concurrency 16 \
  --duration 60
```

## Vector read benchmark (read-vector-bench)

```bash
./target/release/read-vector-bench

./target/release/read-vector-bench \
  --concurrency 32 \
  --duration 120

./target/release/read-vector-bench \
  --sample-size 5000 \
  --output-file vector_read.sql \
  --verbose

./target/release/read-vector-bench \
  --db-host 10.0.1.100 \
  --db-port 4000 \
  --db-name production_db \
  --concurrency 16 \
  --duration 60
```
