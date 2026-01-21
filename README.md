# wiki-vec-bench

Rust benchmark tool for running write workloads (e.g. insert-only / update-mixed) against a MySQL-compatible endpoint.

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
- optionally add a FULLTEXT index on `(title, text)` if `--build-index true`.

Alternatively, you can skip the dataset download and use randomly generated
in-memory samples by passing `--use-random-data true`.

```bash
# Insert-only workload with FULLTEXT index
./target/release/wiki-vec-bench \
  --mode insert-only \
  --concurrency 16 \
  --duration 60 \
  --build-index true

# Update-mixed workload without FULLTEXT index
./target/release/wiki-vec-bench \
  --mode update-mixed \
  --concurrency 16 \
  --duration 60 \
  --build-index false
```
