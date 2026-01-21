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

On startup it will automatically create a table named
`wiki_paragraphs_embeddings_YYYYMMDDHHMMSS` and add a FULLTEXT index on
`(title, text)`.

```bash
# Insert-only workload
./target/release/wiki-vec-bench \
  --mode insert-only \
  --concurrency 16 \
  --duration 60

# Update-mixed workload
./target/release/wiki-vec-bench \
  --mode update-mixed \
  --concurrency 16 \
  --duration 60
```
