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
```bash
Insert-only
./target/release/wiki-vec-bench \
  --url "mysql://user:password@127.0.0.1:4000/test" \
  --mode insert-only \
  --concurrency 16 \
  --duration 60 \
  --writers 1

Update-mixed
./target/release/wiki-vec-bench \
  --url "mysql://user:password@127.0.0.1:4000/test" \
  --mode update-mixed \
  --concurrency 16 \
  --duration 60 \
  --writers 2
```
