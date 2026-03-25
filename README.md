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

## Vector baseline (vector-baseline)

```bash
./target/release/vector-baseline

./target/release/vector-baseline \
  --concurrency 32 \
  --duration 120

./target/release/vector-baseline \
  --sample-size 5000 \
  --output-file vector_baseline.sql \
  --verbose

./target/release/vector-baseline \
  --realtime-insert \
  --concurrency 16 \
  --duration 60

./target/release/vector-baseline \
  --db-host 10.0.1.100 \
  --db-port 4000 \
  --db-name production_db \
  --concurrency 16 \
  --duration 60
```

## Freshness benchmark (freshness-bench)

```bash
./target/release/freshness-bench

./target/release/freshness-bench \
  --read-concurrency 16 \
  --write-concurrency 2 \
  --duration 120 \
  --write-interval-ms 5
```

## AWS hybrid benchmark

`aws-hybrid-bench` is the new end-to-end runner for the AWS TiDB + TiCI test flow:

1. load parquet rows into a TiDB raw table
2. build a `FULLTEXT(title, text)` index
3. materialize a vector table with `VECTOR(384)`
4. build a TiFlash vector index
5. run inverted-only and vector-only query benchmarks
6. emit `JSON`, `CSV`, and `Markdown` reports

Example:

```bash
cargo build --release --bin aws-hybrid-bench

./target/release/aws-hybrid-bench \
  --db-host 127.0.0.1 \
  --db-port 4000 \
  --db-name test \
  --dataset-dir data/raw \
  --scale-rows 1000000 \
  --query-concurrency 16 \
  --query-duration 120 \
  --sample-size 5000 \
  --output-dir results
```

The dataset download script now supports downloading a bounded prefix of the
Wikipedia embeddings dataset, which is enough for the `1M` and `10M` tiers:

```bash
python3 scripts/download_wiki_embeddings.py --rows 10000000
```

## AWS orchestration

`scripts/aws_cluster_bench.py` orchestrates the AWS workflow around:

- `/Users/jin/Desktop/terraform-tici`
- the Linux builder host `janeyu@10.2.12.81`
- the TiUP center VM created by `terraform-tici`

It can:

- rewrite Terraform node counts
- run `terraform init/apply`
- deploy the cluster from the center host with TiUP
- package Linux hotfix tarballs from the builder host
- patch `tidb`, `tiflash`, `tici_meta`, and `tici_worker`
- sync this repo to the center host
- download the dataset prefix on the center host
- run the `1M` and `10M` benchmark tiers
- fetch result artifacts back locally

Example full flow:

```bash
python3 scripts/aws_cluster_bench.py all \
  --scales 1000000,10000000 \
  --n-tikv 3 \
  --n-tiflash 1 \
  --n-tici-worker 1
```

Current assumptions:

- Terraform and AWS CLI run on the local machine.
- The builder host `10.2.12.81` is only used to source Linux `ELF x86_64` binaries.
- The center VM builds `aws-hybrid-bench` and runs the benchmark.
- TiCI patch tarballs are generated from one `tici-server` binary plus wrapper entrypoints for `meta` and `worker`.
