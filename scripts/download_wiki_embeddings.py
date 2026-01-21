#!/usr/bin/env python3
"""
Download the Wikipedia embeddings dataset from Hugging Face and
materialize:

- Full dataset as parquet shards under ./data/raw
- A smaller sampled CSV at ./data/samples.csv with columns:
  title,text,vector
"""

from pathlib import Path
import csv

from datasets import load_dataset


DATASET_NAME = "maloyan/wikipedia-22-12-en-embeddings-all-MiniLM-L6-v2"
SPLIT = "train"
SAMPLE_LIMIT = 200_000  # number of rows to keep in samples.csv


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    data_dir = root / "data"
    raw_dir = data_dir / "raw"
    samples_csv = data_dir / "samples.csv"

    data_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading dataset {DATASET_NAME}:{SPLIT} from Hugging Face...", flush=True)
    ds = load_dataset(DATASET_NAME, split=SPLIT)

    print(f"Saving full dataset to parquet under {raw_dir} ...", flush=True)
    # Write a single parquet file; the Rust benchmark will read all *.parquet
    # files under ./data/raw, so one big file is fine here.
    output_path = raw_dir / "wikipedia_embeddings.parquet"
    ds.to_parquet(str(output_path))

    print("Download finished. Parquet shards are stored under ./data/raw.", flush=True)


if __name__ == "__main__":
    main()

