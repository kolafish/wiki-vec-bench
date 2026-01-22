#!/usr/bin/env python3
"""
Download half of the Wikipedia embeddings dataset from Hugging Face and
materialize it as parquet under ./data/raw.
"""

from pathlib import Path
import csv

from datasets import load_dataset


DATASET_NAME = "maloyan/wikipedia-22-12-en-embeddings-all-MiniLM-L6-v2"
# Only load 50% of the train split to reduce download size.
SPLIT = "train[:50%]"


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    data_dir = root / "data"
    raw_dir = data_dir / "raw"
    samples_csv = data_dir / "samples.csv"

    data_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading dataset {DATASET_NAME}:{SPLIT} from Hugging Face...", flush=True)
    ds = load_dataset(DATASET_NAME, split=SPLIT)

    print(f"Saving dataset shards to parquet under {raw_dir} ...", flush=True)
    # Write multiple parquet shards; the Rust benchmark will read all *.parquet
    # files under ./data/raw, so multiple files are fully supported.
    # Pass directory path directly to to_parquet to create shard files in the directory
    ds.to_parquet(str(raw_dir), batch_size=10000)

    print("Download finished. Parquet shards are stored under ./data/raw.", flush=True)


if __name__ == "__main__":
    main()

