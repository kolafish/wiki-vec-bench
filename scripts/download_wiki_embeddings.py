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
    # Manually split the dataset into multiple shards for better write performance.
    num_shards = 8
    total_size = len(ds)
    shard_size = (total_size + num_shards - 1) // num_shards
    
    for i in range(num_shards):
        start_idx = i * shard_size
        end_idx = min((i + 1) * shard_size, total_size)
        if start_idx >= total_size:
            break
        
        shard = ds.select(range(start_idx, end_idx))
        output_path = raw_dir / f"wikipedia_embeddings-{i:05d}-of-{num_shards:05d}.parquet"
        print(f"Writing shard {i+1}/{num_shards} ({start_idx}-{end_idx}) to {output_path.name}...", flush=True)
        shard.to_parquet(str(output_path), batch_size=10000)

    print("Download finished. Parquet shards are stored under ./data/raw.", flush=True)


if __name__ == "__main__":
    main()

