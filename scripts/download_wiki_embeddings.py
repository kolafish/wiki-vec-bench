#!/usr/bin/env python3
"""
Download half of the Wikipedia embeddings dataset from Hugging Face and
materialize it as parquet under ./data/raw.
Supports incremental download: skips shards that already exist and are valid.
"""

from pathlib import Path
import shutil
import os

from datasets import load_dataset


DATASET_NAME = "maloyan/wikipedia-22-12-en-embeddings-all-MiniLM-L6-v2"
# Only load 50% of the train split to reduce download size.
SPLIT = "train[:50%]"


def check_disk_space(path: Path, required_gb: float = 50.0) -> bool:
    """Check if there's enough disk space available."""
    try:
        stat = shutil.disk_usage(path)
        available_gb = stat.free / (1024 ** 3)
        print(f"Available disk space: {available_gb:.2f} GB", flush=True)
        if available_gb < required_gb:
            print(f"Warning: Less than {required_gb:.2f} GB available. Download may fail.", flush=True)
            return False
        return True
    except Exception as e:
        print(f"Warning: Could not check disk space: {e}", flush=True)
        return True


def is_valid_parquet(path: Path) -> bool:
    """Check if a parquet file is valid by trying to read its metadata."""
    if not path.exists() or path.stat().st_size < 1024:
        return False
    
    try:
        import pyarrow.parquet as pq
        pq.read_metadata(str(path))
        return True
    except Exception:
        return False


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    data_dir = root / "data"
    raw_dir = data_dir / "raw"

    data_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Check disk space before starting
    check_disk_space(raw_dir)

    num_shards = 8
    
    # Check which shards already exist and are valid
    existing_shards = set()
    for i in range(num_shards):
        shard_path = raw_dir / f"wikipedia_embeddings-{i:05d}-of-{num_shards:05d}.parquet"
        if is_valid_parquet(shard_path):
            existing_shards.add(i)
            print(f"Shard {i+1}/{num_shards} already exists and is valid: {shard_path.name}", flush=True)
        elif shard_path.exists():
            print(f"Removing corrupted shard: {shard_path.name}", flush=True)
            shard_path.unlink()
    
    if len(existing_shards) == num_shards:
        print("All shards already exist and are valid. Nothing to download.", flush=True)
        return
    
    print(f"Loading dataset {DATASET_NAME}:{SPLIT} from Hugging Face...", flush=True)
    ds = load_dataset(DATASET_NAME, split=SPLIT)

    print(f"Saving dataset shards to parquet under {raw_dir} ...", flush=True)
    total_size = len(ds)
    shard_size = (total_size + num_shards - 1) // num_shards
    
    for i in range(num_shards):
        if i in existing_shards:
            print(f"Skipping shard {i+1}/{num_shards} (already exists)", flush=True)
            continue
            
        start_idx = i * shard_size
        end_idx = min((i + 1) * shard_size, total_size)
        if start_idx >= total_size:
            break
        
        shard = ds.select(range(start_idx, end_idx))
        output_path = raw_dir / f"wikipedia_embeddings-{i:05d}-of-{num_shards:05d}.parquet"
        print(f"Writing shard {i+1}/{num_shards} ({start_idx}-{end_idx}) to {output_path.name}...", flush=True)
        
        try:
            shard.to_parquet(str(output_path), batch_size=10000)
        except Exception as e:
            print(f"Error writing shard {i+1}: {e}", flush=True)
            if output_path.exists():
                output_path.unlink()
            raise

    print("Download finished. Parquet shards are stored under ./data/raw.", flush=True)


if __name__ == "__main__":
    main()

