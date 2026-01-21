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
    ds.to_parquet(str(raw_dir))

    n = min(SAMPLE_LIMIT, len(ds))
    print(f"Writing {n} samples to {samples_csv} ...", flush=True)

    with samples_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["title", "text", "vector"])
        for i in range(n):
            row = ds[i]
            title = row.get("title", "")
            text = row.get("text", "")
            emb = row.get("emb", [])
            vector_str = ",".join(f"{float(x):.6f}" for x in emb)
            writer.writerow([title, text, vector_str])

    print("Download and preprocessing finished.", flush=True)


if __name__ == "__main__":
    main()

