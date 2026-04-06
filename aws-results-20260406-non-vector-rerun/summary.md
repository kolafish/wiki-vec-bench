# AWS Non-Vector Regression Rerun 2026-04-06

Scope:
- Compare latest `tici master` vs current `tici vector`
- Workloads: `fulltext`, `hybrid_inverted`
- Metrics:
  - `insert elapsed`
  - `cdc_done_after_write_secs`
  - `end_to_end_secs`
  - reader warmup `first_shard_add_secs`

## Result

No regression was observed on the non-vector path. In this rerun, `vector` is faster in both workloads.

| case | branch | insert | cdc after write | end-to-end | warmup first shard add |
|---|---:|---:|---:|---:|---:|
| fulltext | master | 220.567s | 29.147s | 249.714s | 13.070s |
| fulltext | vector | 218.838s | 6.583s | 225.421s | 10.668s |
| hybrid_inverted | master | 209.399s | 7.854s | 217.252s | 3.204s |
| hybrid_inverted | vector | 208.867s | 6.347s | 215.213s | 3.103s |

## Delta

### fulltext
- insert: `-1.729s` (`-0.78%`)
- cdc after write: `-22.564s` (`-77.41%`)
- end-to-end: `-24.293s` (`-9.73%`)
- warmup: `-2.403s` (`-18.38%`)

### hybrid_inverted
- insert: `-0.532s` (`-0.25%`)
- cdc after write: `-1.507s` (`-19.19%`)
- end-to-end: `-2.039s` (`-0.94%`)
- warmup: `-0.102s` (`-3.17%`)

## Raw Files

- [master_fulltext_1m.json](/Users/jin/Desktop/wiki-vec-bench/aws-results-20260406-non-vector-rerun/master_fulltext_1m.json)
- [master_hybrid_inverted_1m.json](/Users/jin/Desktop/wiki-vec-bench/aws-results-20260406-non-vector-rerun/master_hybrid_inverted_1m.json)
- [vector_fulltext_1m.json](/Users/jin/Desktop/wiki-vec-bench/aws-results-20260406-non-vector-rerun/vector_fulltext_1m.json)
- [vector_hybrid_inverted_1m.json](/Users/jin/Desktop/wiki-vec-bench/aws-results-20260406-non-vector-rerun/vector_hybrid_inverted_1m.json)
