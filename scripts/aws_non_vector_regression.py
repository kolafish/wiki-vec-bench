#!/usr/bin/env python3
import argparse
import json
import math
import os
import re
import shlex
import statistics
import subprocess
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pymysql


DEFAULT_DB_HOST = "127.0.0.1"
DEFAULT_DB_PORT = 4000
DEFAULT_DB_NAME = "test"
DEFAULT_DB_USER = "bench"
DEFAULT_DB_PASSWORD = "benchpass123"
DEFAULT_DATASET_DIR = "data/raw"
DEFAULT_BATCH_SIZE = 500
DEFAULT_SCALE_ROWS = 1_000_000
DEFAULT_INDEX_MODE = "fulltext"
DEFAULT_QUERY_WORD = "YouTube"
DEFAULT_TIFLASH_HOST = "172.31.9.1"
DEFAULT_TIFLASH_SSH = "ubuntu"
DEFAULT_TIFLASH_LOG = "/tidb-deploy/tiflash-9000/log/tici_searchlib.log"
DEFAULT_TICI_WORKER_HOST = "172.31.11.1"
DEFAULT_TICI_WORKER_SSH = "ubuntu"
DEFAULT_TICI_WORKER_LOG = "/home/ubuntu/tici-worker/tici_worker.log"


@dataclass
class LatencySummary:
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float


@dataclass
class WriteSummary:
    rows: int
    elapsed_secs: float
    rows_per_sec: float
    batches: int
    batch_latency_ms: LatencySummary


@dataclass
class WarmupSummary:
    restart_epoch_ms: int
    restore_done_elapsed_secs: float | None
    first_query_ready_secs: float | None
    shard_add_count: int
    first_shard_add_secs: float | None
    last_shard_add_secs: float | None
    matched_restore_line: str | None


@dataclass
class CdcSummary:
    write_start_epoch_ms: int
    write_end_epoch_ms: int
    first_worker_seen_secs: float | None
    cdc_done_after_write_secs: float | None
    end_to_end_secs: float | None
    matched_done_line: str | None


def run(cmd: str, check: bool = True, capture_output: bool = True) -> subprocess.CompletedProcess:
    print(f"+ {cmd}")
    return subprocess.run(
        cmd,
        shell=True,
        text=True,
        check=check,
        capture_output=capture_output,
    )


def ssh(host: str, command: str, user: str = "ubuntu", check: bool = True) -> subprocess.CompletedProcess:
    return run(
        f"ssh -o StrictHostKeyChecking=no {user}@{host} {json.dumps(command)}",
        check=check,
    )


def escape_sql(text: str) -> str:
    return (
        text.replace("\\", "\\\\")
        .replace("'", "''")
        .replace('"', '\\"')
        .replace("\0", "\\0")
    )


def normalize_title(text: str, row_id: int) -> str:
    compact = " ".join((text or "").split()).strip()
    if not compact:
        compact = f"doc_{row_id}"
    return compact[:256]


def quantile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, math.floor((len(sorted_vals) - 1) * q)))
    return sorted_vals[idx]


def summarize_latencies(latencies_ms: list[float]) -> LatencySummary:
    if not latencies_ms:
        return LatencySummary(0.0, 0.0, 0.0, 0.0)
    values = sorted(latencies_ms)
    return LatencySummary(
        p50_ms=quantile(values, 0.50),
        p95_ms=quantile(values, 0.95),
        p99_ms=quantile(values, 0.99),
        max_ms=values[-1],
    )


def parse_readable_duration_secs(text: str) -> float | None:
    value = text.strip()
    unit_scales = [
        ("ns", 1e-9),
        ("us", 1e-6),
        ("µs", 1e-6),
        ("ms", 1e-3),
        ("s", 1.0),
    ]
    for unit, scale in unit_scales:
        if value.endswith(unit):
            try:
                return float(value[: -len(unit)]) * scale
            except ValueError:
                return None
    return None


def extract_duration_secs_from_line(line: str) -> float | None:
    match = re.search(r"use ([0-9]+(?:\\.[0-9]+)?)(ns|us|µs|ms|s)", line)
    if not match:
        return None
    return parse_readable_duration_secs("".join(match.groups()))


def extract_epoch_ms_from_log_line(line: str) -> int | None:
    match = re.match(r"^\[(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) \+00:00\]", line)
    if not match:
        return None
    try:
        dt = datetime.strptime(match.group(1), "%Y/%m/%d %H:%M:%S.%f").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None
    return int(dt.timestamp() * 1000)


def dataset_rows(dataset_dir: Path, scale_rows: int):
    parquet_files = sorted(dataset_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"no parquet files found under {dataset_dir}")
    emitted = 0
    row_id = 1
    for parquet_file in parquet_files:
        parquet = pq.ParquetFile(parquet_file)
        for batch in parquet.iter_batches(batch_size=2048):
            data = batch.to_pydict()
            titles = data.get("title", [])
            texts = data.get("text", [])
            views = data.get("views", [])
            langs = data.get("langs", [])
            for idx in range(len(titles)):
                title = titles[idx] if idx < len(titles) else ""
                text = texts[idx] if idx < len(texts) else ""
                view = int(views[idx] or 0) if idx < len(views) else 0
                lang = int(langs[idx] or 0) if idx < len(langs) else 0
                title_kw = normalize_title(title, row_id)
                payload = " ".join(((title or "").strip(), (text or "").strip())).strip()
                if not payload:
                    payload = title_kw
                yield {
                    "id": row_id,
                    "title": title_kw,
                    "body": text or payload,
                    "title_kw": title_kw,
                    "payload": payload,
                    "views": view,
                    "langs": lang,
                }
                row_id += 1
                emitted += 1
                if emitted >= scale_rows:
                    return


def connect(args):
    return pymysql.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_password,
        database=args.db_name,
        charset="utf8mb4",
        autocommit=True,
    )


def create_table_and_index(cur, table_name: str, index_mode: str):
    cur.execute(f"DROP TABLE IF EXISTS `{table_name}`")
    if index_mode == "fulltext":
        cur.execute(
            f"""
            CREATE TABLE `{table_name}` (
              id BIGINT NOT NULL,
              title VARCHAR(512) NOT NULL,
              body TEXT NOT NULL,
              views INT NOT NULL,
              langs INT NOT NULL,
              PRIMARY KEY (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        cur.execute(
            f"""
            ALTER TABLE `{table_name}`
            ADD FULLTEXT INDEX ft_index (title, body) WITH PARSER standard
            """
        )
    elif index_mode == "hybrid_inverted":
        cur.execute(
            f"""
            CREATE TABLE `{table_name}` (
              id BIGINT NOT NULL,
              title_kw VARCHAR(256) NOT NULL,
              payload TEXT NOT NULL,
              views INT NOT NULL,
              langs INT NOT NULL,
              PRIMARY KEY (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        cur.execute(
            f"""
            CREATE HYBRID INDEX idx_hybrid
            ON `{table_name}`(title_kw, views, langs)
            PARAMETER '{{
              "inverted": {{
                "columns": ["title_kw"]
              }},
              "sort": {{
                "columns": ["views"],
                "order": ["desc"]
              }},
              "sharding_key": {{
                "columns": ["title_kw"]
              }}
            }}'
            """
        )
    else:
        raise ValueError(f"unsupported index_mode={index_mode}")


def insert_rows(cur, table_name: str, index_mode: str, rows: list[dict]):
    if index_mode == "fulltext":
        values_sql = ",".join(
            f"({r['id']},'{escape_sql(r['title'])}','{escape_sql(r['body'])}',{r['views']},{r['langs']})"
            for r in rows
        )
        cur.execute(
            f"INSERT INTO `{table_name}` (id, title, body, views, langs) VALUES {values_sql}"
        )
    else:
        values_sql = ",".join(
            f"({r['id']},'{escape_sql(r['title_kw'])}','{escape_sql(r['payload'])}',{r['views']},{r['langs']})"
            for r in rows
        )
        cur.execute(
            f"INSERT INTO `{table_name}` (id, title_kw, payload, views, langs) VALUES {values_sql}"
        )


def get_table_id(cur, db_name: str, table_name: str) -> int:
    cur.execute(
        """
        SELECT tidb_table_id
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        (db_name, table_name),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"table_id not found for {db_name}.{table_name}")
    return int(row[0])


def wait_for_index_ready(cur, db_name: str, table_name: str, timeout_secs: int = 3600):
    start = time.time()
    while time.time() - start < timeout_secs:
        cur.execute(
            """
            SELECT
              COUNT(*) AS shard_count,
              CAST(
                SUM(
                  CASE
                    WHEN JSON_EXTRACT(s.manifest, '$.fragments[0].f.property.count') IS NOT NULL THEN 1
                    ELSE 0
                  END
                ) AS SIGNED
              ) AS ready_fragment_count
            FROM tici.tici_shard_meta s
            JOIN information_schema.tables t
              ON t.tidb_table_id = s.table_id
            WHERE t.table_schema = %s AND t.table_name = %s
            """,
            (db_name, table_name),
        )
        shard_count, frag_count = cur.fetchone()
        if (shard_count or 0) > 0 and (frag_count or 0) > 0:
            return
        time.sleep(2)
    raise TimeoutError(f"timeout waiting for TiCI readiness on {table_name}")


def run_probe_query(cur, table_name: str, index_mode: str, query_word: str):
    if index_mode == "fulltext":
        cur.execute(
            f"SELECT COUNT(*) FROM `{table_name}` WHERE fts_match_word(%s, title)",
            (query_word,),
        )
    else:
        cur.execute(
            f"SELECT COUNT(*) FROM `{table_name}` USE INDEX (idx_hybrid) WHERE title_kw = %s",
            (query_word,),
        )
    return int(cur.fetchone()[0])


def restart_reader_and_measure(
    args, cur, table_name: str, table_id: int, index_mode: str
) -> WarmupSummary:
    ssh(
        args.tiflash_host,
        "sudo bash -lc 'set -e; "
        "systemctl stop tiflash-9000.service; "
        "find /tidb-deploy/tiflash-9000/data/tici/searchlib/fragments -mindepth 1 -maxdepth 1 -exec rm -rf {} +; "
        "find /tidb-deploy/tiflash-9000/data/tici/searchlib/temp -mindepth 1 -maxdepth 1 -exec rm -rf {} +; "
        "if [ -d /tidb-deploy/tiflash-9000/data/tici/searchlib/metadata ]; then "
        "find /tidb-deploy/tiflash-9000/data/tici/searchlib/metadata -mindepth 1 -maxdepth 1 -exec rm -rf {} +; "
        "else "
        "rm -f /tidb-deploy/tiflash-9000/data/tici/searchlib/metadata; "
        "fi; "
        "systemctl start tiflash-9000.service'",
        user=args.tiflash_ssh_user,
    )
    restart_epoch_ms = int(time.time() * 1000)

    restore_done_elapsed_secs = None
    matched_restore_line = None
    shard_add_count = 0
    shard_add_lines: set[str] = set()
    first_shard_add_secs = None
    last_shard_add_secs = None
    table_needle = f"t_{table_id}/"
    first_query_ready_secs = None
    probe_triggered = False
    start = time.time()
    while time.time() - start < 3600:
        if not probe_triggered:
            probe_triggered = True
            try:
                run_probe_query(cur, table_name, index_mode, args.query_word)
                first_query_ready_secs = time.time() - (restart_epoch_ms / 1000.0)
            except Exception:
                pass
        # Keep the remote command shell-safe. A simple tail is enough because
        # we only need recent restore / shard-add lines after the restart.
        log_cmd = (
            f"grep -E 'restore: done| is added, seq=' {shlex.quote(args.tiflash_log)} "
            f"| tail -n 2000"
        )
        filtered = ssh(
            args.tiflash_host, log_cmd, user=args.tiflash_ssh_user
        ).stdout.splitlines()
        for line in filtered:
            if " is added, seq=" in line and table_needle in line and line not in shard_add_lines:
                shard_add_lines.add(line)
                shard_add_count = len(shard_add_lines)
                elapsed = time.time() - (restart_epoch_ms / 1000.0)
                if first_shard_add_secs is None:
                    first_shard_add_secs = elapsed
                last_shard_add_secs = elapsed
            if "restore: done" in line:
                matched_restore_line = line
                restore_done_elapsed_secs = extract_duration_secs_from_line(line)
        if shard_add_count > 0:
            break
        time.sleep(5)

    return WarmupSummary(
        restart_epoch_ms=restart_epoch_ms,
        restore_done_elapsed_secs=restore_done_elapsed_secs,
        first_query_ready_secs=first_query_ready_secs,
        shard_add_count=shard_add_count,
        first_shard_add_secs=first_shard_add_secs,
        last_shard_add_secs=last_shard_add_secs,
        matched_restore_line=matched_restore_line,
    )


def wait_for_cdc_done(args, table_id: int, write_start_epoch_ms: int, write_end_epoch_ms: int) -> CdcSummary:
    table_needle = f"t_{table_id}/"
    prefix_needle = f"wiki-vec-bench/cdc/{table_id}"
    first_worker_seen_secs = None
    cdc_done_after_write_secs = None
    end_to_end_secs = None
    matched_done_line = None
    fallback_submit_line = None
    fallback_submit_epoch_ms = None
    last_progress_line = None
    last_progress_wall = None

    log_pattern = f"{table_needle}|{prefix_needle}"
    log_cmd = f"grep -E {shlex.quote(log_pattern)} {shlex.quote(args.tici_worker_log)} | tail -n 4000"
    start = time.time()
    while time.time() - start < 3600:
        lines = ssh(
            args.tici_worker_host,
            log_cmd,
            user=args.tici_worker_ssh_user,
        ).stdout.splitlines()
        latest_valid_line = None
        for line in lines:
            epoch_ms = extract_epoch_ms_from_log_line(line)
            if epoch_ms is None or epoch_ms < write_start_epoch_ms:
                continue
            latest_valid_line = line
            if first_worker_seen_secs is None:
                first_worker_seen_secs = max(0.0, (epoch_ms - write_start_epoch_ms) / 1000.0)
            if (
                "submit_frag_success" in line
                or "compact_fragments: finish_compact_frags success" in line
            ):
                fallback_submit_line = line
                fallback_submit_epoch_ms = epoch_ms
            if (
                prefix_needle in line
                and "unread_files_count=0" in line
                and "first_file=None" in line
            ):
                matched_done_line = line
                cdc_done_after_write_secs = max(0.0, (epoch_ms - write_end_epoch_ms) / 1000.0)
                end_to_end_secs = max(0.0, (epoch_ms - write_start_epoch_ms) / 1000.0)
                return CdcSummary(
                    write_start_epoch_ms=write_start_epoch_ms,
                    write_end_epoch_ms=write_end_epoch_ms,
                    first_worker_seen_secs=first_worker_seen_secs,
                    cdc_done_after_write_secs=cdc_done_after_write_secs,
                    end_to_end_secs=end_to_end_secs,
                    matched_done_line=matched_done_line,
                )
            if "cdc_log_frag_writer_shutdown" in line or "remove_shard_success" in line:
                matched_done_line = line
                cdc_done_after_write_secs = max(0.0, (epoch_ms - write_end_epoch_ms) / 1000.0)
                end_to_end_secs = max(0.0, (epoch_ms - write_start_epoch_ms) / 1000.0)
                return CdcSummary(
                    write_start_epoch_ms=write_start_epoch_ms,
                    write_end_epoch_ms=write_end_epoch_ms,
                    first_worker_seen_secs=first_worker_seen_secs,
                    cdc_done_after_write_secs=cdc_done_after_write_secs,
                    end_to_end_secs=end_to_end_secs,
                    matched_done_line=matched_done_line,
                )
        if latest_valid_line is not None and latest_valid_line != last_progress_line:
            last_progress_line = latest_valid_line
            last_progress_wall = time.time()
        if (
            fallback_submit_epoch_ms is not None
            and last_progress_wall is not None
            and time.time() - last_progress_wall >= 15
        ):
            matched_done_line = fallback_submit_line
            cdc_done_after_write_secs = max(0.0, (fallback_submit_epoch_ms - write_end_epoch_ms) / 1000.0)
            end_to_end_secs = max(0.0, (fallback_submit_epoch_ms - write_start_epoch_ms) / 1000.0)
            return CdcSummary(
                write_start_epoch_ms=write_start_epoch_ms,
                write_end_epoch_ms=write_end_epoch_ms,
                first_worker_seen_secs=first_worker_seen_secs,
                cdc_done_after_write_secs=cdc_done_after_write_secs,
                end_to_end_secs=end_to_end_secs,
                matched_done_line=matched_done_line,
            )
        time.sleep(5)

    if fallback_submit_epoch_ms is not None:
        matched_done_line = fallback_submit_line
        cdc_done_after_write_secs = max(0.0, (fallback_submit_epoch_ms - write_end_epoch_ms) / 1000.0)
        end_to_end_secs = max(0.0, (fallback_submit_epoch_ms - write_start_epoch_ms) / 1000.0)

    return CdcSummary(
        write_start_epoch_ms=write_start_epoch_ms,
        write_end_epoch_ms=write_end_epoch_ms,
        first_worker_seen_secs=first_worker_seen_secs,
        cdc_done_after_write_secs=cdc_done_after_write_secs,
        end_to_end_secs=end_to_end_secs,
        matched_done_line=matched_done_line,
    )


def main():
    parser = argparse.ArgumentParser(description="AWS non-vector write and warmup regression benchmark")
    parser.add_argument("--db-host", default=DEFAULT_DB_HOST)
    parser.add_argument("--db-port", type=int, default=DEFAULT_DB_PORT)
    parser.add_argument("--db-name", default=DEFAULT_DB_NAME)
    parser.add_argument("--db-user", default=DEFAULT_DB_USER)
    parser.add_argument("--db-password", default=DEFAULT_DB_PASSWORD)
    parser.add_argument("--dataset-dir", default=DEFAULT_DATASET_DIR)
    parser.add_argument("--scale-rows", type=int, default=DEFAULT_SCALE_ROWS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--index-mode", choices=["fulltext", "hybrid_inverted"], default=DEFAULT_INDEX_MODE)
    parser.add_argument("--output", required=True)
    parser.add_argument("--query-word", default=DEFAULT_QUERY_WORD)
    parser.add_argument("--tiflash-host", default=DEFAULT_TIFLASH_HOST)
    parser.add_argument("--tiflash-ssh-user", default=DEFAULT_TIFLASH_SSH)
    parser.add_argument("--tiflash-log", default=DEFAULT_TIFLASH_LOG)
    parser.add_argument("--tici-worker-host", default=DEFAULT_TICI_WORKER_HOST)
    parser.add_argument("--tici-worker-ssh-user", default=DEFAULT_TICI_WORKER_SSH)
    parser.add_argument("--tici-worker-log", default=DEFAULT_TICI_WORKER_LOG)
    args = parser.parse_args()

    conn = connect(args)
    try:
        with conn.cursor() as cur:
            table_name = f"nv_reg_{args.index_mode}_{int(time.time())}"
            create_table_and_index(cur, table_name, args.index_mode)
            table_id = get_table_id(cur, args.db_name, table_name)

            batch = []
            batch_latencies_ms = []
            total_rows = 0
            write_start_epoch_ms = int(time.time() * 1000)
            write_start = time.time()
            for row in dataset_rows(Path(args.dataset_dir), args.scale_rows):
                batch.append(row)
                if len(batch) >= args.batch_size:
                    batch_start = time.time()
                    insert_rows(cur, table_name, args.index_mode, batch)
                    batch_latencies_ms.append((time.time() - batch_start) * 1000.0)
                    total_rows += len(batch)
                    batch.clear()
            if batch:
                batch_start = time.time()
                insert_rows(cur, table_name, args.index_mode, batch)
                batch_latencies_ms.append((time.time() - batch_start) * 1000.0)
                total_rows += len(batch)

            write_elapsed = time.time() - write_start
            write_end_epoch_ms = int(write_start_epoch_ms + (write_elapsed * 1000.0))
            wait_for_index_ready(cur, args.db_name, table_name)
            cdc = wait_for_cdc_done(args, table_id, write_start_epoch_ms, write_end_epoch_ms)
            warmup = restart_reader_and_measure(
                args, cur, table_name, table_id, args.index_mode
            )

            report = {
                "table_name": table_name,
                "table_id": table_id,
                "index_mode": args.index_mode,
                "scale_rows": total_rows,
                "write_summary": asdict(
                    WriteSummary(
                        rows=total_rows,
                        elapsed_secs=write_elapsed,
                        rows_per_sec=total_rows / write_elapsed if write_elapsed > 0 else 0.0,
                        batches=len(batch_latencies_ms),
                        batch_latency_ms=summarize_latencies(batch_latencies_ms),
                    )
                ),
                "cdc_summary": asdict(cdc),
                "warmup_summary": asdict(warmup),
            }
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(report, indent=2))
            print(json.dumps(report, indent=2))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
