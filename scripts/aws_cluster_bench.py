#!/usr/bin/env python3
import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import tempfile
import urllib.parse
from pathlib import Path


DEFAULT_CLUSTER_NAME = "tidb-test"
DEFAULT_CENTER_WORKDIR = "~/wiki-vec-bench"
DEFAULT_BUILDER_HOST = "janeyu@10.2.12.81"
DEFAULT_BUILDER_TIDB = "/DATA/disk1/janeyu/aws-bench-builder/worktrees/tidb/bin/tidb-server"
DEFAULT_BUILDER_TIFLASH_COMPONENT = "/DATA/disk1/janeyu/aws-bench-builder/worktrees/tiflash/release-linux-llvm/tiflash"
DEFAULT_BUILDER_TIFLASH = "/DATA/disk1/janeyu/aws-bench-builder/worktrees/tiflash/release-linux-llvm/build-release/dbms/src/Server/tiflash"
DEFAULT_BUILDER_TIFLASH_PROXY = "/DATA/disk1/janeyu/aws-bench-builder/worktrees/tiflash/release-linux-llvm/build-release/contrib/tiflash-proxy-cmake/release/libtiflash_proxy.so"
DEFAULT_BUILDER_TIFLASH_SEARCH_LIB = "/DATA/disk1/janeyu/aws-bench-builder/worktrees/tiflash/release-linux-llvm/build-release/contrib/tici-search-lib/libtici_search_lib.so"
DEFAULT_BUILDER_TICI = "/DATA/disk1/janeyu/aws-bench-builder/worktrees/tici/target/release/tici-server"
DEFAULT_SCALES = "1000000,10000000"
DEFAULT_TIUP_MIRROR = "http://tiup.pingcap.net:8988"
DEFAULT_AWS_REGION = "us-west-2"
DEFAULT_S3_ENDPOINT = "https://s3.us-west-2.amazonaws.com"
DEFAULT_S3_PREFIX = "wiki-vec-bench"
DEFAULT_DB_USER = "bench"
DEFAULT_DB_PASSWORD = "benchpass123"
DEFAULT_CHANGEFEED_ID = "tici-bench-changefeed"


def run(cmd, cwd=None, capture_output=False, env=None):
    print(f"+ {cmd}")
    process_env = os.environ.copy()
    if env:
        process_env.update(env)
    return subprocess.run(
        cmd,
        cwd=cwd,
        env=process_env,
        shell=True,
        check=True,
        text=True,
        capture_output=capture_output,
    )


def ssh(host, command, capture_output=False):
    quoted = shlex.quote(command)
    return run(
        f"ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -o ServerAliveCountMax=10 {host} {quoted}",
        capture_output=capture_output,
    )


def scp(src, dst):
    run(f"scp -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -o ServerAliveCountMax=10 -r {src} {dst}")


def update_terraform_counts(terraform_dir: Path, n_tidb: int, n_tikv: int, n_tiflash: int, n_tici_meta: int, n_tici_worker: int):
    path = terraform_dir / "locals_common.tf"
    content = path.read_text()
    replacements = {
        "n_tidb": n_tidb,
        "n_tikv": n_tikv,
        "n_tiflash": n_tiflash,
        "n_tici_meta": n_tici_meta,
        "n_tici_worker": n_tici_worker,
    }
    for key, value in replacements.items():
        content = re.sub(rf"({key}\s*=\s*)\d+", rf"\g<1>{value}", content)
    path.write_text(content)


def terraform_env(terraform_dir: Path):
    empty_credentials = terraform_dir / ".aws-empty-credentials"
    empty_credentials.touch(exist_ok=True)
    return {
        "AWS_SDK_LOAD_CONFIG": "1",
        "AWS_SHARED_CREDENTIALS_FILE": str(empty_credentials),
    }


def terraform_output(terraform_dir: Path):
    result = run(
        "terraform output -json",
        cwd=terraform_dir,
        capture_output=True,
        env=terraform_env(terraform_dir),
    )
    return json.loads(result.stdout)


def extract_center_host(terraform_json):
    ssh_center = terraform_json["ssh-center"]["value"]
    return ssh_center.split()[-1]


def terraform_value(terraform_json, *keys):
    for key in keys:
        if key in terraform_json:
            return terraform_json[key]["value"]
    raise KeyError(f"Missing terraform outputs: {keys}")


def first_host(value):
    if isinstance(value, list):
        if not value:
            raise ValueError("Expected at least one host, got empty list")
        return value[0]
    return value


def mysql_dsn(user: str, password: str, host: str, port: int):
    user_part = urllib.parse.quote(user, safe="")
    if password:
        password_part = urllib.parse.quote(password, safe="")
        auth_part = f"{user_part}:{password_part}"
    else:
        auth_part = user_part
    return f"mysql://{auth_part}@{host}:{port}"


def sql_string_literal(text: str):
    return text.replace("'", "''")


def ensure_terraform(terraform_dir: Path):
    env = terraform_env(terraform_dir)
    run("terraform init", cwd=terraform_dir, env=env)
    run("terraform apply -auto-approve", cwd=terraform_dir, env=env)


def prepare_nightly_topology(center_host: str):
    command = """
set -euo pipefail
python3 - <<'PY'
from pathlib import Path

src = Path.home() / "topology.yaml"
dst = Path.home() / "topology-nightly.yaml"
lines = src.read_text().splitlines()
out = []
skip = False
for line in lines:
    stripped = line.lstrip()
    if line.startswith("  tici_meta:") or line.startswith("  tici_worker:"):
        continue
    if stripped in {"tici_meta_servers:", "tici_worker_servers:"}:
        skip = True
        continue
    if skip and line and not line.startswith(" "):
        skip = False
    if not skip:
        out.append(line)

text = "\n".join(out) + "\n"
text = text.replace("tidb: v9.0.0-feature.fts", "tidb: nightly")
text = text.replace("tiflash: v9.0.0-feature.fts", "tiflash: nightly")
dst.write_text(text)
PY
"""
    ssh(center_host, command)


def ensure_cluster_deployed(center_host: str, cluster_name: str):
    command = f"""
set -euo pipefail
export PATH="$HOME/.tiup/bin:$PATH"
export TIUP_SKIP_UPDATE_CHECK=1
source ~/.bashrc >/dev/null 2>&1 || true
source ~/.zshrc >/dev/null 2>&1 || true
python3 - <<'PY'
from pathlib import Path

src = Path.home() / "topology.yaml"
dst = Path.home() / "topology-nightly.yaml"
lines = src.read_text().splitlines()
out = []
skip = False
for line in lines:
    stripped = line.lstrip()
    if line.startswith("  tici_meta:") or line.startswith("  tici_worker:"):
        continue
    if stripped in {{"tici_meta_servers:", "tici_worker_servers:"}}:
        skip = True
        continue
    if skip and line and not line.startswith(" "):
        skip = False
    if not skip:
        out.append(line)

text = "\\n".join(out) + "\\n"
text = text.replace("tidb: v9.0.0-feature.fts", "tidb: nightly")
text = text.replace("tiflash: v9.0.0-feature.fts", "tiflash: nightly")
dst.write_text(text)
PY
if tiup cluster display {cluster_name} >/dev/null 2>&1; then
  tiup cluster start {cluster_name}
else
  cd ~
  tiup cluster:v1.16.4 deploy {cluster_name} nightly ./topology-nightly.yaml --user ubuntu -i ~/.ssh/id_rsa --yes
  tiup cluster start {cluster_name}
fi
"""
    ssh(center_host, command)


def sync_repo_to_center(local_repo: Path, center_host: str, remote_dir: str):
    run(
        "rsync -az --delete "
        "--exclude .git "
        "--exclude target "
        f"{shlex.quote(str(local_repo))}/ {center_host}:{shlex.quote(remote_dir)}/"
    )


def ensure_center_toolchain(center_host: str):
    command = """
set -euo pipefail
export PATH="$HOME/.cargo/bin:/usr/local/go/bin:$PATH"
if ! command -v cargo >/dev/null 2>&1; then
  curl https://sh.rustup.rs -sSf | sh -s -- -y
fi
python3 -m pip install --user --upgrade pip
python3 -m pip install --user datasets pyarrow
"""
    ssh(center_host, command)


def package_linux_binaries_from_builder(args, local_package_dir: Path):
    local_package_dir.mkdir(parents=True, exist_ok=True)
    remote_tmp = ssh(
        args.builder_host,
        """
set -euo pipefail
tmpdir=$(mktemp -d)
echo "$tmpdir"
""",
        capture_output=True,
    ).stdout.strip()

    package_cmd = f"""
set -euo pipefail
tmpdir={shlex.quote(remote_tmp)}
mkdir -p "$tmpdir"

cp {shlex.quote(args.builder_tidb)} "$tmpdir/tidb-server"
tar -C "$tmpdir" -czf "$tmpdir/tidb-hotfix.tar.gz" tidb-server

mkdir -p "$tmpdir/tiflash"
if [[ -d {shlex.quote(args.builder_tiflash_component_dir)} ]]; then
  cp -R {shlex.quote(args.builder_tiflash_component_dir)}/. "$tmpdir/tiflash/"
fi
cp {shlex.quote(args.builder_tiflash)} "$tmpdir/tiflash/tiflash"
cp {shlex.quote(args.builder_tiflash_proxy)} "$tmpdir/tiflash/libtiflash_proxy.so"
cp {shlex.quote(args.builder_tiflash_search_lib)} "$tmpdir/tiflash/libtici_search_lib.so"
tar -C "$tmpdir" -czf "$tmpdir/tiflash-hotfix.tar.gz" tiflash

cp {shlex.quote(args.builder_tici)} "$tmpdir/tici-server"
cat > "$tmpdir/meta_service_server" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
exec "$SCRIPT_DIR/tici-server" meta "$@"
EOF
cat > "$tmpdir/worker_node_server" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
exec "$SCRIPT_DIR/tici-server" worker "$@"
EOF
chmod +x "$tmpdir/meta_service_server" "$tmpdir/worker_node_server"
tar -C "$tmpdir" -czf "$tmpdir/tici-meta-hotfix.tar.gz" tici-server meta_service_server
tar -C "$tmpdir" -czf "$tmpdir/tici-worker-hotfix.tar.gz" tici-server worker_node_server

file {shlex.quote(args.builder_tidb)}
file {shlex.quote(args.builder_tiflash)}
file {shlex.quote(args.builder_tiflash_proxy)}
file {shlex.quote(args.builder_tiflash_search_lib)}
file {shlex.quote(args.builder_tici)}
"""
    ssh(args.builder_host, package_cmd)

    for package_name in [
        "tidb-hotfix.tar.gz",
        "tiflash-hotfix.tar.gz",
        "tici-meta-hotfix.tar.gz",
        "tici-worker-hotfix.tar.gz",
    ]:
        scp(
            f"{args.builder_host}:{shlex.quote(remote_tmp)}/{package_name}",
            shlex.quote(str(local_package_dir / package_name)),
        )

    ssh(args.builder_host, f"rm -rf {shlex.quote(remote_tmp)}")


def package_set_ready(local_package_dir: Path) -> bool:
    required = [
        "tidb-hotfix.tar.gz",
        "tiflash-hotfix.tar.gz",
        "tici-meta-hotfix.tar.gz",
        "tici-worker-hotfix.tar.gz",
    ]
    return all((local_package_dir / name).exists() for name in required)


def copy_to_host_if_needed(host: str, local_path: Path, remote_path: str):
    local_size = local_path.stat().st_size
    result = ssh(
        host,
        f"if [ -f {shlex.quote(remote_path)} ]; then stat -c %s {shlex.quote(remote_path)}; fi",
        capture_output=True,
    )
    remote_size_text = result.stdout.strip()
    if remote_size_text and int(remote_size_text) == local_size:
        print(f"+ reusing existing remote package {host}:{remote_path}")
        return
    scp(shlex.quote(str(local_path)), f"{host}:{remote_path}")


def install_tici_services(center_host: str, terraform_json, local_package_dir: Path, args):
    remote_package_dir = "~/bench-packages"
    remote_package_abs_dir = "/home/ubuntu/bench-packages"
    meta_hosts = terraform_value(terraform_json, "tici_meta_private_ips")
    worker_hosts = terraform_value(terraform_json, "tici_worker_private_ips")
    tidb_hosts = terraform_value(terraform_json, "private-ip-tidb")
    pd_host = first_host(terraform_value(terraform_json, "private-ip-pd"))
    s3_bucket = terraform_value(terraform_json, "s3-bucket")
    meta_host = first_host(meta_hosts)
    worker_host = first_host(worker_hosts)
    tidb_host = first_host(tidb_hosts)

    for package_name in ["tici-meta-hotfix.tar.gz", "tici-worker-hotfix.tar.gz"]:
        copy_to_host_if_needed(
            center_host,
            local_package_dir / package_name,
            f"{remote_package_abs_dir}/{package_name}",
        )

    command = f"""
set -euo pipefail
mkdir -p {remote_package_dir} ~/tici-meta/conf ~/tici-worker/conf
cat > ~/tici-meta/conf/meta.toml <<'EOF'
[tidb-server]
dsns = ["{mysql_dsn(args.db_user, args.db_password, tidb_host, 4000)}"]

[server]
addr = "0.0.0.0:8500"
advertise-addr = "{meta_host}:8500"
status-addr = "0.0.0.0:8501"
advertise-status-addr = "{meta_host}:8501"
pd-addr = "{pd_host}:2379"

[s3]
endpoint = "{args.s3_endpoint}"
region = "{args.aws_region}"
bucket = "{s3_bucket}"
prefix = "{args.s3_prefix}"
use-path-style = true

[logger]
filename = "tici_meta.log"
level = "info"
EOF

cat > ~/tici-worker/conf/worker.toml <<'EOF'
heartbeat-interval = "3s"
meta-service-wait-timeout = 120

[server]
addr = "0.0.0.0:8510"
advertise-addr = "{worker_host}:8510"
status-addr = "0.0.0.0:8511"
advertise-status-addr = "{worker_host}:8511"
pd-addr = "{pd_host}:2379"

[s3]
endpoint = "{args.s3_endpoint}"
region = "{args.aws_region}"
bucket = "{s3_bucket}"
prefix = "{args.s3_prefix}"
use-path-style = true

[storage]
data-dir = "/home/ubuntu/tici-worker-data"

[frag-writer]
index-num-threads = 4
index-mem-budget = "512MB"
index-flush-interval = "5s"
index-flush-size-limit = "32MB"
poller-interval = "1s"

[compaction]
max-concurrency = 8
index-num-threads = 2
index-mem-budget = "1GB"

[logger]
filename = "tici_worker.log"
level = "info"
EOF

scp -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa {remote_package_dir}/tici-meta-hotfix.tar.gz ubuntu@{meta_host}:~/tici-meta-hotfix.tar.gz
scp -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa {remote_package_dir}/tici-worker-hotfix.tar.gz ubuntu@{worker_host}:~/tici-worker-hotfix.tar.gz
scp -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa ~/tici-meta/conf/meta.toml ubuntu@{meta_host}:~/meta.toml
scp -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa ~/tici-worker/conf/worker.toml ubuntu@{worker_host}:~/worker.toml

ssh -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa ubuntu@{meta_host} '
  bash -lc "
    set -euo pipefail
    mkdir -p ~/tici-meta/bin ~/tici-meta/conf ~/tici-meta/log
    tar -xzf ~/tici-meta-hotfix.tar.gz -C ~/tici-meta/bin
    mv ~/meta.toml ~/tici-meta/conf/meta.toml
    pkill -f /home/ubuntu/tici-meta/conf/meta.toml || true
    pkill -f ~/tici-meta/bin/meta_service_server || true
    nohup ~/tici-meta/bin/meta_service_server --config ~/tici-meta/conf/meta.toml > ~/tici-meta/log/stdout.log 2>&1 < /dev/null &
    sleep 2
    ss -ltn | grep 8500
    ss -ltn | grep 8501
  "
'

ssh -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa ubuntu@{worker_host} '
  bash -lc "
    set -euo pipefail
    mkdir -p ~/tici-worker/bin ~/tici-worker/conf ~/tici-worker/log /home/ubuntu/tici-worker-data
    tar -xzf ~/tici-worker-hotfix.tar.gz -C ~/tici-worker/bin
    mv ~/worker.toml ~/tici-worker/conf/worker.toml
    pkill -f /home/ubuntu/tici-worker/conf/worker.toml || true
    pkill -f ~/tici-worker/bin/worker_node_server || true
    nohup ~/tici-worker/bin/worker_node_server --config ~/tici-worker/conf/worker.toml > ~/tici-worker/log/stdout.log 2>&1 < /dev/null &
    sleep 2
    ss -ltn | grep 8510
    ss -ltn | grep 8511
  "
'
"""
    ssh(center_host, command)


def ensure_changefeed(center_host: str, terraform_json, args):
    cdc_host = first_host(terraform_value(terraform_json, "cdc_private_ips"))
    s3_bucket = terraform_value(terraform_json, "s3-bucket")
    sink_uri = (
        f"s3://{s3_bucket}/{args.s3_prefix}/cdc"
        f"?protocol=canal-json"
        f"&endpoint={args.s3_endpoint}"
        f"&region={args.aws_region}"
        f"&enable-tidb-extension=true"
        f"&output-row-key=true"
        f"&flush-interval=1s"
        f"&use-table-id-as-path=true"
    )

    command = f"""
set -euo pipefail
export PATH="$HOME/.tiup/bin:$PATH"
python3 - <<'PY'
import json
import subprocess
import sys

server = "http://{cdc_host}:8300"
changefeed_id = "{args.changefeed_id}"
sink_uri = "{sink_uri}"

list_cmd = ["tiup", "cdc", "cli", "changefeed", "list", "--server", server]
result = subprocess.run(list_cmd, check=True, text=True, capture_output=True)
feeds = json.loads(result.stdout)
if any(feed.get("id") == changefeed_id for feed in feeds):
    print(f"+ changefeed {{changefeed_id}} already exists")
    sys.exit(0)

create_cmd = [
    "tiup",
    "cdc",
    "cli",
    "changefeed",
    "create",
    "--server",
    server,
    "--changefeed-id",
    changefeed_id,
    "--sink-uri",
    sink_uri,
    "--no-confirm",
]
subprocess.run(create_cmd, check=True)
PY
"""
    ssh(center_host, command)


def ensure_db_user(center_host: str, terraform_json, args):
    tidb_host = first_host(terraform_value(terraform_json, "private-ip-tidb"))
    pd_host = first_host(terraform_value(terraform_json, "private-ip-pd"))
    sql_user = sql_string_literal(args.db_user)
    sql_password = sql_string_literal(args.db_password)
    verify_cmd = f'mysql -h 127.0.0.1 -P 4000 -u {shlex.quote(args.db_user)} -p{shlex.quote(args.db_password)} -e "select user(), current_user();"'

    try:
        ssh(center_host, verify_cmd)
        print(f"+ benchmark user {args.db_user} already works on {center_host}")
        return
    except subprocess.CalledProcessError:
        print(f"+ benchmark user {args.db_user} is not ready, attempting recovery")

    command = f"""
set -euo pipefail
ssh -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa ubuntu@{tidb_host} '
  sudo bash -lc "
    set -euo pipefail
    pkill -f \\"/tidb-deploy/tidb-4000/bin/tidb-server -P 4001\\" >/dev/null 2>&1 || true
    printf \\"[security]\\\\nskip-grant-table = true\\\\n\\" > /tmp/tidb-codex-skip-grant.toml
    nohup /tidb-deploy/tidb-4000/bin/tidb-server -P 4001 --status=10081 --host=127.0.0.1 --advertise-address={tidb_host} --store=tikv --initialize-insecure --path={pd_host}:2379 --config=/tmp/tidb-codex-skip-grant.toml --log-file=/tmp/tidb-codex-4001.log --log-slow-query=/tmp/tidb-codex-4001-slow.log >/tmp/tidb-codex-4001.stdout 2>&1 < /dev/null &
    TEMP_TIDB_PID=\\$!
    for _ in \\$(seq 1 30); do
      mysql -h 127.0.0.1 -P 4001 -u __codex__ -e \\"select 1\\" >/dev/null 2>&1 && break
      sleep 1
    done
    mysql -h 127.0.0.1 -P 4001 -u __codex__ -e \\"DROP USER IF EXISTS '{sql_user}'@'%'; CREATE USER '{sql_user}'@'%' IDENTIFIED BY '{sql_password}'; GRANT ALL PRIVILEGES ON *.* TO '{sql_user}'@'%'; FLUSH PRIVILEGES;\\"
    kill \\$TEMP_TIDB_PID >/dev/null 2>&1 || true
    wait \\$TEMP_TIDB_PID >/dev/null 2>&1 || true
  "
'
{verify_cmd}
"""
    ssh(center_host, command)


def patch_cluster(center_host: str, cluster_name: str, local_package_dir: Path, terraform_json, args):
    remote_package_dir = "~/bench-packages"
    remote_package_abs_dir = "/home/ubuntu/bench-packages"
    ssh(center_host, f"mkdir -p {remote_package_dir}")
    for package_name in [
        "tidb-hotfix.tar.gz",
        "tiflash-hotfix.tar.gz",
    ]:
        copy_to_host_if_needed(
            center_host,
            local_package_dir / package_name,
            f"{remote_package_abs_dir}/{package_name}",
        )

    patch_cmd = f"""
set -euo pipefail
export PATH="$HOME/.tiup/bin:$PATH"
export TIUP_SKIP_UPDATE_CHECK=1
source ~/.bashrc >/dev/null 2>&1 || true
source ~/.zshrc >/dev/null 2>&1 || true
tiup cluster patch {cluster_name} {remote_package_dir}/tidb-hotfix.tar.gz -R tidb --overwrite --yes
tiup cluster patch {cluster_name} {remote_package_dir}/tiflash-hotfix.tar.gz -R tiflash --overwrite --yes
"""
    ensure_db_user(center_host, terraform_json, args)
    ssh(center_host, patch_cmd)
    install_tici_services(center_host, terraform_json, local_package_dir, args)


def build_bench_on_center(center_host: str, remote_dir: str):
    command = f"""
set -euo pipefail
export PATH="$HOME/.cargo/bin:/usr/local/go/bin:$PATH"
cd {shlex.quote(remote_dir)}
cargo build --release --bin aws-hybrid-bench
"""
    ssh(center_host, command)


def download_dataset_on_center(center_host: str, remote_dir: str, rows: int):
    command = f"""
set -euo pipefail
cd {shlex.quote(remote_dir)}

count_shards() {{
  find data/raw -maxdepth 1 -name 'wikipedia_embeddings-*.parquet' 2>/dev/null | wc -l | tr -d ' '
}}

if [[ "$(count_shards)" == "8" ]]; then
  echo "dataset shards already present under data/raw"
  exit 0
fi

running_pid="$(pgrep -af "python3 scripts/download_wiki_embeddings.py --rows {rows}" | awk 'NR==1 {{print $1}}')"
if [[ -n "$running_pid" ]]; then
  echo "waiting for existing dataset download pid=$running_pid"
  while kill -0 "$running_pid" 2>/dev/null; do
    sleep 30
  done
fi

if [[ "$(count_shards)" != "8" ]]; then
  python3 scripts/download_wiki_embeddings.py --rows {rows}
fi
"""
    ssh(center_host, command)


def run_benchmarks(center_host: str, remote_dir: str, scales, output_root: str, args):
    scale_args = " ".join(str(scale) for scale in scales)
    command = f"""
set -euo pipefail
export PATH="$HOME/.cargo/bin:/usr/local/go/bin:$PATH"
cd {shlex.quote(remote_dir)}
for scale in {scale_args}; do
  ./target/release/aws-hybrid-bench \
    --scale-rows "$scale" \
    --dataset-dir data/raw \
    --output-dir {shlex.quote(output_root)} \
    --db-user {shlex.quote(args.db_user)} \
    --db-password {shlex.quote(args.db_password)} \
    --query-concurrency 16 \
    --query-duration 120 \
    --sample-size 5000
done
"""
    ssh(center_host, command)


def fetch_results(center_host: str, remote_dir: str, local_results_dir: Path):
    local_results_dir.mkdir(parents=True, exist_ok=True)
    scp(f"{center_host}:{shlex.quote(remote_dir)}/results", shlex.quote(str(local_results_dir)))


def parse_scales(scale_text: str):
    return [int(item.strip()) for item in scale_text.split(",") if item.strip()]


def build_parser():
    parser = argparse.ArgumentParser(description="Deploy and run AWS TiDB + TiCI benchmark")
    parser.add_argument("--terraform-dir", default="/Users/jin/Desktop/terraform-tici")
    parser.add_argument("--local-repo", default="/Users/jin/Desktop/wiki-vec-bench")
    parser.add_argument("--cluster-name", default=DEFAULT_CLUSTER_NAME)
    parser.add_argument("--builder-host", default=DEFAULT_BUILDER_HOST)
    parser.add_argument("--builder-tidb", default=DEFAULT_BUILDER_TIDB)
    parser.add_argument("--builder-tiflash", default=DEFAULT_BUILDER_TIFLASH)
    parser.add_argument("--builder-tiflash-component-dir", default=DEFAULT_BUILDER_TIFLASH_COMPONENT)
    parser.add_argument("--builder-tiflash-proxy", default=DEFAULT_BUILDER_TIFLASH_PROXY)
    parser.add_argument("--builder-tiflash-search-lib", default=DEFAULT_BUILDER_TIFLASH_SEARCH_LIB)
    parser.add_argument("--builder-tici", default=DEFAULT_BUILDER_TICI)
    parser.add_argument("--remote-dir", default=DEFAULT_CENTER_WORKDIR)
    parser.add_argument("--scales", default=DEFAULT_SCALES)
    parser.add_argument("--aws-region", default=DEFAULT_AWS_REGION)
    parser.add_argument("--s3-endpoint", default=DEFAULT_S3_ENDPOINT)
    parser.add_argument("--s3-prefix", default=DEFAULT_S3_PREFIX)
    parser.add_argument("--changefeed-id", default=DEFAULT_CHANGEFEED_ID)
    parser.add_argument("--db-user", default=DEFAULT_DB_USER)
    parser.add_argument("--db-password", default=DEFAULT_DB_PASSWORD)
    parser.add_argument("--n-tidb", type=int, default=1)
    parser.add_argument("--n-tikv", type=int, default=3)
    parser.add_argument("--n-tiflash", type=int, default=1)
    parser.add_argument("--n-tici-meta", type=int, default=1)
    parser.add_argument("--n-tici-worker", type=int, default=1)
    parser.add_argument("--local-package-dir", default="")
    parser.add_argument("--local-results-dir", default="/Users/jin/Desktop/wiki-vec-bench/aws-results")

    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in [
        "terraform-apply",
        "deploy-cluster",
        "package-binaries",
        "patch-binaries",
        "install-tici",
        "run-bench",
        "all",
    ]:
        subparsers.add_parser(name)
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    terraform_dir = Path(args.terraform_dir)
    local_repo = Path(args.local_repo)
    local_results_dir = Path(args.local_results_dir)
    scales = parse_scales(args.scales)
    local_package_dir = Path(args.local_package_dir) if args.local_package_dir else Path(tempfile.mkdtemp(prefix="aws-bench-packages-"))

    center_host = None
    terraform_json = None
    if args.command in {"deploy-cluster", "patch-binaries", "install-tici", "run-bench", "all"}:
        terraform_json = terraform_output(terraform_dir)
        center_host = extract_center_host(terraform_json)

    if args.command in {"terraform-apply", "all"}:
        update_terraform_counts(
            terraform_dir,
            args.n_tidb,
            args.n_tikv,
            args.n_tiflash,
            args.n_tici_meta,
            args.n_tici_worker,
        )
        ensure_terraform(terraform_dir)
        terraform_json = terraform_output(terraform_dir)
        center_host = extract_center_host(terraform_json)

    if args.command in {"deploy-cluster", "all"}:
        ensure_cluster_deployed(center_host, args.cluster_name)

    if args.command in {"package-binaries", "patch-binaries", "all"}:
        if package_set_ready(local_package_dir):
            print(f"+ using existing hotfix packages from {local_package_dir}")
        else:
            package_linux_binaries_from_builder(args, local_package_dir)

    if args.command in {"patch-binaries", "all"}:
        patch_cluster(center_host, args.cluster_name, local_package_dir, terraform_json, args)
        ensure_changefeed(center_host, terraform_json, args)

    if args.command in {"install-tici"}:
        install_tici_services(center_host, terraform_json, local_package_dir, args)
        ensure_changefeed(center_host, terraform_json, args)

    if args.command in {"run-bench", "all"}:
        ensure_changefeed(center_host, terraform_json, args)
        ensure_center_toolchain(center_host)
        sync_repo_to_center(local_repo, center_host, args.remote_dir)
        build_bench_on_center(center_host, args.remote_dir)
        download_dataset_on_center(center_host, args.remote_dir, max(scales))
        run_benchmarks(center_host, args.remote_dir, scales, f"{args.remote_dir}/results", args)
        fetch_results(center_host, args.remote_dir, local_results_dir)

    if not args.local_package_dir and local_package_dir.exists():
        shutil.rmtree(local_package_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
