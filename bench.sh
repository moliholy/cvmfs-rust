#!/bin/bash
set -euo pipefail

RUST_MOUNT="/tmp/bench_rust"
CPP_MOUNT="/tmp/bench_cpp"
RUST_CACHE="/tmp/bench_rust_cache"
CPP_CACHE="/tmp/bench_cpp_cache"
REPO_URL="http://cvmfs-stratum-one.cern.ch/opt/boss"
REPO_FQRN="boss.cern.ch"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUST_BIN="$SCRIPT_DIR/target/release/cvmfs-cli"
RUNS=100
WARMUP=10

cleanup() {
    umount "$RUST_MOUNT" 2>/dev/null || diskutil unmount force "$RUST_MOUNT" 2>/dev/null || true
    umount "$CPP_MOUNT" 2>/dev/null || diskutil unmount force "$CPP_MOUNT" 2>/dev/null || true
    kill "${RUST_PID:-}" 2>/dev/null || true
    kill "${CPP_PID:-}" 2>/dev/null || true
    sleep 1
    rmdir "$RUST_MOUNT" "$CPP_MOUNT" 2>/dev/null || true
    rm -rf "$RUST_CACHE" "$CPP_CACHE"
}
trap cleanup EXIT

if [ ! -f "$RUST_BIN" ]; then
    echo "ERROR: build first with: cargo build --release"
    exit 1
fi

if ! command -v cvmfs2 &>/dev/null; then
    echo "ERROR: cvmfs2 not found"
    exit 1
fi

if ! command -v hyperfine &>/dev/null; then
    echo "ERROR: hyperfine not found (brew install hyperfine)"
    exit 1
fi

cleanup 2>/dev/null || true
mkdir -p "$RUST_MOUNT" "$CPP_MOUNT" "$RUST_CACHE" "$CPP_CACHE" /var/run/cvmfs

echo "Mounting Rust cvmfs-cli..."
"$RUST_BIN" "$REPO_URL" "$RUST_MOUNT" "$RUST_CACHE" &
RUST_PID=$!
sleep 4

if ! stat "$RUST_MOUNT/testfile" &>/dev/null; then
    echo "ERROR: Rust mount failed"
    kill $RUST_PID 2>/dev/null || true
    exit 1
fi
echo "  OK: $RUST_MOUNT"

echo "Mounting C++..."
cat > /tmp/cvmfs_bench.local <<EOF
CVMFS_CACHE_BASE=$CPP_CACHE
CVMFS_HTTP_PROXY=DIRECT
CVMFS_SERVER_URL="http://cvmfs-stratum-one.cern.ch/cvmfs/@fqrn@"
CVMFS_KEYS_DIR=/etc/cvmfs/keys/cern.ch
EOF
cvmfs2 -o config=/tmp/cvmfs_bench.local "$REPO_FQRN" "$CPP_MOUNT" &
CPP_PID=$!
sleep 4

if ! stat "$CPP_MOUNT/testfile" &>/dev/null; then
    echo "ERROR: C++ mount failed"
    kill $RUST_PID 2>/dev/null || true
    kill $CPP_PID 2>/dev/null || true
    exit 1
fi
echo "  OK: $CPP_MOUNT"

echo "Warming up caches..."
for p in / /testfile /database /pacman-3.29 /pacman-3.29/setup.csh /slc4_ia32_gcc34 /database/run.db /pacman-latest.tar.gz; do
    stat "${RUST_MOUNT}${p}" &>/dev/null || true
    stat "${CPP_MOUNT}${p}" &>/dev/null || true
done
ls "$RUST_MOUNT/" >/dev/null 2>&1
ls "$CPP_MOUNT/" >/dev/null 2>&1
cat "$RUST_MOUNT/testfile" >/dev/null 2>&1
cat "$CPP_MOUNT/testfile" >/dev/null 2>&1
find "$RUST_MOUNT" -maxdepth 3 >/dev/null 2>&1 || true
find "$CPP_MOUNT" -maxdepth 3 >/dev/null 2>&1 || true

# ── Benchmark ──

CVMFS2_VERSION=$(cvmfs2 --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' || echo "unknown")
RUST_VERSION=$(cargo metadata --no-deps --format-version 1 2>/dev/null | python3 -c "import sys,json;print(json.load(sys.stdin)['packages'][0]['version'])" 2>/dev/null || echo "unknown")

echo ""
echo "CVMFS Benchmark: Rust v${RUST_VERSION} vs C++ v${CVMFS2_VERSION}"
echo "Repository: $REPO_FQRN"
echo "Runs: $RUNS per command, warmup: $WARMUP"
echo ""

RUST_WINS=0
CPP_WINS=0
TOTAL=0

run_bench() {
    local label="$1"
    local rust_cmd="$2"
    local cpp_cmd="$3"
    local runs="${4:-$RUNS}"
    local warmup="${5:-$WARMUP}"
    local json_file="/tmp/bench_result.json"

    echo "── $label ──"
    hyperfine \
        --runs "$runs" \
        --warmup "$warmup" \
        --style basic \
        --export-json "$json_file" \
        -n "Rust" "$rust_cmd" \
        -n "C++" "$cpp_cmd"
    echo ""

    local rust_mean cpp_mean
    rust_mean=$(python3 -c "import json;r=json.load(open('$json_file'))['results'];print(r[0]['mean'])")
    cpp_mean=$(python3 -c "import json;r=json.load(open('$json_file'))['results'];print(r[1]['mean'])")

    if python3 -c "exit(0 if $rust_mean <= $cpp_mean else 1)"; then
        RUST_WINS=$((RUST_WINS + 1))
    else
        CPP_WINS=$((CPP_WINS + 1))
    fi
    TOTAL=$((TOTAL + 1))
}

# ── stat ──
run_bench "stat / (root)" \
    "stat $RUST_MOUNT/" \
    "stat $CPP_MOUNT/"
run_bench "stat /testfile" \
    "stat $RUST_MOUNT/testfile" \
    "stat $CPP_MOUNT/testfile"
run_bench "stat /database" \
    "stat $RUST_MOUNT/database" \
    "stat $CPP_MOUNT/database"
run_bench "stat symlink" \
    "stat $RUST_MOUNT/pacman-3.29/setup.csh" \
    "stat $CPP_MOUNT/pacman-3.29/setup.csh"

# ── readdir ──
run_bench "ls / (root)" \
    "ls $RUST_MOUNT/" \
    "ls $CPP_MOUNT/"
run_bench "ls /database" \
    "ls $RUST_MOUNT/database" \
    "ls $CPP_MOUNT/database"
run_bench "ls /pacman-3.29" \
    "ls $RUST_MOUNT/pacman-3.29" \
    "ls $CPP_MOUNT/pacman-3.29"
run_bench "ls /slc4_ia32_gcc34 (nested catalog)" \
    "ls $RUST_MOUNT/slc4_ia32_gcc34" \
    "ls $CPP_MOUNT/slc4_ia32_gcc34"

# ── readlink ──
run_bench "readlink symlink" \
    "readlink $RUST_MOUNT/pacman-3.29/setup.csh" \
    "readlink $CPP_MOUNT/pacman-3.29/setup.csh"

# ── file read ──
run_bench "cat /testfile (50 bytes)" \
    "cat $RUST_MOUNT/testfile" \
    "cat $CPP_MOUNT/testfile"
run_bench "head -c 16 offlinedb.db (chunked)" \
    "head -c 16 $RUST_MOUNT/database/offlinedb.db" \
    "head -c 16 $CPP_MOUNT/database/offlinedb.db"
run_bench "head -c 2 pacman-latest.tar.gz" \
    "head -c 2 $RUST_MOUNT/pacman-latest.tar.gz" \
    "head -c 2 $CPP_MOUNT/pacman-latest.tar.gz"

# ── seek + read ──
run_bench "dd skip into offlinedb.db" \
    "dd if=$RUST_MOUNT/database/offlinedb.db bs=64 count=1 skip=100 2>/dev/null" \
    "dd if=$CPP_MOUNT/database/offlinedb.db bs=64 count=1 skip=100 2>/dev/null"

# ── recursive traversal ──
run_bench "find /pacman-3.29 -maxdepth 1" \
    "find $RUST_MOUNT/pacman-3.29 -maxdepth 1" \
    "find $CPP_MOUNT/pacman-3.29 -maxdepth 1"
run_bench "find /database -type f" \
    "find $RUST_MOUNT/database -type f" \
    "find $CPP_MOUNT/database -type f"
run_bench "find / -maxdepth 3" \
    "find $RUST_MOUNT -maxdepth 3 2>/dev/null || true" \
    "find $CPP_MOUNT -maxdepth 3 2>/dev/null || true"

# ── full file read ──
run_bench "wc -c /testfile" \
    "wc -c $RUST_MOUNT/testfile" \
    "wc -c $CPP_MOUNT/testfile"
run_bench "cat pacman-latest.tar.gz" \
    "cat $RUST_MOUNT/pacman-latest.tar.gz > /dev/null" \
    "cat $CPP_MOUNT/pacman-latest.tar.gz > /dev/null"

# ── hash ──
run_bench "md5 /testfile" \
    "md5 -q $RUST_MOUNT/testfile" \
    "md5 -q $CPP_MOUNT/testfile"
run_bench "md5 pacman-latest.tar.gz" \
    "md5 -q $RUST_MOUNT/pacman-latest.tar.gz" \
    "md5 -q $CPP_MOUNT/pacman-latest.tar.gz"

# ── du ──
run_bench "du -d 2" \
    "du -d 2 -s $RUST_MOUNT 2>/dev/null || true" \
    "du -d 2 -s $CPP_MOUNT 2>/dev/null || true"

# ── heavy (fewer runs, long operations) ──
run_bench "md5 run.db (chunked, 410MB)" \
    "md5 -q $RUST_MOUNT/database/run.db" \
    "md5 -q $CPP_MOUNT/database/run.db" \
    10 2
run_bench "cat run.db (chunked, 410MB)" \
    "cat $RUST_MOUNT/database/run.db > /dev/null" \
    "cat $CPP_MOUNT/database/run.db > /dev/null" \
    10 2

# ── Summary ──
echo ""
printf '=%.0s' {1..60}; echo ""
echo "Rust wins: $RUST_WINS/$TOTAL"
echo "C++ wins:  $CPP_WINS/$TOTAL"
if [ "$RUST_WINS" -gt "$CPP_WINS" ]; then
    echo "Result: Rust is faster overall."
elif [ "$CPP_WINS" -gt "$RUST_WINS" ]; then
    echo "Result: C++ is faster overall."
else
    echo "Result: Tied."
fi
