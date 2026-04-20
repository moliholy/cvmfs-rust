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
ITERATIONS=50

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

echo "Mounting C++ cvmfs2..."
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

echo "Warming up..."
for p in / /testfile /database /pacman-3.29 /pacman-3.29/setup.csh /slc4_ia32_gcc34 /database/run.db /pacman-latest.tar.gz; do
    stat "${RUST_MOUNT}${p}" &>/dev/null || true
    stat "${CPP_MOUNT}${p}" &>/dev/null || true
done
ls "$RUST_MOUNT/" >/dev/null 2>&1
ls "$CPP_MOUNT/" >/dev/null 2>&1
cat "$RUST_MOUNT/testfile" >/dev/null 2>&1
cat "$CPP_MOUNT/testfile" >/dev/null 2>&1

# ── Helpers ──

# Time N iterations of a command, return median in nanoseconds.
# Uses a single python3 process to avoid per-call startup overhead.
measure() {
    local cmd="$1"
    local n="$2"
    python3 -c "
import subprocess, time, statistics
times = []
for _ in range($n):
    start = time.monotonic_ns()
    subprocess.run('$cmd', shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    times.append(time.monotonic_ns() - start)
print(int(statistics.median(times)))
"
}

fmt_ns() {
    python3 -c "
ns = $1
if ns < 1000:
    print(f'{ns}ns')
elif ns < 1_000_000:
    print(f'{ns/1000:.1f}us')
elif ns < 1_000_000_000:
    print(f'{ns/1_000_000:.2f}ms')
else:
    print(f'{ns/1_000_000_000:.3f}s')
"
}

RUST_WINS=0
CPP_WINS=0
TOTAL=0

run_bench() {
    local label="$1"
    local rust_cmd="$2"
    local cpp_cmd="$3"

    local r_med c_med
    r_med=$(measure "$rust_cmd" "$ITERATIONS")
    c_med=$(measure "$cpp_cmd" "$ITERATIONS")

    local winner pct
    if [ "$r_med" -le "$c_med" ]; then
        winner="Rust"
        RUST_WINS=$((RUST_WINS + 1))
        if [ "$r_med" -gt 0 ]; then
            pct=$(python3 -c "print(f'+{($c_med/$r_med - 1)*100:.0f}%')")
        else
            pct="inf"
        fi
    else
        winner="C++ "
        CPP_WINS=$((CPP_WINS + 1))
        if [ "$c_med" -gt 0 ]; then
            pct=$(python3 -c "print(f'+{($r_med/$c_med - 1)*100:.0f}%')")
        else
            pct="inf"
        fi
    fi
    TOTAL=$((TOTAL + 1))

    printf "  %-40s %12s %12s  %s %s\n" \
        "$label" "$(fmt_ns "$r_med")" "$(fmt_ns "$c_med")" "$winner" "$pct"
}

print_section() {
    echo ""
    echo "== $1 =="
    printf "  %-40s %12s %12s  %s\n" "Operation" "Rust" "C++ cvmfs2" "Winner"
    printf "  %s\n" "$(printf -- '-%.0s' {1..85})"
}

# ── Benchmarks ──

CVMFS2_VERSION=$(cvmfs2 --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' || echo "unknown")
RUST_VERSION=$(cargo metadata --no-deps --format-version 1 2>/dev/null | python3 -c "import sys,json;print(json.load(sys.stdin)['packages'][0]['version'])" 2>/dev/null || echo "unknown")

echo ""
echo "CVMFS Benchmark: Rust v${RUST_VERSION} (FUSE) vs C++ cvmfs2 v${CVMFS2_VERSION} (FUSE)"
echo "Repository: $REPO_FQRN"
echo "Iterations: $ITERATIONS per operation (after warmup)"
echo "Rust mount: $RUST_MOUNT"
echo "C++ mount:  $CPP_MOUNT"

print_section "stat"
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

print_section "ls (readdir)"
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

print_section "readlink"
run_bench "readlink symlink" \
    "readlink $RUST_MOUNT/pacman-3.29/setup.csh" \
    "readlink $CPP_MOUNT/pacman-3.29/setup.csh"

print_section "cat (file read)"
run_bench "cat /testfile (50 bytes)" \
    "cat $RUST_MOUNT/testfile" \
    "cat $CPP_MOUNT/testfile"
run_bench "head -c 16 offlinedb.db (chunked)" \
    "head -c 16 $RUST_MOUNT/database/offlinedb.db" \
    "head -c 16 $CPP_MOUNT/database/offlinedb.db"
run_bench "head -c 2 pacman-latest.tar.gz" \
    "head -c 2 $RUST_MOUNT/pacman-latest.tar.gz" \
    "head -c 2 $CPP_MOUNT/pacman-latest.tar.gz"

print_section "dd (seek + read)"
run_bench "dd skip into offlinedb.db" \
    "dd if=$RUST_MOUNT/database/offlinedb.db bs=64 count=1 skip=100" \
    "dd if=$CPP_MOUNT/database/offlinedb.db bs=64 count=1 skip=100"

print_section "find (recursive traversal)"
run_bench "find /pacman-3.29 -maxdepth 1" \
    "find $RUST_MOUNT/pacman-3.29 -maxdepth 1" \
    "find $CPP_MOUNT/pacman-3.29 -maxdepth 1"
run_bench "find /database -type f" \
    "find $RUST_MOUNT/database -type f" \
    "find $CPP_MOUNT/database -type f"

print_section "wc (full file read + count)"
run_bench "wc -c /testfile" \
    "wc -c $RUST_MOUNT/testfile" \
    "wc -c $CPP_MOUNT/testfile"
run_bench "wc -c /pacman-latest.tar.gz" \
    "wc -c $RUST_MOUNT/pacman-latest.tar.gz" \
    "wc -c $CPP_MOUNT/pacman-latest.tar.gz"

print_section "md5 (hash full file)"
run_bench "md5 /testfile" \
    "md5 -q $RUST_MOUNT/testfile" \
    "md5 -q $CPP_MOUNT/testfile"

print_section "full file read"
run_bench "cat pacman-latest.tar.gz (full)" \
    "cat $RUST_MOUNT/pacman-latest.tar.gz" \
    "cat $CPP_MOUNT/pacman-latest.tar.gz"

print_section "recursive traversal"
run_bench "find / -maxdepth 3 (all entries)" \
    "find $RUST_MOUNT -maxdepth 3" \
    "find $CPP_MOUNT -maxdepth 3"

print_section "du (recursive stat)"
run_bench "du -s / -maxdepth 2" \
    "du -d 2 -s $RUST_MOUNT" \
    "du -d 2 -s $CPP_MOUNT"

print_section "hash large files"
run_bench "md5 run.db (chunked, 410MB)" \
    "md5 -q $RUST_MOUNT/database/run.db" \
    "md5 -q $CPP_MOUNT/database/run.db"
run_bench "md5 pacman-latest.tar.gz" \
    "md5 -q $RUST_MOUNT/pacman-latest.tar.gz" \
    "md5 -q $CPP_MOUNT/pacman-latest.tar.gz"

# ── Heavy benchmarks (1 iteration, >5s operations) ──
ITERATIONS=1

print_section "heavy (1 iteration)"
run_bench "cat run.db (chunked, 410MB)" \
    "cat $RUST_MOUNT/database/run.db" \
    "cat $CPP_MOUNT/database/run.db"
run_bench "find / -maxdepth 3 -type f" \
    "find $RUST_MOUNT -maxdepth 3 -type f" \
    "find $CPP_MOUNT -maxdepth 3 -type f"

echo ""
printf '=%.0s' {1..87}; echo ""
echo "Rust wins: $RUST_WINS/$TOTAL"
echo "C++ wins:  $CPP_WINS/$TOTAL"
if [ "$RUST_WINS" -gt "$CPP_WINS" ]; then
    echo "Result: Rust is faster overall."
elif [ "$CPP_WINS" -gt "$RUST_WINS" ]; then
    echo "Result: C++ cvmfs2 is faster overall."
else
    echo "Result: Tied."
fi
