#!/usr/bin/env bash
# Builds an optimized realmucketd and realmucket-bench, starts a daemon on a scratch directory and
# runs the benchmark against it. Extra arguments go to realmucket-bench, e.g.
#   scripts/bench-local.sh --scenarios small --concurrency 16
# Environment: BENCH_BUILD_DIR (build tree, kept between runs), BENCH_PORT (default 17333).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
tmp="${TMPDIR:-/tmp}"
build="${BENCH_BUILD_DIR:-${tmp%/}/realmucket-bench-build}"
port="${BENCH_PORT:-17333}"
metrics_port=$((port + 1))
work="$(mktemp -d "${tmp%/}/realmucket-bench.XXXXXX")"
daemon_pid=""

cleanup() {
    [[ -n "$daemon_pid" ]] && kill "$daemon_pid" 2>/dev/null && wait "$daemon_pid" 2>/dev/null || true
    rm -rf "$work"
}
trap cleanup EXIT

cmake_args=(-DCMAKE_BUILD_TYPE=Release -DRMK_BUILD_TESTS=OFF)
[[ "$(uname)" == "Darwin" ]] && cmake_args+=(-DCMAKE_CXX_COMPILER=/usr/bin/clang++)
cmake -S "$root" -B "$build" "${cmake_args[@]}" >/dev/null
cmake --build "$build" --target realmucketd realmucket-bench -j"$(getconf _NPROCESSORS_ONLN)" >/dev/null

mkdir -p "$work/keys" "$work/data"
openssl rand -hex 32 >"$work/keys/1"
echo 1 >"$work/keys/active"

ulimit -n 10240 2>/dev/null || true
RMK_INSECURE=true RMK_DATA_DIR="$work/data" RMK_MASTER_KEY_DIR="$work/keys" \
    RMK_LISTEN="127.0.0.1:$port" RMK_METRICS_PORT="$metrics_port" \
    "$build/realmucketd" >"$work/daemon.log" 2>&1 &
daemon_pid=$!

for _ in $(seq 1 100); do
    curl -fs "http://127.0.0.1:$metrics_port/healthz" >/dev/null 2>&1 && break
    kill -0 "$daemon_pid" 2>/dev/null || { cat "$work/daemon.log" >&2; exit 1; }
    sleep 0.1
done

echo "# $(uname -sm), $(git -C "$root" rev-parse --short HEAD 2>/dev/null || echo unversioned), data on $(df -P "$work" | tail -1 | awk '{print $1}')"
"$build/realmucket-bench" --endpoint "127.0.0.1:$port" --insecure "$@"
