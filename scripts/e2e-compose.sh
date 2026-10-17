#!/usr/bin/env bash
# End-to-end check of the real deployment shape: FoundationDB, 4 Data Nodes, Controller and
# Gateway from deploy/compose, driven with rmkctl from the image. Needs Docker; tears down after itself.
#
#   scripts/e2e-compose.sh            build the image, run everything
#   SKIP_BUILD=1 scripts/e2e-compose.sh   reuse an existing realmucket:dev
set -euo pipefail

cd "$(dirname "$0")/.."

PROJECT=rmk-e2e
NETWORK=${PROJECT}_default
IMAGE=realmucket:dev
COMPOSE=(docker compose -f deploy/compose/docker-compose.yaml -p "$PROJECT")
WORK=$(mktemp -d "${TMPDIR:-/tmp}/rmk-e2e.XXXXXX")
chmod 777 "$WORK"   # the container runs as uid 10001
# Host port of the gateway; 7000 is often taken (macOS AirPlay Receiver), and this script only uses the compose network.
export RMK_GATEWAY_PORT=${RMK_GATEWAY_PORT:-17000}

step() { printf '\n==> %s\n' "$*"; }
die() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

cleanup() {
    local status=$?
    if [ "$status" -ne 0 ]; then
        printf '\n--- gateway and controller logs ---\n' >&2
        "${COMPOSE[@]}" logs --no-color --tail=30 gateway controller >&2 || true
    fi
    step "tearing down"
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
    rm -rf "$WORK" 2>/dev/null || true
    if [ "$status" -eq 0 ]; then echo "PASS"; else echo "FAILED (exit $status)" >&2; fi
}
trap cleanup EXIT

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

# rmkctl from the image, on the compose network, with $WORK mounted at /work.
ctl() {
    docker run --rm --network "$NETWORK" -v "$WORK:/work" \
        -e RMK_ENDPOINT=gateway:7000 \
        "$IMAGE" rmkctl --insecure "$@"
}

wait_until() {
    local what=$1 seconds=$2
    shift 2
    for ((i = 0; i < seconds; i++)); do
        if "$@" >/dev/null 2>&1; then return 0; fi
        sleep 1
    done
    die "timed out after ${seconds}s waiting for $what"
}

gateway_healthy() {
    "${COMPOSE[@]}" exec -T gateway bash -c \
        'exec 3<>/dev/tcp/127.0.0.1/9100 && printf "GET /healthz HTTP/1.0\r\n\r\n" >&3 && head -n1 <&3 | grep -q " 200 "'
}

# put FILE as KEY, then get it back and compare checksums.
roundtrip() {
    local file=$1 key=$2 want got
    want=$(sha256 "$WORK/$file")
    ctl object put data "$key" "/work/$file" --tenant e2e >/dev/null
    rm -f "$WORK/$key.out"
    ctl object get data "$key" "/work/$key.out" --tenant e2e >/dev/null
    got=$(sha256 "$WORK/$key.out")
    [ "$want" = "$got" ] || die "$key: sha256 differs after the round trip ($want vs $got)"
    echo "$key: sha256 $want"
}

# get an already stored object and compare with the original file.
read_back() {
    local file=$1 key=$2 want got
    want=$(sha256 "$WORK/$file")
    rm -f "$WORK/$key.out"
    ctl object get data "$key" "/work/$key.out" --tenant e2e >/dev/null
    got=$(sha256 "$WORK/$key.out")
    [ "$want" = "$got" ] || die "$key: sha256 differs ($want vs $got)"
    echo "$key: sha256 $want"
}

if [ -z "${SKIP_BUILD:-}" ]; then
    step "building images"
    docker image inspect realmucket-dev:latest >/dev/null 2>&1 ||
        docker build -f docker/dev.Dockerfile -t realmucket-dev .
    docker build -f docker/runtime.Dockerfile -t "$IMAGE" .
fi

step "starting the cluster (project $PROJECT)"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d --no-build
wait_until "the gateway /healthz" 120 gateway_healthy
echo "gateway is healthy"

step "tenant, bucket"
wait_until "the gateway to accept RPCs" 60 ctl tenant create e2e
ctl bucket create data --tenant e2e

step "small object (waits for the Controller's first open Chunks)"
head -c 1000000 /dev/urandom >"$WORK/small.bin"
wait_until "the cluster to accept writes" 90 ctl object put data warmup /work/small.bin --tenant e2e
roundtrip small.bin small

step "multipart object, 100 MiB"
head -c 104857600 /dev/urandom >"$WORK/big.bin"
roundtrip big.bin big

step "one Data Node down, reads continue"
"${COMPOSE[@]}" stop datanode2
read_back small.bin small
read_back big.bin big

step "listing"
ctl object ls data --tenant e2e
