#!/usr/bin/env bash
# Spark-ANN end-to-end smoke test.
#
# Builds a bundle offline via sbt → starts the api-server → loads the
# bundle → runs an online single-query search → asserts pk passthrough
# end-to-end (the search response id == one of the user pks we
# inserted, which would not be possible without pattern-B working).
#
# This is the script form of docs/STARTUP.md. It exits 0 on success
# and non-zero on any step failure. Tested on macOS with Java 11 +
# sbt 1.9 + curl + jq.
#
# Env overrides:
#   ANN_PORT      api-server port             (default 18080)
#   ANN_WORKDIR   scratch dir for bundle/log  (default /tmp/spark-ann-e2e)
#   ANN_VECTORS   how many vectors to build   (default 800)
#   ANN_DIM       vector dimensionality       (default 32)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PORT="${ANN_PORT:-18080}"
WORKDIR="${ANN_WORKDIR:-/tmp/spark-ann-e2e}"
N_VECTORS="${ANN_VECTORS:-800}"
DIM="${ANN_DIM:-32}"

SPARK_ANN_JAR="$REPO_ROOT/spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar"
API_JAR="$REPO_ROOT/api-server/target/scala-2.12/spark-ann-api-server-assembly.jar"

bundle_dir="$WORKDIR/bundle"
server_log="$WORKDIR/server.log"
server_pid_file="$WORKDIR/server.pid"

# ── housekeeping ──────────────────────────────────────────────────────

cleanup() {
  if [[ -f "$server_pid_file" ]]; then
    local pid
    pid="$(cat "$server_pid_file")"
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
    rm -f "$server_pid_file"
  fi
}
trap cleanup EXIT INT TERM

rm -rf "$WORKDIR"
mkdir -p "$WORKDIR"
cd "$REPO_ROOT"

# ── 0. preflight ──────────────────────────────────────────────────────

echo "[step 0] preflight"
command -v java >/dev/null || { echo "java not on PATH"; exit 2; }
command -v sbt  >/dev/null || { echo "sbt not on PATH";  exit 2; }
command -v curl >/dev/null || { echo "curl not on PATH"; exit 2; }
command -v jq   >/dev/null || { echo "jq not on PATH (brew install jq)"; exit 2; }
java -version 2>&1 | head -1

# ── 1. build assemblies if missing ────────────────────────────────────

if [[ ! -f "$SPARK_ANN_JAR" || ! -f "$API_JAR" ]]; then
  echo "[step 1] building assemblies (one-time, ~30s)"
  sbt 'sparkIntegration/assembly' 'apiServer/assembly' \
    | grep -E '^\[info\] Built:'
else
  echo "[step 1] assemblies already built"
fi
ls -lh "$SPARK_ANN_JAR" "$API_JAR"

# ── 2. offline build via sbt Test/runMain ─────────────────────────────
#
# Spark deps are `% Provided` in spark-integration, so a bare `java -jar
# spark-ann-integration-assembly.jar` won't run. Test scope has Spark in
# scope, which is what OfflineSmoke needs to run as a driver. In a real
# cluster you'd use `spark-submit --class ...OfflineSmoke
# spark-ann-integration-assembly.jar <bundle-path> [n] [dim]` instead.

echo "[step 2] offline build via sbt Test/runMain — bundle at $bundle_dir"
sbt "sparkIntegration/Test/runMain com.wayblink.ann.spark.examples.OfflineSmoke $bundle_dir $N_VECTORS $DIM" \
  | grep -E '\[offline\]' \
  | tee "$WORKDIR/offline.log"

if [[ ! -f "$bundle_dir/ann_index.json" ]]; then
  echo "[step 2] bundle produced no ann_index.json — abort"; exit 3
fi

echo "[step 2] bundle layout:"
find "$bundle_dir" -maxdepth 2 -name 'ann_index.json' -o -name 'boundary_mapping.json' \
  -o -name '*.hnsw' -o -name 'global' -o -name 'local' \
  | sort | sed 's|^|           |'

# ── 3. start api-server ───────────────────────────────────────────────

echo "[step 3] starting api-server on port $PORT"
if curl -sf "http://localhost:$PORT/api/v1/health/ready" >/dev/null 2>&1; then
  echo "    a server is already serving port $PORT — abort"; exit 4
fi

ANN_SERVER_PORT="$PORT" java -jar "$API_JAR" >"$server_log" 2>&1 &
echo $! > "$server_pid_file"

# Up to 30 s for the readiness probe to come back.
for _ in $(seq 1 60); do
  if curl -sf "http://localhost:$PORT/api/v1/health/ready" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
curl -sf "http://localhost:$PORT/api/v1/health/ready" >/dev/null \
  || { echo "    server failed to become ready; tail of $server_log:"; tail -40 "$server_log"; exit 5; }
echo "    ready"

# ── 4. load bundle ────────────────────────────────────────────────────

echo "[step 4] POST /api/v1/indexes/bundle"
load_response="$(curl -sf -X POST "http://localhost:$PORT/api/v1/indexes/bundle" \
  -H "Content-Type: application/json" \
  -d "{\"indexId\":\"products\",\"bundlePath\":\"$bundle_dir\"}")"
echo "    $load_response"

echo "[step 4] GET /api/v1/indexes"
curl -sf "http://localhost:$PORT/api/v1/indexes" | jq .

# ── 5. online single-query search ─────────────────────────────────────

echo "[step 5] online single-query search"
# Build a synthetic query vector of the right dimension. Anything will
# do; the bundle has uniformly random vectors and the routing is
# expected to return a top-k regardless.
query_vec="$(python3 - <<EOF
import json,random
random.seed(1)
print(json.dumps([random.random() for _ in range($DIM)]))
EOF
)"
search_response="$(curl -sf -X POST "http://localhost:$PORT/api/v1/indexes/products/search" \
  -H "Content-Type: application/json" \
  -d "{\"vector\":$query_vec,\"k\":5}")"
echo "$search_response" | jq .

# ── 6. assert pk passthrough end-to-end ───────────────────────────────
#
# The offline driver inserted ids of the shape 10_000_000 + i*7. If the
# api-server is correctly serving the bundle WITH pk passthrough, every
# returned id will follow that shape. If pk passthrough broke, ids
# would be small parquet-row offsets (0, 1, 2, ...).

echo "[step 6] verify ids are user pks (not parquet row offsets)"
min_id="$(echo "$search_response" | jq '[.results[].id] | min')"
if [[ "$min_id" -lt 10000000 ]]; then
  echo "    FAIL — returned id $min_id is below 10_000_000, looks like a parquet row offset"
  echo "    pk passthrough not working in pattern-B serve path"
  exit 6
fi
echo "    OK — all returned ids are user pks (min=$min_id)"

# ── 7. teardown ───────────────────────────────────────────────────────

echo "[step 7] shutting down api-server (will run on exit via trap)"
echo "[done]  end-to-end OK"
echo "        bundle:   $bundle_dir"
echo "        api log:  $server_log"
echo "        offline:  $WORKDIR/offline.log"
