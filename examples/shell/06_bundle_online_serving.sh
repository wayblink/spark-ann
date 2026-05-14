#!/usr/bin/env bash
# 06: Pattern-B online serving end-to-end smoke test.
#
# Builds a small bundle via spark-shell, starts the api-server, loads
# the bundle through POST /indexes/bundle, runs a search, and asserts
# the response contains hits. Exits non-zero on any failure.
#
# Requirements:
#   - sbt sparkIntegration/assembly  (or any spark-shell with the JAR)
#   - sbt apiServer/assembly         (the api-server fat JAR)
#   - jq (for response inspection)
#
# Run:
#   examples/shell/06_bundle_online_serving.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SPARK_ANN_JAR="$REPO_ROOT/spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar"
API_JAR="$REPO_ROOT/api-server/target/scala-2.12/spark-ann-api-server-assembly.jar"
BUNDLE_DIR="$(mktemp -d -t spark-ann-bundle.XXXXXX)"
PORT="${ANN_PORT:-18080}"

if [[ ! -f "$SPARK_ANN_JAR" ]]; then
  echo "[example] missing $SPARK_ANN_JAR — run: sbt sparkIntegration/assembly"
  exit 2
fi
if [[ ! -f "$API_JAR" ]]; then
  echo "[example] missing $API_JAR — run: sbt apiServer/assembly"
  exit 2
fi

cleanup() {
  if [[ -n "${API_PID:-}" ]] && kill -0 "$API_PID" 2>/dev/null; then
    kill "$API_PID" 2>/dev/null || true
    wait "$API_PID" 2>/dev/null || true
  fi
  rm -rf "$BUNDLE_DIR"
}
trap cleanup EXIT

echo "[example] building a tiny bundle at $BUNDLE_DIR"
cat > "$BUNDLE_DIR/build.scala" <<EOF
import com.wayblink.ann.spark.api.ANNIndexAPI
import com.wayblink.ann.bundle.ANNIndexConfig
val rng = new scala.util.Random(13L)
val rows = (0 until 500).map { i =>
  (10000000L + i.toLong * 7L, (0 until 32).map(_ => rng.nextFloat()).toSeq)
}
import spark.implicits._
val df = rows.toDF("pk", "vector")
val cfg = ANNIndexConfig(M = 16, efConstruction = 100, pk = Some("pk"))
ANNIndexAPI.buildIndex(df, "vector", "$BUNDLE_DIR/idx", cfg)
println("[example] bundle written to $BUNDLE_DIR/idx")
System.exit(0)
EOF

spark-shell --jars "$SPARK_ANN_JAR" -i "$BUNDLE_DIR/build.scala" >/dev/null 2>&1 || {
  echo "[example] spark-shell build failed — falling back to scala -classpath";
}

if [[ ! -d "$BUNDLE_DIR/idx" ]]; then
  echo "[example] bundle not produced; check that spark-shell is on PATH"
  exit 3
fi

echo "[example] starting api-server on port $PORT"
ANN_SERVER_PORT="$PORT" java -jar "$API_JAR" >/dev/null 2>&1 &
API_PID=$!
# Wait up to 20s for the health endpoint.
for _ in $(seq 1 40); do
  if curl -sf "http://localhost:$PORT/api/v1/health/ready" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
curl -sf "http://localhost:$PORT/api/v1/health/ready" >/dev/null

echo "[example] loading bundle"
curl -sf -X POST "http://localhost:$PORT/api/v1/indexes/bundle" \
  -H "Content-Type: application/json" \
  -d "{\"indexId\": \"products\", \"bundlePath\": \"$BUNDLE_DIR/idx\"}" >/dev/null

echo "[example] listing indexes"
curl -sf "http://localhost:$PORT/api/v1/indexes" | jq -r '.indexes[] | "\(.kind)\t\(.indexId)\t\(.size) vectors\t algo=\(.algorithm // "n/a")"'

echo "[example] running a search"
query_json='{"vector":'$(python3 -c "import random,json; random.seed(0); print(json.dumps([random.random() for _ in range(32)]))")',"k":3}'
hits="$(curl -sf -X POST "http://localhost:$PORT/api/v1/indexes/products/search" \
  -H "Content-Type: application/json" \
  -d "$query_json")"
echo "$hits" | jq .
count="$(echo "$hits" | jq '.results | length')"
if [[ "$count" -lt 1 ]]; then
  echo "[example] expected at least one hit"
  exit 4
fi
echo "[example] OK"
