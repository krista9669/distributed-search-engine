#!/usr/bin/env bash
# Node 3: build the inverted index, run PageRank and export for Node 4.
#
#   bash scripts/node3_run.sh phase0          # Phase 0, real table  -> /export/phase0
#   bash scripts/node3_run.sh phase1          # Phase 1, real table  -> /export/phase1
#   OVERWRITE=1 bash scripts/node3_run.sh phase0   # redo a version (old copy is kept)
#
# Rehearsal on a throwaway table and a throwaway export folder:
#   bash scripts/node3_run.sh rehearsal1 lake.search.<rehearsal_table> /tmp/rehearsal/export
set -euo pipefail

VERSION="${1:?usage: node3_run.sh <version> [table] [export_base]}"
TABLE="${2:-lake.search.documents}"
EXPORT_BASE="${3:-/export}"

NN="${NN:-hdfs://10.145.6.245:9000}"
MASTER="${MASTER:-spark://10.145.6.188:7077}"
DRIVER_HOST="${DRIVER_HOST:-10.145.6.188}"
SPARK_HOME="${SPARK_HOME:-/opt/spark}"
DERIVED_BASE="${DERIVED_BASE:-/derived}"
ITERATIONS="${PAGERANK_ITERATIONS:-10}"

HERE="$(cd "$(dirname "$0")/.." && pwd)"
STAMP="$(date +%Y%m%d-%H%M%S)"
mkdir -p "$HERE/logs"
LOG="$HERE/logs/node3_${VERSION}_${STAMP}.log"

WORK="$NN$DERIVED_BASE/$VERSION"
FINAL="$NN$EXPORT_BASE/$VERSION"

SUBMIT=(
  "$SPARK_HOME/bin/spark-submit"
  --master "$MASTER" --deploy-mode client
  --conf "spark.driver.host=$DRIVER_HOST"
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.lake.type=hadoop
  --conf "spark.sql.catalog.lake.warehouse=$NN/warehouse"
  --conf spark.sql.shuffle.partitions=4
  --conf spark.executor.memory=1g
  --conf spark.network.timeout=600s
  --conf spark.executor.heartbeatInterval=60s
  --conf spark.rpc.askTimeout=600s
  --conf spark.task.maxFailures=8
)

OVERWRITE_FLAG=()
if [ "${OVERWRITE:-0}" = "1" ]; then OVERWRITE_FLAG=(--overwrite); fi

{
  echo "version=$VERSION table=$TABLE export=$FINAL work=$WORK started=$(date)"

  echo; echo "### 1/3 inverted index"
  "${SUBMIT[@]}" "$HERE/indexing/build_index.py" --table "$TABLE" --output "$WORK"

  echo; echo "### 2/3 pagerank"
  "${SUBMIT[@]}" "$HERE/pagerank/pagerank.py" --table "$TABLE" \
      --output "$WORK/pagerank" --iterations "$ITERATIONS"

  echo; echo "### 3/3 export"
  "${SUBMIT[@]}" "$HERE/export/export.py" --table "$TABLE" \
      --pagerank "$WORK/pagerank" --final "$FINAL" \
      --staging "$WORK/export_staging" --backup "$WORK/export_backup_$STAMP" \
      "${OVERWRITE_FLAG[@]}"

  echo; echo "ALL THREE STEPS FINISHED for $VERSION at $(date)"
  echo "Tell Node 4 the export is at: $EXPORT_BASE/$VERSION"
} 2>&1 | tee "$LOG"

echo "log saved to $LOG"
