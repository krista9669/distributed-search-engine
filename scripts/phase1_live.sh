#!/usr/bin/env bash
# Phase 1 runbook for Node 1.
#   bash scripts/phase1_live.sh LIVE        real Phase 1 on the production table (evaluation)
#   bash scripts/phase1_live.sh REHEARSAL   same steps on a private throwaway table; production untouched
cd ~/distributed-search-engine || exit 1
source .venv/bin/activate
source cluster.env
MODE="${1:-}"
case "$MODE" in
  LIVE)      T="$TBL"; HDIR=/data/raw/phase1 ;;
  REHEARSAL) T="${TBL}_rehearsal_$(date +%H%M%S)"; HDIR=/tmp/rehearsal/phase1 ;;
  *) echo "Usage: bash scripts/phase1_live.sh LIVE|REHEARSAL"; exit 2 ;;
esac
COMMON=(--master "$MASTER" --catalog "$CAT" --table "$T" --warehouse "$WH" \
        --conf "spark.driver.host=$N1IP" \
        --conf "spark.network.timeout=600s" \
        --conf "spark.executor.heartbeatInterval=60s" \
        --conf "spark.rpc.askTimeout=600s" \
        --conf "spark.task.maxFailures=8" \
        --allow-cluster)
run_ingest() { python ingestion/ingest.py --phase "$1" --input "$2" "${COMMON[@]}" 2>&1 | grep -v WARN; }

echo "== mode: $MODE   table: $CAT.$T"
if [ "$MODE" = "REHEARSAL" ]; then
  echo "== rehearsal only: loading Phase 0 into the throwaway table"
  hdfs dfs -mkdir -p /tmp/rehearsal/phase0
  hdfs dfs -put -f data/phase0/pages.jsonl /tmp/rehearsal/phase0/
  run_ingest 0 "$NN/tmp/rehearsal/phase0/pages.jsonl" | grep -E 'rows after|RESULT'
fi

START=$(date +%s)
echo "== 1/4 upload changes.jsonl to HDFS"
hdfs dfs -mkdir -p "$HDIR"
hdfs dfs -put -f data/phase1/changes.jsonl "$HDIR/"
hdfs dfs -ls "$HDIR"
echo "== 2/4 run Phase 1 ingestion"
run_ingest 1 "$NN$HDIR/changes.jsonl"
cp logs/ingest_phase1_summary.json "logs/ingest_phase1_summary_${MODE}.json"
echo "== 3/4 idempotency check: ingest the same phase again"
run_ingest 1 "$NN$HDIR/changes.jsonl" | grep -E 'rows before|inserted|updated|deleted|rows after|table changed|RESULT'
echo "== 4/4 independent verification"
python ingestion/verify.py --through 1 --master "$MASTER" --catalog "$CAT" --table "$T" \
  --warehouse "$WH" --conf "spark.driver.host=$N1IP" \
  --conf "spark.network.timeout=600s" \
  --conf "spark.executor.heartbeatInterval=60s" \
  --conf "spark.rpc.askTimeout=600s" \
  --conf "spark.task.maxFailures=8" 2>&1 | grep -v WARN
echo "== elapsed for steps 1-4: $(( $(date +%s) - START )) s"
