# Runbook: Live Evaluation

Step-by-step procedure for the live evaluation. Items marked `TODO(team)` need the exact script name from `scripts/`.

**Budget at least 90 minutes.** The Phase 1 rehearsal took about 67.8 minutes end to end, before cluster startup and verification.

Run every command on the node named in its heading, after `source cluster.env`.

---

## 0. Roles during the run

| Node | Owner | Duty |
|---|---|---|
| Node 1 | PES2UG24CS011 | Runs production Phase 1 ingestion |
| Node 2 | PES2UG24CS361 | Confirms HDFS and worker health |
| Node 3 | PES2UG24CS200 | Confirms Spark; runs PageRank and export |
| Node 4 | PES2UG24CS234 | Swaps the serving export; demonstrates search |

Assign one person to record start and end times and the numbers printed by each step.

## 1. Pre-flight checks (before anything is changed)

**Node 2:** HDFS is healthy and both DataNodes are live.

```bash
jps                                  # expect NameNode and DataNode (plus Worker)
hdfs dfsadmin -report | head -30     # expect 2 live datanodes
hdfs dfs -ls /warehouse
```

**Node 3:** Spark master is up and both workers are registered.

```bash
jps                                  # expect Master, Worker, DataNode
curl -s http://10.145.6.188:8080 | grep -i "alive workers"   # expect 2
```

Spark master UI: `http://10.145.6.188:8080` (expect two workers in state ALIVE).

**Node 1:** confirm the production table is intact before touching it.

```bash
spark-sql --jars "$ICEBERG_JAR" -e "SELECT COUNT(*), COUNT(DISTINCT doc_id) FROM lake.search.documents"
# expect 1000 and 1000
```

**Node 4:** the current Phase 0 export serves correctly.

```bash
curl -s http://10.145.6.187:8000/health   # expect index_loaded true, documents_loaded 1000
```

Stop here if any check fails and use [TROUBLESHOOTING.md](TROUBLESHOOTING.md).

## 2. Record a rollback point

On Node 1, note the current snapshot ID of the production table. This is how Phase 1 is undone if needed.

```bash
spark-sql --jars "$ICEBERG_JAR" -e \
  "SELECT snapshot_id, committed_at, operation FROM lake.search.documents.snapshots ORDER BY committed_at"
```

Write down the latest `snapshot_id` as `PHASE0_SNAPSHOT`.

Back up the Phase 0 serving export on Node 4:

```bash
cp -r "$SEARCH_EXPORT_PATH" "${SEARCH_EXPORT_PATH}_backup_$(date +%Y%m%d_%H%M)"
```

## 3. Run production Phase 1 (Node 1)

Only after the evaluator authorizes it.

```bash
# TODO(team): replace with the real Phase 1 script and arguments
bash scripts/<phase1_script>.sh --input data/phase1/changes.jsonl --table lake.search.documents
```

Record from the output: input lines, valid, rejected, UPSERT, DELETE, rows before, inserted, updated, deleted, rows after.

Expected for the rehearsal batch (production should match if the batch is the same): 100 / 100 / 0 / 50 / 50 / 1,000 / 25 / 25 / 50 / 975.

## 4. Idempotency check (Node 1)

Re-run the same command. Expect 0 inserted, 0 updated, 0 deleted and a final count that does not change.

## 5. Independent verification (Node 1)

```bash
# TODO(team): replace with the real verification script
bash scripts/<verify_phase1_script>.sh --table lake.search.documents
```

Confirm: row count equals distinct `doc_id` count; the ID set matches the expected set; no dangling links; no empty `url`, `title`, or `text`; snapshot count increased; the Phase 0 snapshot is readable by time travel:

```bash
spark-sql --jars "$ICEBERG_JAR" -e \
  "SELECT COUNT(*) FROM lake.search.documents VERSION AS OF <PHASE0_SNAPSHOT>"   # expect 1000
```

## 6. Recompute PageRank and export (Node 3)

```bash
# TODO(team): replace with the real PageRank and export scripts
bash scripts/<pagerank_script>.sh
bash scripts/<export_script>.sh
hdfs dfs -ls <export_path_on_hdfs>
```

Confirm the export has 975 rows and the five serving fields, and that the PageRank sum is 1.0.

## 7. Refresh the serving data (Node 4)

```bash
# Copy the new export from HDFS to the Node 4 export directory
hdfs dfs -get <export_path_on_hdfs>/* "$SEARCH_EXPORT_PATH"/   # or the team's usual transfer method

# Restart the API so it rebuilds the BM25 index
pkill -f "uvicorn search.backend.main:app" || true
source .venv/bin/activate
nohup uvicorn search.backend.main:app --host 0.0.0.0 --port 8000 > logs/api.log 2>&1 &

curl -s http://127.0.0.1:8000/health      # expect documents_loaded 975
```

Clear old files from `$SEARCH_EXPORT_PATH` first if the loader reads every file in the directory, so stale rows are not mixed in.

Spot-check a deleted document no longer appears and an updated document shows new text.

## 8. Demonstrate search

Open `http://10.145.6.187:5173` and run two or three queries (for example "Alan Turing" and "machine learning"). Point out BM25, PageRank, and final scores, snippets, and query timing.

## 9. Rollback (only if something went wrong)

Undo Phase 1 on the production table by rolling back to the recorded snapshot:

```bash
spark-sql --jars "$ICEBERG_JAR" -e \
  "CALL lake.system.rollback_to_snapshot('search.documents', <PHASE0_SNAPSHOT>)"
```

Then restore the backed-up export on Node 4 and restart the API. Do not delete the table or warehouse.
