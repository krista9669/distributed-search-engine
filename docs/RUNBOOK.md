# Runbook: Live Evaluation

Step-by-step procedure for production Phase 1, using the scripts that exist in this repository. Run each step on the node named in its heading.

**Budget at least 90 minutes.** The Phase 1 rehearsal took about 67.8 minutes end to end, before cluster startup and verification.

> Production Phase 1 is reserved for the live evaluation. Do not run `LIVE` mode earlier.

## 0. Roles

| Node | Owner | Duty |
|---|---|---|
| Node 1 | PES2UG24CS011 | Production Phase 1 ingestion and verification |
| Node 2 | PES2UG24CS361 | Confirms HDFS and worker health |
| Node 3 | PES2UG24CS200 | Confirms Spark; runs index, PageRank and export |
| Node 4 | PES2UG24CS234 | Swaps the serving export; demonstrates search |

Assign one person to write down start and end times and the numbers printed by each step.

## 1. Pre-flight (nothing is changed)

**Node 2.** HDFS is healthy with both DataNodes live.

```bash
jps                                   # NameNode, DataNode, Worker
hdfs dfsadmin -report | head -30      # 2 live datanodes
hdfs dfs -ls /warehouse
```

**Node 3.** Spark master is up and both workers are registered.

```bash
jps                                   # Master, Worker, DataNode
```

Open `http://10.145.6.188:8080` and confirm two workers in state ALIVE.

**Node 1.** Confirm the production table is the intact Phase 0 baseline. `verify.py` only reads.

```bash
cd ~/distributed-search-engine && source .venv/bin/activate && source cluster.env
python ingestion/verify.py --through 0 --master "$MASTER" --catalog "$CAT" --table "$TBL" \
  --warehouse "$WH" --conf "spark.driver.host=$N1IP"
```

Expect all `PASS`, 1,000 rows, and one snapshot. **Write down the printed `snapshot_id` as `PHASE0_SNAPSHOT`**; it is your rollback point.

**Node 4.** The current export serves correctly.

```bash
curl -s http://10.145.6.187:8000/health     # index_loaded true, documents_loaded 1000
```

Stop and use [TROUBLESHOOTING.md](TROUBLESHOOTING.md) if any check fails.

## 2. Check the Phase 1 input file name (Node 1)

`scripts/phase1_live.sh` uploads `data/phase1/changes.jsonl`, but its verification step calls `verify.py` without `--phase1`, and that script defaults to `data/phase1/changes.json`. Make sure the file the script reads exists, or add the flag, before the run:

```bash
ls -l data/phase1/
# If only changes.jsonl exists, edit the verify call in scripts/phase1_live.sh to add:
#   --phase1 data/phase1/changes.jsonl
```

## 3. Phase 1 ingestion (Node 1)

Only after the evaluator authorizes it.

```bash
bash scripts/phase1_live.sh LIVE
```

The script (1) uploads `changes.jsonl` to `/data/raw/phase1` on HDFS, (2) runs Phase 1 ingestion on the production table, (3) runs the same phase again to prove idempotency, and (4) runs independent verification. Its summary is saved to `logs/ingest_phase1_summary_LIVE.json`.

Record from the output: input lines read, rejected, valid, upserts, deletes, rows before, inserted, updated, deleted, rows after, dangling links, and `RESULT`.

Expected if the batch matches the rehearsal: 100 lines, 0 rejected, 50 upserts, 50 deletes, 1,000 rows before, 25 inserted, 25 updated, 50 deleted, 975 rows after, 0 dangling links.

The second run must show 0 inserted, 0 updated, 0 deleted, an unchanged row count, and `table changed: no`. Verification must end with `ALL CHECKS PASSED`.

## 4. Index, PageRank and export (Node 3)

```bash
cd ~/distributed-search-engine
bash scripts/node3_run.sh phase1
```

This runs `indexing/build_index.py`, `pagerank/pagerank.py` and `export/export.py` against `lake.search.documents` and publishes the export to `/export/phase1` on HDFS. The Phase 0 export in `/export/phase0` is not touched. The log is saved in `logs/node3_phase1_<timestamp>.log`.

Confirm the output shows `INDEX OK`, `PAGERANK OK` (sum of scores 1.000000) and `EXPORT OK` with 975 rows. If `/export/phase1` already exists the export refuses to replace it; use `OVERWRITE=1` only if you mean to redo the version (the old copy is kept as a backup).

## 5. Refresh serving data (Node 4)

The backend reads a local path, so bring the export over first. The transfer is not scripted; one way is `hdfs dfs -get /export/phase1 ...` on a cluster node followed by `scp` or `rsync` to Node 4.

```bash
# On Node 4, after the new export is in place at ~/distributed-search-engine/serving/phase1
cd ~/distributed-search-engine && source .venv/bin/activate
pkill -f "uvicorn search.backend.main:app" || true
export SEARCH_EXPORT_PATH=$HOME/distributed-search-engine/serving/phase1
nohup uvicorn search.backend.main:app --host 0.0.0.0 --port 8000 > logs/api.log 2>&1 &
sleep 5
curl -s http://127.0.0.1:8000/health      # index_loaded true, documents_loaded 975
```

The Phase 0 export stays in its own folder, so keep both and switch between them with `SEARCH_EXPORT_PATH`.

## 6. Demonstrate search

Open `http://10.145.6.187:5173`, run two or three queries (for example `machine learning` and `computer`), and point out the BM25, PageRank and rank scores and the query time. Confirm a deleted document no longer appears and an updated document shows its new text.

## 7. Rollback (only if something went wrong)

**Serving only.** Point Node 4 back at the Phase 0 export and restart the API:

```bash
export SEARCH_EXPORT_PATH=$HOME/distributed-search-engine/serving/phase0
pkill -f "uvicorn search.backend.main:app" || true
nohup uvicorn search.backend.main:app --host 0.0.0.0 --port 8000 > logs/api.log 2>&1 &
```

**Production table.** Roll the Iceberg table back to the recorded snapshot. This procedure was not exercised in the rehearsal, so try it on a rehearsal table first. Do not delete the table or the warehouse.

```bash
spark-sql --master "$MASTER" --jars "$ICEBERG_JAR" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=hadoop \
  --conf "spark.sql.catalog.lake.warehouse=$WH" \
  --conf "spark.driver.host=$N1IP" \
  -e "CALL lake.system.rollback_to_snapshot('search.documents', <PHASE0_SNAPSHOT>)"
```

After a rollback, re-run the Phase 0 verification from step 1 to confirm 1,000 rows.

## 8. Record results

Keep the saved logs (`logs/ingest_phase1_summary_LIVE.json` and `logs/node3_phase1_<timestamp>.log`) and the numbers you wrote down in each step. Add the production Phase 1 results to the verified-results section of the README once they are confirmed.
