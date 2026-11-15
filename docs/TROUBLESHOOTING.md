# Troubleshooting

Check service state and logs before restarting anything. Never start a duplicate daemon and never run `hdfs namenode -format`.

## Quick diagnostics

```bash
jps                                         # which Java daemons are running on this node
hdfs dfsadmin -report | head -30            # live DataNodes and capacity
hdfs dfs -ls /warehouse                     # warehouse reachable
curl -s http://10.145.6.188:8080 | head     # Spark master UI
curl -s http://127.0.0.1:8000/health        # search API
ping -c 2 10.145.6.245                      # ZeroTier reachability
```

Web UIs: Spark master `http://10.145.6.188:8080`; HDFS NameNode `http://10.145.6.245:9870` (Hadoop 3 default port).

## Spark

| Symptom | Likely cause | What to do |
|---|---|---|
| "Initial job has not accepted any resources" | No workers registered, or the job asks for more cores or memory than available | Open the master UI and confirm both workers are ALIVE with free cores and memory. Lower `--executor-memory` or `--total-executor-cores` only after confirming workers are healthy. |
| A worker is missing from the master UI | Worker not running, wrong master URL, or network issue | On that node run `jps`; check the worker log; confirm it points to `spark://10.145.6.188:7077`. Start it only if `jps` shows it is not running. |
| `ClassNotFoundException` for Iceberg classes | Iceberg runtime JAR not supplied | Pass the Spark 3.3 / Scala 2.12 runtime JAR (version 1.8.1) with `--jars` or the configured classpath. |
| Executors lost mid-job | Out of memory or a worker restarted | Check executor logs on the worker nodes; reduce partition size or executor memory pressure. |
| Port already in use on start | A daemon is already running | Do not start another. Confirm with `jps` and use the existing one. |

## HDFS

| Symptom | Likely cause | What to do |
|---|---|---|
| Only one live DataNode | DataNode on Node 3 (or 2) is down or cannot reach the NameNode | Check `jps` and the DataNode log on that node; verify ZeroTier connectivity to `10.145.6.245:9000`. |
| Under-replicated blocks | A DataNode was down | Bring the DataNode back; HDFS re-replicates automatically (replication factor is 2). |
| Safe mode on startup | NameNode waiting for block reports | Wait; check `hdfs dfsadmin -safemode get`. Do not force it off unless you know why. |
| "Connection refused" to port 9000 | NameNode not running or wrong address | Confirm NameNode on Node 2 with `jps`; confirm `HDFS_URI` in `cluster.env`. |
| Permission denied on a path | Different OS user than the file owner | Run as the Hadoop user the cluster was set up with. |

## Iceberg and ingestion

| Symptom | Likely cause | What to do |
|---|---|---|
| Table not found | Catalog not configured in this Spark session | Source `cluster.env` and use the project's Spark configuration for the `lake` catalog. |
| Phase 1 changes rows on the second run | Idempotency broken or wrong table | Stop. Compare the target table, check deduplication logic, and inspect snapshots. |
| Dangling links after deletes | Link reconciliation did not run | Re-run the reconciliation step on the rehearsal table first, then verify. |
| Need to undo a run | Wrong batch or wrong table | Roll back to the recorded snapshot (see RUNBOOK section 9). |

## Search API (Node 4)

| Symptom | Likely cause | What to do |
|---|---|---|
| `/health` shows `index_loaded: false` | Export path wrong or empty | Check `SEARCH_EXPORT_PATH`; list the directory; confirm Parquet files exist. |
| `documents_loaded` is 0 | Parquet columns not recognized | Confirm columns `doc_id`, `title`, `url`, `text` (and optionally `pagerank_score`). |
| Old data after refreshing the export | API not restarted, or stale files left in the directory | Remove old files, restart uvicorn, recheck `/health`. |
| All PageRank scores 0 | Export lacks `pagerank_score` | The column is optional and defaults to 0; re-export with PageRank. |
| Search returns nothing | Query terms not in corpus | Try a known term such as a title word; check `documents_loaded`. |
| `422` response | `top_k` outside 1 to 50, or missing `q` | Fix the request parameters. |

## Frontend

| Symptom | Likely cause | What to do |
|---|---|---|
| UI shows "backend unreachable" | Wrong `VITE_API_URL` or API down | Check `.env.local`; open the API URL directly; restart the dev server after changing `.env.local`. |
| CORS error in the browser console | API not allowing the frontend origin | Check CORS settings in `search/backend/main.py` for the frontend origin. |
| Works locally, not from another machine | Dev server bound to localhost | Start with `npm run dev -- --host 0.0.0.0` and use the Node 4 ZeroTier IP. |

## Network (ZeroTier)

- Confirm every node shows as online in the ZeroTier network and has the expected `10.145.6.x` address.
- If addresses changed, update `cluster.env` and the Hadoop and Spark configuration, not application code.
- Firewalls must allow ports 9000 (HDFS), 7077 and 8080 (Spark), 8000 (API), and 5173 (frontend).
