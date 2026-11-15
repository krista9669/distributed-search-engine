# Deployment Guide

How to rebuild the four-node cluster from scratch. This is a reference for a new deployment; on the existing cluster, follow the README and RUNBOOK and do not repeat these steps.

> **Warning:** Steps such as formatting the NameNode destroy HDFS metadata. They apply to a brand-new cluster only. Never run them on the existing cluster.

## 1. Topology

| Node | Role | Services |
|---|---|---|
| Node 1 | Ingestion | Spark client and ingestion scripts |
| Node 2 | Storage and compute | HDFS NameNode, DataNode, Spark Worker |
| Node 3 | Compute | Spark Master, Spark Worker, HDFS DataNode |
| Node 4 | Serving | FastAPI, React |

## 2. Prerequisites (all nodes)

- Linux with a dedicated user for the cluster
- Java 11
- Python 3.10
- Passwordless SSH between Nodes 1 to 3 (for Hadoop and Spark control scripts)
- ZeroTier installed and joined to the same network

## 3. ZeroTier network

```bash
curl -s https://install.zerotier.com | sudo bash
sudo zerotier-cli join <NETWORK_ID>
sudo zerotier-cli listnetworks
```

Authorize each node in ZeroTier Central, then confirm each node can reach the others by ZeroTier IP. Record the addresses in `cluster.env`. Open the ports used by the cluster:

| Port | Service |
|---|---|
| 9000 | HDFS NameNode RPC |
| 9870 | HDFS NameNode web UI |
| 7077 | Spark master |
| 8080 | Spark master web UI |
| 8000 | Search API |
| 5173 | Frontend dev server |

## 4. Hadoop 3.3.6 (Nodes 2 and 3)

Install the same version on both nodes and set `JAVA_HOME` and `HADOOP_HOME`.

`core-site.xml`:

```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://10.145.6.245:9000</value>
  </property>
</configuration>
```

`hdfs-site.xml`:

```xml
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///data/hdfs/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///data/hdfs/datanode</value>
  </property>
</configuration>
```

List both nodes in `workers` (Node 2 and Node 3).

First-time start on a **new** cluster only (Node 2):

```bash
hdfs namenode -format      # NEW CLUSTER ONLY
start-dfs.sh
hdfs dfsadmin -report      # expect 2 live DataNodes
hdfs dfs -mkdir -p /warehouse
```

## 5. Spark 3.3.4 standalone (Nodes 2 and 3)

Install the same version on both nodes.

Node 3, start the master:

```bash
$SPARK_HOME/sbin/start-master.sh
```

Nodes 2 and 3, start a worker each:

```bash
$SPARK_HOME/sbin/start-worker.sh spark://10.145.6.188:7077
```

Confirm two ALIVE workers at `http://10.145.6.188:8080`.

## 6. Iceberg

Place the Spark 3.3 / Scala 2.12 runtime JAR (`iceberg-spark-runtime-3.3_2.12-1.8.1.jar`) in `lib/` and pass it to every Spark job. Typical catalog settings for a Hadoop catalog named `lake`:

```text
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lake.type=hadoop
spark.sql.catalog.lake.warehouse=hdfs://10.145.6.245:9000/warehouse
```

Keep these in `cluster.env` or the Spark defaults file so every job uses the same configuration.

## 7. Node 1: ingestion

Install Python 3.10 and Spark client libraries matching the cluster (Spark 3.3.4). Source `cluster.env`, then run the project's Phase 0 ingestion script against `pages.jsonl` and verify 1,000 rows.

## 8. Node 3: processing and export

Run indexing, PageRank (damping 0.85, at least 10 iterations), and the Parquet export through the project's scripts. Confirm the export exists on HDFS and has the five serving fields.

## 9. Node 4: serving

```bash
git clone https://github.com/krista9669/distributed-search-engine.git
cd distributed-search-engine
python3.10 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Place the Parquet export on this machine, then:
export SEARCH_EXPORT_PATH="$HOME/distributed-search-engine/serving/phase0"
uvicorn search.backend.main:app --host 0.0.0.0 --port 8000
```

Frontend:

```bash
cd search/frontend
cp .env.example .env.local        # set VITE_API_URL to the Node 4 address
npm install
npm run dev -- --host 0.0.0.0     # development
npm run build                     # production assets in dist/
```

## 10. Smoke test

```bash
hdfs dfsadmin -report | grep "Live datanodes"      # 2
curl -s http://10.145.6.188:8080 | grep -i alive   # 2 workers
curl -s http://10.145.6.187:8000/health            # index_loaded true
```

Then run a search from the UI and confirm ranked results with BM25, PageRank, and final scores.

## 11. Backups

- Back up Hadoop and Spark configuration files before editing them.
- Keep a copy of the serving export before replacing it.
- Record Iceberg snapshot IDs before any production change so it can be rolled back.
