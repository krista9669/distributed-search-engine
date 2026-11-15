"""Spark transforms + table back-ends for Node 1 ingestion.

Flow for one phase:
  parse (Spark) -> reject bad lines -> dedupe by doc_id (highest version wins)
  -> apply to existing table state -> clean dangling links -> diff
  -> MERGE the diff into Iceberg (or write parquet in --dry-run) -> verify.
"""
import os
import shutil
import sys

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import core  # noqa: E402

TABLE_COLS = ["doc_id", "url", "title", "text", "links", "version", "phase",
              "content_hash"]
BASE_COLS = ["doc_id", "url", "title", "text", "links", "version", "phase"]

PARSED_SCHEMA = T.StructType([
    T.StructField("line_no", T.LongType()),
    T.StructField("status", T.StringType()),
    T.StructField("reason", T.StringType()),
    T.StructField("op", T.StringType()),
    T.StructField("doc_id", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("text", T.StringType()),
    T.StructField("links", T.ArrayType(T.StringType())),
    T.StructField("version", T.IntegerType()),
    T.StructField("raw", T.StringType()),
])

TABLE_SCHEMA = T.StructType([
    T.StructField("doc_id", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("text", T.StringType()),
    T.StructField("links", T.ArrayType(T.StringType())),
    T.StructField("version", T.IntegerType()),
    T.StructField("phase", T.IntegerType()),
    T.StructField("content_hash", T.StringType()),
])


def to_uri(path):
    """Local paths become explicit file:// URIs so a later HDFS default
    filesystem can never hijack them. hdfs:// and file:// pass through."""
    if "://" in path:
        return path
    return "file://" + os.path.abspath(os.path.expanduser(path))


# --------------------------------------------------------------------------
# Spark session
# --------------------------------------------------------------------------
def make_spark(app, master, catalog=None, warehouse=None, iceberg=True,
               extra_conf=()):
    b = (SparkSession.builder.appName(app).master(master)
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.ui.showConsoleProgress", "false"))
    if iceberg:
        jar = os.environ.get("ICEBERG_JAR")
        if jar:
            b = b.config("spark.jars", jar)
        b = (b.config("spark.sql.extensions",
                      "org.apache.iceberg.spark.extensions."
                      "IcebergSparkSessionExtensions")
             .config("spark.sql.catalog.%s" % catalog,
                     "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.%s.type" % catalog, "hadoop")
             .config("spark.sql.catalog.%s.warehouse" % catalog, warehouse))
    for kv in extra_conf:
        k, _, v = kv.partition("=")
        b = b.config(k, v)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# --------------------------------------------------------------------------
# Transforms
# --------------------------------------------------------------------------
def parse_input(spark, path, phase):
    sc = spark.sparkContext
    for f in ("config.py", "core.py"):
        sc.addPyFile(os.path.join(HERE, f))
    rdd = (sc.textFile(path).zipWithIndex()
           .map(lambda p: core.parse_line(p[1] + 1, p[0], phase)))
    return spark.createDataFrame(rdd, PARSED_SCHEMA)


def dedupe(ok_df):
    """One row per doc_id: highest version wins; on a tie the later line wins."""
    w = Window.partitionBy("doc_id").orderBy(F.col("version").desc(),
                                             F.col("line_no").desc())
    return (ok_df.withColumn("_rn", F.row_number().over(w))
            .filter("_rn = 1").drop("_rn"))


def count_conflicts(ok_df):
    """Same doc_id AND same version but different content (real conflicts)."""
    h = F.sha2(F.to_json(F.struct("op", "url", "title", "text", "links")), 256)
    g = (ok_df.withColumn("_h", h).groupBy("doc_id", "version")
         .agg(F.count("*").alias("n"), F.countDistinct("_h").alias("d")))
    return g.filter("n > 1 AND d > 1").count()


def with_hash(df):
    return df.withColumn(
        "content_hash",
        F.sha2(F.to_json(F.struct("url", "title", "text", "links")), 256))


def clean_links(df, valid_ids):
    """Drop link targets that are not in valid_ids; keep order; keep the doc."""
    v = valid_ids.select(F.col("doc_id").alias("_vid")).distinct()
    ex = df.select("doc_id", F.posexplode_outer("links").alias("_pos", "_link"))
    kept = ex.join(v, ex["_link"] == v["_vid"], "inner")
    agg = (kept.groupBy("doc_id")
           .agg(F.sort_array(F.collect_list(F.struct("_pos", "_link")))
                .alias("_s")))
    return (df.drop("links").join(agg, "doc_id", "left")
            .withColumn("links", F.coalesce(
                F.transform("_s", lambda x: x["_link"]),
                F.array().cast("array<string>")))
            .drop("_s"))


def compute_final(existing, unique, phase):
    """Apply one phase of de-duplicated operations to the existing table state.

    Returns (final_df, changes_df, stale_upserts_df):
      final_df    complete table content after this phase
      changes_df  minimal diff vs existing: op UPSERT (new/changed) or DELETE
      stale_df    UPSERTs ignored because the table already holds a higher version
    """
    existing = existing.select(*TABLE_COLS)
    deletes = unique.filter("op = 'DELETE'").select("doc_id")
    ups = (unique.filter("op = 'UPSERT'")
           .select("doc_id", "url", "title", "text", "links", "version")
           .withColumn("phase", F.lit(phase).cast("int")))

    e_ver = existing.select("doc_id", F.col("version").alias("_e_ver"))
    ups_j = ups.join(e_ver, "doc_id", "left")
    is_stale = F.col("_e_ver").isNotNull() & (F.col("version") < F.col("_e_ver"))
    accepted = ups_j.filter(~is_stale).drop("_e_ver")
    stale = ups_j.filter(is_stale).drop("_e_ver")

    kept = (existing.join(deletes, "doc_id", "left_anti")
            .join(accepted.select("doc_id"), "doc_id", "left_anti")
            .select(*BASE_COLS))
    final_raw = kept.unionByName(accepted.select(*BASE_COLS))
    cleaned = clean_links(final_raw, final_raw.select("doc_id"))
    final = with_hash(cleaned).select(*TABLE_COLS)

    e = existing.select("doc_id", F.col("content_hash").alias("_e_hash"),
                        F.col("version").alias("_e_version"))
    changed = (final.join(e, "doc_id", "left")
               .filter(F.col("_e_hash").isNull()
                       | (F.col("_e_hash") != F.col("content_hash"))
                       | (F.col("_e_version") != F.col("version")))
               .drop("_e_hash", "_e_version")
               .select(F.lit("UPSERT").alias("op"), *TABLE_COLS))
    gone = (existing.join(final.select("doc_id"), "doc_id", "left_anti")
            .select(F.lit("DELETE").alias("op"), "doc_id",
                    F.lit(None).cast("string").alias("url"),
                    F.lit(None).cast("string").alias("title"),
                    F.lit(None).cast("string").alias("text"),
                    F.lit(None).cast("array<string>").alias("links"),
                    F.lit(None).cast("int").alias("version"),
                    F.lit(None).cast("int").alias("phase"),
                    F.lit(None).cast("string").alias("content_hash")))
    return final, changed.unionByName(gone), stale, final_raw, cleaned


def digest(df):
    """(row count, order-independent checksum) of a table-shaped DataFrame."""
    r = df.agg(
        F.count("*").alias("n"),
        F.sum(F.xxhash64("doc_id", "content_hash", "version") % F.lit(1000003))
        .alias("d")).collect()[0]
    return int(r["n"]), int(r["d"] or 0)


# --------------------------------------------------------------------------
# Table back-ends
# --------------------------------------------------------------------------
class IcebergTable:
    def __init__(self, spark, catalog, table):
        self.spark = spark
        self.catalog = catalog
        self.name = "%s.%s" % (catalog, table)
        self.namespace = "%s.%s" % (catalog, table.rsplit(".", 1)[0])

    def ensure(self):
        s = self.spark
        s.sql("CREATE NAMESPACE IF NOT EXISTS %s" % self.namespace)
        s.sql("""CREATE TABLE IF NOT EXISTS %s (
                   doc_id string, url string, title string, text string,
                   links array<string>, version int, phase int,
                   content_hash string)
                 USING iceberg
                 TBLPROPERTIES ('format-version'='2')""" % self.name)

    def read(self):
        return self.spark.table(self.name).select(*TABLE_COLS)

    def apply(self, changes, final=None):
        changes.createOrReplaceTempView("node1_changes")
        self.spark.sql("""
            MERGE INTO %s t USING node1_changes s ON t.doc_id = s.doc_id
            WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
            WHEN MATCHED AND s.op = 'UPSERT' THEN UPDATE SET
                t.url = s.url, t.title = s.title, t.text = s.text,
                t.links = s.links, t.version = s.version, t.phase = s.phase,
                t.content_hash = s.content_hash
            WHEN NOT MATCHED AND s.op = 'UPSERT' THEN INSERT
                (doc_id, url, title, text, links, version, phase, content_hash)
                VALUES (s.doc_id, s.url, s.title, s.text, s.links, s.version,
                        s.phase, s.content_hash)
        """ % self.name)


class DryRunTable:
    """Parquet-backed stand-in for the Iceberg table. No catalog is touched."""

    def __init__(self, spark, state_dir):
        self.spark = spark
        self.path = os.path.join(os.path.expanduser(state_dir), "documents")

    def ensure(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

    def read(self):
        if os.path.isdir(self.path):
            return self.spark.read.parquet("file://" + self.path)\
                .select(*TABLE_COLS)
        return self.spark.createDataFrame([], TABLE_SCHEMA)

    def apply(self, changes, final=None):
        tmp = self.path + ".new"
        shutil.rmtree(tmp, ignore_errors=True)
        final.write.parquet("file://" + tmp)
        shutil.rmtree(self.path, ignore_errors=True)
        os.rename(tmp, self.path)
