#!/usr/bin/env python3
"""Node 3 - export for Node 4.

Joins the documents table with the PageRank scores and writes plain Parquet
in the layout Node 4's loader expects:

  <final>/part-*.parquet   doc_id, title, url, text (strings, never null),
                           pagerank_score (double, never null)
  <final>/_SUCCESS

The export is written to a staging folder first, checked, and only then moved
to <final>. If a check fails, <final> is left exactly as it was, so Node 4
keeps serving the previous version.
"""
import argparse
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def fail(spark, msg):
    print("=" * 60)
    print("EXPORT FAILED: " + msg)
    print("Nothing was changed in the final export folder.")
    print("=" * 60)
    spark.stop()
    sys.exit(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True)
    ap.add_argument("--pagerank", required=True, help="HDFS folder written by pagerank.py")
    ap.add_argument("--final", required=True, help="e.g. hdfs://.../export/phase0")
    ap.add_argument("--staging", required=True, help="HDFS scratch folder")
    ap.add_argument("--backup", required=True, help="where an older export of the same version is moved")
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--files", type=int, default=2)
    args = ap.parse_args()

    t0 = time.time()
    spark = SparkSession.builder.appName("node3-export " + args.table).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path
    final_p, staging_p, backup_p = Path(args.final), Path(args.staging), Path(args.backup)
    fs = final_p.getFileSystem(hconf)

    if fs.exists(final_p) and not args.overwrite:
        fail(spark, "%s already exists. Rerun with OVERWRITE=1 to replace it "
                    "(the old copy is kept as a backup)." % args.final)

    docs = spark.table(args.table).select(
        "doc_id",
        F.coalesce(F.col("title"), F.lit("")).alias("title"),
        F.coalesce(F.col("url"), F.lit("")).alias("url"),
        F.coalesce(F.col("text"), F.lit("")).alias("text"),
    )
    pr = spark.read.parquet(args.pagerank).select("doc_id", "pagerank_score")

    n_table = docs.count()
    if n_table == 0:
        fail(spark, "table %s has no rows" % args.table)
    if docs.where(F.col("doc_id").isNull()).count() > 0:
        fail(spark, "table has rows with a null doc_id")
    if docs.select("doc_id").distinct().count() != n_table:
        fail(spark, "doc_id is not unique in the table")
    missing = docs.join(pr, "doc_id", "left_anti").count()
    extra = pr.join(docs, "doc_id", "left_anti").count()
    if missing or extra:
        fail(spark, "PageRank scores do not match the table (%d documents without a "
                    "score, %d scores without a document). Rerun pagerank first."
                    % (missing, extra))

    out = (
        docs.join(pr, "doc_id", "left")
        .withColumn("pagerank_score", F.coalesce(F.col("pagerank_score").cast("double"), F.lit(0.0)))
        .select("doc_id", "title", "url", "text", "pagerank_score")
    )
    out.coalesce(args.files).write.mode("overwrite").parquet(args.staging)

    # Check what was actually written before publishing it.
    if not fs.exists(Path(args.staging.rstrip("/") + "/_SUCCESS")):
        fail(spark, "staging export has no _SUCCESS marker")
    back = spark.read.parquet(args.staging)
    n_back = back.count()
    if n_back != n_table:
        fail(spark, "staging export has %d rows, table has %d" % (n_back, n_table))
    nulls = back.where(
        F.col("doc_id").isNull() | F.col("title").isNull() | F.col("url").isNull()
        | F.col("text").isNull() | F.col("pagerank_score").isNull()
    ).count()
    if nulls:
        fail(spark, "staging export has %d rows with null values" % nulls)
    stats = back.agg(F.sum("pagerank_score"), F.max("pagerank_score"), F.min("pagerank_score")).first()

    # Publish: move any older copy aside, then move staging into place.
    backed_up = False
    if fs.exists(final_p):
        fs.mkdirs(backup_p.getParent())
        if not fs.rename(final_p, backup_p):
            fail(spark, "could not move the old export to %s" % args.backup)
        backed_up = True
    fs.mkdirs(final_p.getParent())
    if not fs.rename(staging_p, final_p):
        if backed_up:
            fs.rename(backup_p, final_p)
        fail(spark, "could not move the staging export to %s" % args.final)

    files = [s.getPath().getName() for s in fs.listStatus(final_p)]
    print("=" * 60)
    print("EXPORT OK")
    print("table            : %s" % args.table)
    print("rows exported    : %d" % n_back)
    print("columns          : doc_id, title, url, text (string), pagerank_score (double)")
    print("pagerank sum/max/min : %.6f / %.6f / %.6f" % (stats[0], stats[1], stats[2]))
    print("export folder    : %s" % args.final)
    print("files            : %s" % ", ".join(sorted(files)))
    if backed_up:
        print("old copy kept at : %s" % args.backup)
    print("seconds          : %.1f" % (time.time() - t0))
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
