#!/usr/bin/env python3
"""Node 3 - distributed inverted index.

Reads the Iceberg documents table (read-only), tokenizes title + " " + text
with exactly the same rule Node 4 uses for BM25, and writes to HDFS:

  <output>/index      term string, df int, postings array<struct<doc_id,tf>>
  <output>/doc_stats  doc_id string, doc_len int

The work (tokenizing, counting, grouping) runs on the Spark workers.
"""
import argparse
import re
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

# Must stay identical to Node 4's tokenizer.
TOKEN_RE = re.compile(r"\b[a-z0-9]+\b")


def tokenize_text(s):
    return TOKEN_RE.findall((s or "").lower())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True, help="e.g. lake.search.documents")
    ap.add_argument("--output", required=True, help="HDFS folder for the index")
    ap.add_argument("--partitions", type=int, default=4)
    args = ap.parse_args()

    t0 = time.time()
    spark = SparkSession.builder.appName("node3-index " + args.table).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    docs = (
        spark.table(args.table)
        .select(
            "doc_id",
            F.coalesce(F.col("title"), F.lit("")).alias("title"),
            F.coalesce(F.col("text"), F.lit("")).alias("text"),
        )
        .repartition(args.partitions, "doc_id")
    )

    tokenize = F.udf(tokenize_text, ArrayType(StringType()))
    tokens = docs.select(
        "doc_id", tokenize(F.concat_ws(" ", "title", "text")).alias("tokens")
    ).cache()

    n_docs = tokens.count()
    if n_docs == 0:
        print("INDEX FAILED: table %s has no rows" % args.table)
        spark.stop()
        sys.exit(1)

    doc_stats = tokens.select("doc_id", F.size("tokens").cast("int").alias("doc_len"))

    postings = (
        tokens.select("doc_id", F.explode("tokens").alias("term"))
        .groupBy("term", "doc_id")
        .agg(F.count(F.lit(1)).cast("int").alias("tf"))
    )
    index = postings.groupBy("term").agg(
        F.count(F.lit(1)).cast("int").alias("df"),
        F.sort_array(F.collect_list(F.struct("doc_id", "tf"))).alias("postings"),
    )

    index_path = args.output.rstrip("/") + "/index"
    stats_path = args.output.rstrip("/") + "/doc_stats"
    index.coalesce(args.partitions).write.mode("overwrite").parquet(index_path)
    doc_stats.coalesce(1).write.mode("overwrite").parquet(stats_path)

    idx = spark.read.parquet(index_path)
    st = spark.read.parquet(stats_path)
    n_terms = idx.count()
    n_postings = idx.agg(F.sum("df")).first()[0]
    avgdl = st.agg(F.avg("doc_len")).first()[0]

    print("=" * 60)
    print("INDEX OK")
    print("table            : %s" % args.table)
    print("documents        : %d" % n_docs)
    print("distinct terms   : %d" % n_terms)
    print("postings         : %d" % n_postings)
    print("avg doc length   : %.2f tokens" % avgdl)
    print("index written to : %s" % index_path)
    print("doc stats        : %s" % stats_path)
    print("most common terms (term, df):")
    for r in idx.orderBy(F.desc("df"), "term").select("term", "df").take(5):
        print("   %s  %d" % (r["term"], r["df"]))
    print("seconds          : %.1f" % (time.time() - t0))
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
