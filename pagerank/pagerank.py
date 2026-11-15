#!/usr/bin/env python3
"""Node 3 - PageRank over the documents' links.

Reads the Iceberg documents table (read-only), builds the link graph and runs
PageRank on the Spark workers.

  damping factor 0.85, at least 10 iterations (course requirement)
  rank(d) = (1 - 0.85)/N + 0.85 * (sum of rank(s)/outdegree(s) over pages s
            linking to d  +  rank held by pages with no out-links / N)

Pages with no out-links share their rank equally with everyone, so the ranks
always add up to 1.

Output: <output>  Parquet with doc_id string, pagerank_score double
"""
import argparse
import sys
import time
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True)
    ap.add_argument("--output", required=True, help="HDFS folder for the scores")
    ap.add_argument("--iterations", type=int, default=10)
    ap.add_argument("--damping", type=float, default=0.85)
    ap.add_argument("--partitions", type=int, default=4)
    args = ap.parse_args()

    if args.iterations < 10:
        print("PAGERANK FAILED: the course requires at least 10 iterations")
        sys.exit(1)

    t0 = time.time()
    P = args.partitions
    d = args.damping
    spark = SparkSession.builder.appName("node3-pagerank " + args.table).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    docs = spark.table(args.table).select("doc_id", "url", "title", "links").cache()
    n = docs.count()
    if n == 0:
        print("PAGERANK FAILED: table %s has no rows" % args.table)
        spark.stop()
        sys.exit(1)

    keys = (
        docs.select(F.col("doc_id").alias("key"), "doc_id", F.lit(0).alias("pri"))
        .unionByName(docs.select(F.col("url").alias("key"), "doc_id", F.lit(1).alias("pri")))
        .unionByName(docs.select(F.col("title").alias("key"), "doc_id", F.lit(2).alias("pri")))
        .where(F.col("key").isNotNull())
        .groupBy("key")
        .agg(F.min(F.struct("pri", "doc_id")).alias("m"))
        .select("key", F.col("m.doc_id").alias("dst"))
    )
    raw = docs.select(F.col("doc_id").alias("src"), F.explode("links").alias("link"))
    resolved = raw.join(keys, raw["link"] == keys["key"], "left").select("src", "link", "dst").cache()
    n_raw = resolved.count()
    n_unresolved = resolved.where(F.col("dst").isNull()).count()
    edges = (
        resolved.where(F.col("dst").isNotNull() & (F.col("dst") != F.col("src")))
        .select("src", "dst")
        .distinct()
    )

    nodes = docs.select("doc_id").rdd.map(lambda r: (r[0], None)).partitionBy(P)
    links = edges.rdd.map(lambda r: (r[0], r[1])).groupByKey(P).mapValues(list)
    adj = nodes.leftOuterJoin(links, P).mapValues(lambda v: v[1] or []).persist()
    n_edges = adj.map(lambda kv: len(kv[1])).sum()
    n_dangling = adj.filter(lambda kv: len(kv[1]) == 0).count()

    print("pages %d, links in table %d, unresolved %d, edges used %d, pages with no out-links %d"
          % (n, n_raw, n_unresolved, n_edges, n_dangling))

    base = (1.0 - d) / n
    ranks = adj.mapValues(lambda _: 1.0 / n).persist()

    for i in range(1, args.iterations + 1):
        joined = adj.join(ranks, P)
        dangling_mass = (
            joined.filter(lambda kv: len(kv[1][0]) == 0).map(lambda kv: kv[1][1]).sum()
        )
        contribs = joined.flatMap(
            lambda kv: [(dst, kv[1][1] / len(kv[1][0])) for dst in kv[1][0]]
        ).reduceByKey(add, P)
        new_ranks = adj.leftOuterJoin(contribs, P).mapValues(
            lambda v, dm=dangling_mass: base + d * ((v[1] or 0.0) + dm / n)
        ).persist()
        delta = new_ranks.join(ranks, P).map(lambda kv: abs(kv[1][0] - kv[1][1])).sum()
        ranks.unpersist()
        ranks = new_ranks
        print("iteration %2d  total change %.8f" % (i, delta))

    total = ranks.values().sum()
    top = ranks.takeOrdered(10, key=lambda kv: -kv[1])

    out = spark.createDataFrame(
        ranks.map(lambda kv: (kv[0], float(kv[1]))),
        "doc_id string, pagerank_score double",
    )
    out.coalesce(1).write.mode("overwrite").parquet(args.output)
    n_written = spark.read.parquet(args.output).count()

    print("=" * 60)
    print("PAGERANK OK" if n_written == n and abs(total - 1.0) < 1e-6 else "PAGERANK CHECK FAILED")
    print("table            : %s" % args.table)
    print("pages            : %d" % n)
    print("edges            : %d" % n_edges)
    print("iterations       : %d   damping %.2f" % (args.iterations, d))
    print("sum of scores    : %.6f (should be 1.000000)" % total)
    print("scores written   : %d rows to %s" % (n_written, args.output))
    print("top 10 (doc_id, score):")
    for doc_id, score in top:
        print("   %s  %.6f" % (doc_id, score))
    print("seconds          : %.1f" % (time.time() - t0))
    print("=" * 60)
    ok = n_written == n and abs(total - 1.0) < 1e-6
    spark.stop()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
