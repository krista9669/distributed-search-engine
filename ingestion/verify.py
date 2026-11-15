#!/usr/bin/env python3
"""Independent check of the table against the raw files (plain Python, no Spark
logic shared with ingest.py), plus Iceberg snapshot / time-travel checks.

  python ingestion/verify.py --through 0
  python ingestion/verify.py --through 1
Add --dry-run to check the parquet stand-in instead of Iceberg.
"""
import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

from pyspark.sql import functions as F  # noqa: E402

import core  # noqa: E402
import pipeline as P  # noqa: E402

FAILS = []


def check(label, ok, detail=""):
    print("%-4s %s %s" % ("PASS" if ok else "FAIL", label, detail))
    if not ok:
        FAILS.append(label)


def expected_state(phase0, phase1, through):
    """Pure-Python reference implementation of the rules."""
    docs = {}  # doc_id -> (version, line_no, record)

    def load(path, phase):
        ops = {}
        with open(path, encoding="utf-8") as f:
            for n, raw in enumerate(f, 1):
                t = core.parse_line(n, raw, phase)
                if t[1] != "ok":
                    continue
                _, _, _, op, doc_id, url, title, text, links, ver, _ = t
                cur = ops.get(doc_id)
                if cur is None or (ver, n) > (cur[0], cur[1]):
                    ops[doc_id] = (ver, n, op, url, title, text, links)
        return ops

    phases = [(phase0, 0)] + ([(phase1, 1)] if through >= 1 else [])
    for path, ph in phases:
        for doc_id, (ver, n, op, url, title, text, links) in load(path, ph).items():
            if op == "DELETE":
                docs.pop(doc_id, None)
            elif doc_id not in docs or ver >= docs[doc_id][0]:
                docs[doc_id] = (ver, url, title, text, links)
    ids = set(docs)
    removed = 0
    final = {}
    for doc_id, (ver, url, title, text, links) in docs.items():
        clean = [x for x in links if x in ids]
        removed += len(links) - len(clean)
        final[doc_id] = (url, title, text, clean)
    return final, removed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--through", type=int, choices=[0, 1], required=True)
    ap.add_argument("--phase0", default=os.path.join(ROOT, "data/phase0/pages.jsonl"))
    ap.add_argument("--phase1", default=os.path.join(ROOT, "data/phase1/changes.json"))
    ap.add_argument("--master", default="local[*]")
    ap.add_argument("--catalog", default="local")
    ap.add_argument("--table", default="search.documents")
    ap.add_argument("--warehouse", default=os.path.join(ROOT, "warehouse_local"))
    ap.add_argument("--state-dir", default=os.path.join(ROOT, "dryrun_state"))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--conf", action="append", default=[])
    a = ap.parse_args()

    spark = P.make_spark("node1-verify", a.master, a.catalog,
                         P.to_uri(a.warehouse), iceberg=not a.dry_run,
                         extra_conf=a.conf)
    if a.dry_run:
        table = P.DryRunTable(spark, a.state_dir)
    else:
        table = P.IcebergTable(spark, a.catalog, a.table)
    df = table.read().persist()

    exp, removed = expected_state(a.phase0, a.phase1, a.through)
    rows = df.collect()
    got = {r["doc_id"]: (r["url"], r["title"], r["text"], list(r["links"] or []))
           for r in rows}

    print("\n===== VERIFY (through phase %d) =====" % a.through)
    check("row count equals distinct doc_id", len(rows) == len(got),
          "(%d rows)" % len(rows))
    check("doc_id set matches independent calculation", set(got) == set(exp),
          "(table %d, expected %d)" % (len(got), len(exp)))
    bad = [d for d in exp if d in got and got[d] != exp[d]]
    check("url/title/text/links identical for every document", not bad,
          "(%d mismatches)" % len(bad))
    ids = set(got)
    dangling = sum(1 for v in got.values() for l in v[3] if l not in ids)
    check("no dangling links", dangling == 0, "(%d)" % dangling)
    nulls = sum(1 for v in got.values() if not (v[0] and v[1] and v[2]))
    check("no empty url/title/text", nulls == 0, "(%d)" % nulls)
    print("INFO links removed vs raw (independent calc): %d" % removed)
    print("INFO phase distribution:", sorted(
        (r["phase"], r["n"]) for r in
        df.groupBy("phase").agg(F.count("*").alias("n")).collect()))

    if not a.dry_run:
        snaps = spark.sql(
            "SELECT snapshot_id, committed_at, operation FROM %s.snapshots "
            "ORDER BY committed_at" % table.name).collect()
        print("\nSnapshots:")
        for s in snaps:
            print("  %s  %s  %s" % (s["snapshot_id"], s["committed_at"],
                                    s["operation"]))
        check("snapshot count == phases applied", len(snaps) == a.through + 1,
              "(%d snapshots)" % len(snaps))
        if len(snaps) >= 2:
            def snap(sid):
                return (spark.read.format("iceberg")
                        .option("snapshot-id", sid).load(table.name)
                        .select("doc_id", "content_hash", "version"))
            old, new = snap(snaps[0]["snapshot_id"]), snap(snaps[-1]["snapshot_id"])
            added = new.join(old, "doc_id", "left_anti").count()
            gone = old.join(new, "doc_id", "left_anti").count()
            changed = (old.alias("o").join(new.alias("n"), "doc_id")
                       .filter("o.content_hash <> n.content_hash "
                               "OR o.version <> n.version").count())
            print("Change detection (first snapshot -> latest): "
                  "added=%d removed=%d changed=%d" % (added, gone, changed))
            check("time travel: Phase 0 snapshot still readable",
                  old.count() > 0, "(%d rows)" % old.count())

    print("\nRESULT: %s" % ("ALL CHECKS PASSED" if not FAILS else
                            "FAILED: " + ", ".join(FAILS)))
    spark.stop()
    sys.exit(1 if FAILS else 0)


if __name__ == "__main__":
    main()
