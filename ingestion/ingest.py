#!/usr/bin/env python3
"""Node 1 ingestion job: one phase per run.

  python ingestion/ingest.py --phase 0 --input data/phase0/pages.jsonl
  python ingestion/ingest.py --phase 1 --input data/phase1/changes.json

Add --dry-run to test everything without Iceberg (parquet stand-in table).
"""
import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

from pyspark.sql import functions as F  # noqa: E402

import pipeline as P  # noqa: E402


def run_phase(spark, table, input_uri, phase, logs_dir):
    os.makedirs(logs_dir, exist_ok=True)
    table.ensure()

    parsed = P.parse_input(spark, input_uri, phase).persist()
    by_status = {r["status"]: r["n"] for r in
                 parsed.groupBy("status").agg(F.count("*").alias("n")).collect()}
    lines_read = sum(by_status.values())
    rejected = by_status.get("reject", 0)

    rejects = (parsed.filter("status = 'reject'")
               .select("line_no", "reason", "raw").orderBy("line_no").collect())
    rej_path = os.path.join(logs_dir, "rejects_phase%d.jsonl" % phase)
    with open(rej_path, "w", encoding="utf-8") as f:
        for r in rejects:
            f.write(json.dumps({"line_no": r["line_no"], "reason": r["reason"],
                                "raw": r["raw"]}) + "\n")

    existing = table.read().persist()
    rows_before = existing.count()
    max_phase = existing.agg(F.max("phase")).collect()[0][0]
    if max_phase is not None and phase < max_phase:
        summary = {
            "phase": phase, "input": input_uri, "skipped": True,
            "skipped_reason": (
                "table already contains Phase %d data, which is newer than "
                "Phase %d" % (max_phase, phase)),
            "lines_read": lines_read, "rejected": rejected,
            "rows_before": rows_before, "rows_after": rows_before,
            "table_changed": False, "table_matches_expected": True,
        }
        with open(os.path.join(logs_dir, "ingest_phase%d_skipped.json" % phase),
                  "w") as f:
            json.dump(summary, f, indent=2)
        return summary

    ok = parsed.filter("status = 'ok'")
    valid = ok.count()
    conflicts = P.count_conflicts(ok)
    unique = P.dedupe(ok).persist()
    n_unique = unique.count()
    n_ups = unique.filter("op = 'UPSERT'").count()
    n_dels = unique.filter("op = 'DELETE'").count()
    dels_absent = (unique.filter("op = 'DELETE'").select("doc_id")
                   .join(existing.select("doc_id"), "doc_id", "left_anti").count())

    final, changes, stale, final_raw, cleaned = P.compute_final(
        existing, unique, phase)
    changes = changes.persist()
    n_changes = changes.count()
    stale_n = stale.count()
    up = changes.filter("op = 'UPSERT'")
    inserted = up.join(existing.select("doc_id"), "doc_id", "left_anti").count()
    updated = up.count() - inserted
    deleted = changes.filter("op = 'DELETE'").count()

    n0 = final_raw.select("doc_id", F.size("links").alias("n0"))
    n1 = cleaned.select("doc_id", F.size("links").alias("n1"))
    lr = (n0.join(n1, "doc_id").agg(
        F.sum(F.col("n0") - F.col("n1")).alias("links_removed"),
        F.sum(F.when(F.col("n0") > F.col("n1"), 1).otherwise(0))
        .alias("docs_touched")).collect()[0])
    links_removed = int(lr["links_removed"] or 0)
    docs_touched = int(lr["docs_touched"] or 0)

    expected = P.digest(final)
    if n_changes > 0:
        table.apply(changes, final=final)
    spark.catalog.clearCache()  # never verify against a stale cached read
    after = table.read()
    actual = P.digest(after)
    ids = after.select("doc_id")
    dangling = (after.select("doc_id", F.explode_outer("links").alias("l"))
                .filter("l IS NOT NULL")
                .join(ids.withColumnRenamed("doc_id", "l"), "l", "left_anti")
                .count())
    ok_check = (expected == actual) and dangling == 0

    summary = {
        "phase": phase, "input": input_uri, "lines_read": lines_read,
        "rejected": rejected, "valid_records": valid,
        "duplicates_dropped": valid - n_unique, "conflicting_duplicates": conflicts,
        "unique_operations": n_unique, "upserts": n_ups, "deletes": n_dels,
        "deletes_for_absent_docs": dels_absent, "stale_upserts_ignored": stale_n,
        "links_removed": links_removed, "docs_with_links_removed": docs_touched,
        "rows_before": rows_before, "inserted": inserted, "updated": updated,
        "deleted": deleted, "rows_after": actual[0],
        "dangling_links_in_table": dangling, "table_matches_expected": ok_check,
        "table_changed": n_changes > 0,
    }
    with open(os.path.join(logs_dir, "ingest_phase%d_summary.json" % phase),
              "w") as f:
        json.dump(summary, f, indent=2)
    return summary


def print_summary(s, mode):
    if s.get("skipped"):
        print("\n===== PHASE %d SUMMARY (%s) =====" % (s["phase"], mode))
        print("SKIPPED: %s." % s["skipped_reason"])
        print("Replaying an older phase would bring back deleted or superseded "
              "documents, so nothing was applied.")
        print("%-26s: %s" % ("rows before", s["rows_before"]))
        print("%-26s: %s" % ("rows after", s["rows_after"]))
        print("%-26s: no (nothing to do)" % "table changed")
        print("RESULT: PASS (table unchanged)")
        return
    rows = [
        ("input lines read", s["lines_read"]),
        ("rejected lines", s["rejected"]),
        ("valid records", s["valid_records"]),
        ("duplicates dropped", s["duplicates_dropped"]),
        ("conflicting duplicates", s["conflicting_duplicates"]),
        ("unique operations", s["unique_operations"]),
        ("  upserts", s["upserts"]),
        ("  deletes", s["deletes"]),
        ("deletes for absent docs", s["deletes_for_absent_docs"]),
        ("stale upserts ignored", s["stale_upserts_ignored"]),
        ("dangling links removed", s["links_removed"]),
        ("docs that lost links", s["docs_with_links_removed"]),
        ("rows before", s["rows_before"]),
        ("  inserted", s["inserted"]),
        ("  updated", s["updated"]),
        ("  deleted", s["deleted"]),
        ("rows after", s["rows_after"]),
        ("dangling links in table", s["dangling_links_in_table"]),
    ]
    print("\n===== PHASE %d SUMMARY (%s) =====" % (s["phase"], mode))
    for k, v in rows:
        print("%-26s: %s" % (k, v))
    print("%-26s: %s" % ("table changed", "yes" if s["table_changed"] else
                         "no (nothing to do)"))
    print("RESULT: %s" % ("PASS" if s["table_matches_expected"] else "FAIL"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", type=int, required=True)
    ap.add_argument("--input", required=True)
    ap.add_argument("--master", default="local[*]")
    ap.add_argument("--catalog", default="local")
    ap.add_argument("--table", default="search.documents")
    ap.add_argument("--warehouse", default=os.path.join(ROOT, "warehouse_local"))
    ap.add_argument("--state-dir", default=os.path.join(ROOT, "dryrun_state"))
    ap.add_argument("--logs-dir", default=os.path.join(ROOT, "logs"))
    ap.add_argument("--dry-run", action="store_true",
                    help="no Iceberg; use a parquet stand-in table")
    ap.add_argument("--conf", action="append", default=[],
                    help="extra Spark conf KEY=VALUE (repeatable)")
    ap.add_argument("--allow-cluster", action="store_true",
                    help="required for hdfs:// warehouse or non-local master")
    a = ap.parse_args()

    warehouse = P.to_uri(a.warehouse)
    if not a.dry_run and not a.allow_cluster and (
            warehouse.startswith("hdfs://") or not a.master.startswith("local")):
        sys.exit("REFUSED: cluster warehouse/master needs --allow-cluster "
                 "(only after the team agrees the shared catalog).")
    if not os.path.exists(os.path.expanduser(a.input)) and "://" not in a.input:
        sys.exit("Input file not found: %s" % a.input)

    spark = P.make_spark("node1-ingest-phase%d" % a.phase, a.master, a.catalog,
                         warehouse, iceberg=not a.dry_run, extra_conf=a.conf)
    if a.dry_run:
        table = P.DryRunTable(spark, a.state_dir)
        mode = "DRY RUN - parquet stand-in, no Iceberg"
    else:
        table = P.IcebergTable(spark, a.catalog, a.table)
        mode = "Iceberg %s.%s @ %s" % (a.catalog, a.table, warehouse)
    summary = run_phase(spark, table, P.to_uri(a.input), a.phase, a.logs_dir)
    print_summary(summary, mode)
    spark.stop()
    sys.exit(0 if summary["table_matches_expected"] else 1)


if __name__ == "__main__":
    main()
