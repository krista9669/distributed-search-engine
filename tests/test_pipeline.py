"""End-to-end tests on tiny synthetic files, using the parquet stand-in table
(no Iceberg needed). Each test runs real Spark in local mode."""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

import ingest  # noqa: E402
import pipeline as P  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = P.make_spark("node1-tests", "local[2]", iceberg=False)
    yield s
    s.stop()


def write(path, lines):
    with open(path, "w") as f:
        for l in lines:
            f.write((l if isinstance(l, str) else json.dumps(l)) + "\n")
    return "file://" + str(path)


def doc(i, links=None, **kw):
    r = {"doc_id": str(i), "url": "u%s" % i, "title": "t%s" % i, "text": "x%s" % i}
    if links is not None:
        r["links"] = links
    r.update(kw)
    return r


def run(spark, tmp_path, phase, uri):
    table = P.DryRunTable(spark, str(tmp_path / "state"))
    s = ingest.run_phase(spark, table, uri, phase, str(tmp_path / "logs"))
    return s, {r["doc_id"]: r for r in table.read().collect()}


def test_phase0_reject_dedupe_missing_links(spark, tmp_path):
    uri = write(tmp_path / "p0.jsonl", [
        doc(1, ["2", "99"]), doc(2, []), doc(3),  # 3 has no links field
        doc(3),                                     # exact duplicate
        '{"doc_id": "bad", "url":',                 # malformed
        {"doc_id": "5", "url": "u", "title": "t"},  # missing text
    ])
    s, rows = run(spark, tmp_path, 0, uri)
    assert s["rejected"] == 2 and s["duplicates_dropped"] == 1
    assert sorted(rows) == ["1", "2", "3"]
    assert rows["1"]["links"] == ["2"]             # dangling 99 removed
    assert rows["3"]["links"] == []
    assert s["links_removed"] == 1 and s["table_matches_expected"]


def test_highest_version_wins_inside_file(spark, tmp_path):
    uri = write(tmp_path / "p0.jsonl", [
        doc(1, [], title="new", version=5), doc(1, [], title="old", version=2)])
    s, rows = run(spark, tmp_path, 0, uri)
    assert rows["1"]["title"] == "new" and rows["1"]["version"] == 5


def test_phase1_upsert_delete_and_relink(spark, tmp_path):
    p0 = write(tmp_path / "p0.jsonl", [doc(1, ["2", "3"]), doc(2, ["1"]), doc(3, [])])
    run(spark, tmp_path, 0, p0)
    p1 = write(tmp_path / "p1.jsonl", [
        {"doc_id": "3", "op": "DELETE"},
        {"doc_id": "9", "op": "DELETE"},               # absent: no-op
        dict(doc(2, ["1", "77"]), op="UPSERT", title="changed"),
        dict(doc(4, ["1", "3"]), op="UPSERT"),
    ])
    s, rows = run(spark, tmp_path, 1, p1)
    assert sorted(rows) == ["1", "2", "4"]
    assert rows["1"]["links"] == ["2"]      # existing doc relinked (3 deleted)
    assert rows["2"]["links"] == ["1"] and rows["2"]["title"] == "changed"
    assert rows["4"]["links"] == ["1"]
    assert rows["2"]["version"] == 1
    assert s["deleted"] == 1 and s["deletes_for_absent_docs"] == 1
    assert s["inserted"] == 1 and s["updated"] == 2 and s["rows_after"] == 3


def test_rerun_same_phase_is_noop(spark, tmp_path):
    p0 = write(tmp_path / "p0.jsonl", [doc(1, ["2"]), doc(2, [])])
    run(spark, tmp_path, 0, p0)
    p1 = write(tmp_path / "p1.jsonl", [{"doc_id": "2", "op": "DELETE"},
                                       dict(doc(3, ["1", "2"]), op="UPSERT")])
    s1, rows1 = run(spark, tmp_path, 1, p1)
    s2, rows2 = run(spark, tmp_path, 1, p1)
    assert s2["table_changed"] is False
    assert rows1 == rows2 and s2["rows_after"] == s1["rows_after"]


def test_replaying_older_phase_is_a_clean_noop(spark, tmp_path):
    p0 = write(tmp_path / "p0.jsonl", [doc(1, [])])
    run(spark, tmp_path, 0, p0)
    p1 = write(tmp_path / "p1.jsonl", [dict(doc(2, []), op="UPSERT"),
                                       {"doc_id": "1", "op": "DELETE"}])
    _, rows1 = run(spark, tmp_path, 1, p1)
    s, rows2 = run(spark, tmp_path, 0, p0)     # replay Phase 0 after Phase 1
    assert s["skipped"] is True and s["table_changed"] is False
    assert s["table_matches_expected"] is True
    assert rows2 == rows1 and sorted(rows2) == ["2"]   # doc 1 stays deleted


def test_stale_upsert_ignored(spark, tmp_path):
    p0 = write(tmp_path / "p0.jsonl", [doc(1, [], title="v5", version=5)])
    run(spark, tmp_path, 0, p0)
    p1 = write(tmp_path / "p1.jsonl", [dict(doc(1, [], title="v1", version=1),
                                            op="UPSERT")])
    s, rows = run(spark, tmp_path, 1, p1)
    assert rows["1"]["title"] == "v5" and s["stale_upserts_ignored"] == 1
