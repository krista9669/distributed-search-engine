import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))

import config  # noqa: E402
import core  # noqa: E402

GOOD = {"doc_id": "1", "url": "http://x/1", "title": "T", "text": "body",
        "links": ["2", "3"]}


def p(rec, phase=0, line=1):
    raw = rec if isinstance(rec, str) else json.dumps(rec)
    return core.parse_line(line, raw, phase)


def test_valid_record():
    t = p(GOOD)
    assert t[1] == "ok" and t[3] == "UPSERT" and t[4] == "1"
    assert t[8] == ["2", "3"] and t[9] == 0


def test_truncated_json_rejected():
    t = p('{"doc_id": "malformed-1", "url":', line=1021)
    assert t[1] == "reject" and t[0] == 1021 and "malformed JSON" in t[2]


def test_blank_line_skipped():
    assert p("   ")[1] == "skip"


def test_non_object_rejected():
    assert p("[1,2]")[1] == "reject"


def test_missing_links_defaults_to_empty_list():
    rec = dict(GOOD)
    del rec["links"]
    t = p(rec)
    assert t[1] == "ok" and t[8] == []


def test_links_required_when_configured(monkeypatch):
    monkeypatch.setattr(config, "REQUIRED_FIELDS",
                        config.REQUIRED_FIELDS + ("links",))
    rec = dict(GOOD)
    del rec["links"]
    t = p(rec)
    assert t[1] == "reject" and "links" in t[2]


def test_missing_required_field_rejected():
    for field in ("doc_id", "url", "title", "text"):
        rec = dict(GOOD)
        del rec[field]
        assert p(rec)[1] == "reject", field


def test_empty_title_rejected():
    assert p(dict(GOOD, title="  "))[1] == "reject"


def test_links_wrong_type_rejected():
    assert p(dict(GOOD, links="2,3"))[1] == "reject"
    assert p(dict(GOOD, links=[1, 2]))[1] == "reject"


def test_delete_only_needs_doc_id():
    t = p({"doc_id": "9", "op": "DELETE"}, phase=1)
    assert t[1] == "ok" and t[3] == "DELETE" and t[4] == "9" and t[9] == 1


def test_delete_without_doc_id_rejected():
    assert p({"op": "DELETE"})[1] == "reject"


def test_invalid_op_rejected():
    assert p(dict(GOOD, op="MERGE"))[1] == "reject"


def test_version_defaults_to_phase_and_own_version_wins():
    assert p(GOOD, phase=1)[9] == 1
    assert p(dict(GOOD, version=7), phase=1)[9] == 7
    assert p(dict(GOOD, version="x"))[1] == "reject"


def test_numeric_doc_id_normalised():
    assert p(dict(GOOD, doc_id=42))[4] == "42"
