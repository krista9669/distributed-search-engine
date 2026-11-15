"""Pure-Python parsing and validation of one JSONL line (no Spark needed).

parse_line returns a flat tuple so Spark can turn it into a DataFrame row:
  (line_no, status, reason, op, doc_id, url, title, text, links, version, raw)
status is "ok", "reject" or "skip" (blank line).
"""
import json

import config


def _reject(line_no, reason, raw):
    return (line_no, "reject", reason, None, None, None, None, None, None, None,
            (raw or "")[:200])


def _is_int(x):
    return isinstance(x, int) and not isinstance(x, bool)


def parse_line(line_no, raw, phase):
    if raw is None or not raw.strip():
        return (line_no, "skip", "blank line", None, None, None, None, None,
                None, None, None)
    try:
        rec = json.loads(raw)
    except ValueError as exc:
        return _reject(line_no, "malformed JSON: %s" % exc, raw)
    if not isinstance(rec, dict):
        return _reject(line_no, "record is not a JSON object", raw)

    # operation (Phase 0 records have none -> UPSERT)
    op = rec.get("op", "UPSERT")
    op = op.upper() if isinstance(op, str) else op
    if op not in config.VALID_OPS:
        return _reject(line_no, "invalid op: %r" % (rec.get("op"),), raw)

    # doc_id
    doc_id = rec.get("doc_id")
    if _is_int(doc_id):
        doc_id = str(doc_id)
    if not isinstance(doc_id, str) or not doc_id.strip():
        return _reject(line_no, "missing or empty doc_id", raw)

    # version
    version = phase
    if config.VERSION_FIELD in rec:
        if not _is_int(rec[config.VERSION_FIELD]):
            return _reject(line_no, "version must be an integer", raw)
        version = rec[config.VERSION_FIELD]

    if op == "DELETE":
        return (line_no, "ok", None, "DELETE", doc_id, None, None, None, None,
                version, None)

    # UPSERT: required fields
    for field in config.REQUIRED_FIELDS:
        if field not in rec:
            return _reject(line_no, "missing required field: %s" % field, raw)
    for field in ("url", "title", "text"):
        val = rec.get(field)
        if field in config.REQUIRED_FIELDS or field in rec:
            if not isinstance(val, str) or not val.strip():
                return _reject(
                    line_no, "field %s must be a non-empty string" % field, raw)

    # links: optional unless listed in REQUIRED_FIELDS
    links = rec.get("links")
    if links is None:
        if "links" in config.REQUIRED_FIELDS:
            return _reject(line_no, "missing required field: links", raw)
        links = []
    if not isinstance(links, list) or not all(isinstance(x, str) for x in links):
        return _reject(line_no, "links must be a list of strings", raw)

    return (line_no, "ok", None, "UPSERT", doc_id, rec["url"], rec["title"],
            rec["text"], list(links), version, None)
