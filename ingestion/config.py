"""Node 1 ingestion rules. Change rules HERE only.

To make `links` mandatory (reject records that lack it), add "links" to
REQUIRED_FIELDS. Nothing else needs to change.
"""

# Fields every UPSERT record must have as non-empty values.
REQUIRED_FIELDS = ("doc_id", "url", "title", "text")

# `links` is optional unless listed above; when absent it becomes [].
VALID_OPS = ("UPSERT", "DELETE")

# If a record carries its own integer "version", it is used. Otherwise the
# version is the phase number (Phase 0 -> 0, Phase 1 -> 1).
VERSION_FIELD = "version"
