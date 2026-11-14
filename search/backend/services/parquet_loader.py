from pathlib import Path

import pandas as pd


COLUMN_ALIASES = {
    "doc_id": ["doc_id", "document_id", "id"],
    "title": ["title", "page_title"],
    "url": ["url", "page_url", "link"],
    "text": ["text", "content", "page_text", "body"],
    "pagerank_score": [
        "pagerank_score",
        "pagerank",
        "page_rank",
        "page_rank_score"
    ],
}


def resolve_columns(dataframe):
    """Resolve actual Parquet columns to the backend's standard fields."""
    available = {
        str(column).lower(): column
        for column in dataframe.columns
    }

    resolved = {}

    for standard_name, aliases in COLUMN_ALIASES.items():
        for alias in aliases:
            if alias.lower() in available:
                resolved[standard_name] = available[alias.lower()]
                break

    required = ["doc_id", "title", "url", "text"]
    missing = [
        column for column in required
        if column not in resolved
    ]

    if missing:
        raise ValueError(
            f"Missing required Parquet columns: {missing}. "
            f"Available columns: {list(dataframe.columns)}"
        )

    return resolved


def load_documents(export_path: str | Path) -> list[dict]:
    """
    Load Parquet documents and normalize them for the search backend.

    export_path can be a single Parquet file or a directory
    containing Parquet part-files.
    """
    path = Path(export_path).expanduser()

    if not path.exists():
        raise FileNotFoundError(
            f"Parquet export path does not exist: {path}"
        )

    if path.is_dir():
        parquet_files = list(path.rglob("*.parquet"))

        if not parquet_files:
            raise FileNotFoundError(
                f"No Parquet files found in: {path}"
            )

    elif path.suffix.lower() == ".parquet":
        parquet_files = [path]

    else:
        raise ValueError(
            "Expected a .parquet file or a directory containing "
            "Parquet files."
        )

    dataframe = pd.read_parquet(path)

    if dataframe.empty:
        return []

    columns = resolve_columns(dataframe)

    documents = []

    for _, row in dataframe.iterrows():
        pagerank_column = columns.get("pagerank_score")

        pagerank = (
            row[pagerank_column]
            if pagerank_column is not None
            else 0.0
        )

        documents.append({
            "doc_id": str(row[columns["doc_id"]]),
            "title": str(row[columns["title"]]),
            "url": str(row[columns["url"]]),
            "text": str(row[columns["text"]]),
            "pagerank_score": float(pagerank)
            if pd.notna(pagerank) else 0.0,
        })

    return documents
