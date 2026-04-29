"""
Doc360 Data Client

Loads raw Doc360 articles from a local CSV (or JSON) file and applies
the field mapper from SAG-769 to produce pipeline-compatible dicts.

Phase 1 (current): Local file ingestion (CSV / JSON).
Phase 2 (future):  Databricks SQL via
    datalake_reportinglayer.sagitta.vw_document360_knowledge__kav
"""

import csv
import json
import logging
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MAPPING_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "mapping")
sys.path.insert(0, MAPPING_DIR)

from doc360_mapper import map_doc360_row

logger = logging.getLogger(__name__)

FUTURE_DATABRICKS_VIEW = (
    "datalake_reportinglayer.sagitta.vw_document360_knowledge__kav"
)


class Doc360Client:
    """Loads and maps Doc360 articles from a local file."""

    def __init__(self, source_path: str):
        self.source_path = source_path
        if not os.path.isfile(source_path):
            raise FileNotFoundError(f"Source file not found: {source_path}")

    def load_raw_articles(self) -> list[dict]:
        """
        Load raw article rows from the source file.
        Supports .csv and .json extensions.
        """
        ext = os.path.splitext(self.source_path)[1].lower()
        if ext == ".csv":
            return self._load_csv()
        if ext == ".json":
            return self._load_json()
        raise ValueError(f"Unsupported file extension: {ext}. Use .csv or .json")

    def load_and_map(self) -> list[dict]:
        """Load raw articles and run them through the Doc360 field mapper."""
        raw = self.load_raw_articles()
        logger.info("Loaded %d raw articles from %s", len(raw), self.source_path)
        mapped = [map_doc360_row(row) for row in raw]
        logger.info("Mapped %d articles to pipeline contract", len(mapped))
        return mapped

    def _load_csv(self) -> list[dict]:
        with open(self.source_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            return list(reader)

    def _load_json(self) -> list[dict]:
        with open(self.source_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        raise ValueError("JSON file must contain a top-level array of article objects")
