"""
Doc360 Data Client

Loads Doc360 articles and normalises them to the pipeline contract.

Supports two CSV formats:
  1. Databricks view format (production):
     Columns like Id, Answer__c, article_url__c, Title, Language,
     PublishStatus — from vw_document360_knowledge__kav.
  2. Raw Doc360 format (legacy 15-sample):
     Columns like article_id, content_text, url, title, tags.

Phase 1 (current): Local file ingestion (CSV / JSON).
Phase 2 (future):  Databricks SQL via
    datalake_reportinglayer.sagitta.vw_document360_knowledge__kav
"""

import csv
import json
import logging
import os
import re
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MAPPING_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "mapping")
sys.path.insert(0, MAPPING_DIR)

from doc360_mapper import map_doc360_row, strip_css_artifacts

logger = logging.getLogger(__name__)

FUTURE_DATABRICKS_VIEW = (
    "datalake_reportinglayer.sagitta.vw_document360_knowledge__kav"
)

ADVERTISER_RECORD_TYPE_ID = "0122p000000C4WUAA0"

SHAREASALE_TITLE_PATTERNS = re.compile(
    r"\b(shareasale|sas)\b", re.IGNORECASE
)


def _is_databricks_view_format(columns: list[str]) -> bool:
    """Detect if CSV uses the Databricks view column names."""
    view_markers = {"Id", "Answer__c", "Title", "PublishStatus", "Language"}
    return view_markers.issubset(set(columns))


def _detect_shareasale(row: dict) -> bool:
    """
    Detect ShareASale articles. Primary: Tags field. Fallback: title match.
    Tags will be populated in future exports; title is the interim signal.
    """
    tags_raw = row.get("Tags", "")
    if tags_raw and tags_raw.lower() not in ("null", "", "none"):
        if isinstance(tags_raw, str):
            if "shareasale" in tags_raw.lower():
                return True
        elif isinstance(tags_raw, list):
            if any("shareasale" in t.lower() for t in tags_raw):
                return True

    title = row.get("Title", "")
    if SHAREASALE_TITLE_PATTERNS.search(title):
        return True

    return False


def _is_published_view_format(row: dict) -> bool:
    """
    Determine visibility for Databricks view format rows.
    Uses PublishStatus and isPublished__c (IsVisibleInPkb is often null).
    """
    publish_status = (row.get("PublishStatus") or "").strip().lower()
    if publish_status == "published":
        return True

    is_pub = (row.get("isPublished__c") or "").strip().upper()
    if is_pub == "TRUE":
        return True

    return False


def _map_view_row(raw: dict) -> dict:
    """
    Map a Databricks view format row to the pipeline contract.
    The view already has most fields named correctly — we normalise
    and add computed fields.
    """
    body = raw.get("Answer__c", "")
    clean_body = strip_css_artifacts(body)

    source_url = (raw.get("article_url__c") or "").strip()
    title = (raw.get("Title") or "").strip()
    article_id = (raw.get("Id") or "").strip()
    language = (raw.get("Language") or "en").strip()

    is_shareasale = _detect_shareasale(raw)
    region = "en_US" if is_shareasale else language

    visible = _is_published_view_format(raw)

    return {
        "Id": article_id,
        "Title": title,
        "Question__c": title,
        "Answer__c": clean_body,
        "Related_Questions__c": None,
        "Language": language,
        "IsMasterLanguage": True,
        "IsVisibleInPkb": visible,
        "RecordTypeId": ADVERTISER_RECORD_TYPE_ID,
        "PublishStatus": raw.get("PublishStatus", "Published"),
        "UrlName": (raw.get("UrlName") or "").strip(),
        "ArticleCreatedDate": raw.get("ArticleCreatedDate"),
        "LastModifiedDate": raw.get("LastModifiedDate"),
        "source": source_url,
        "account_type": "advertiser",
        "region": region,
        "client": [],
        "Tags": raw.get("Tags", ""),
        "is_shareasale": is_shareasale,
        "workspace": "advertisers",
        "description": None,
        "from_doc360": True,
    }


class Doc360Client:
    """Loads and maps Doc360 articles from a local file."""

    def __init__(self, source_path: str):
        self.source_path = source_path
        if not os.path.isfile(source_path):
            raise FileNotFoundError(f"Source file not found: {source_path}")

    def load_raw_articles(self) -> list[dict]:
        """
        Load raw article rows from the source file.
        Supports .csv and .json extensions. Filters out empty rows.
        """
        ext = os.path.splitext(self.source_path)[1].lower()
        if ext == ".csv":
            rows = self._load_csv()
        elif ext == ".json":
            rows = self._load_json()
        else:
            raise ValueError(f"Unsupported file extension: {ext}. Use .csv or .json")

        before = len(rows)
        rows = [r for r in rows if any(
            v and str(v).strip() and str(v).strip().lower() != "null"
            for k, v in r.items()
            if k in ("Id", "article_id", "Title", "title", "Answer__c", "content_text")
        )]
        if len(rows) < before:
            logger.info("Filtered out %d empty rows", before - len(rows))
        return rows

    def load_and_map(self) -> list[dict]:
        """Load raw articles and map them to pipeline contract."""
        raw = self.load_raw_articles()
        logger.info("Loaded %d raw articles from %s", len(raw), self.source_path)

        if not raw:
            return []

        columns = list(raw[0].keys())
        if _is_databricks_view_format(columns):
            logger.info("Detected Databricks view format — using view mapper")
            mapped = [_map_view_row(row) for row in raw]
        else:
            logger.info("Detected raw Doc360 format — using legacy mapper")
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
