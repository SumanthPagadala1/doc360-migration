import re
import json
import csv
from typing import Optional


ADVERTISER_RECORD_TYPE_ID = "0122p000000C4WUAA0"
SHAREASALE_TAG = "ShareASale"

CSS_ARTIFACT_PATTERN = re.compile(
    r'\s*p\[data-block-id\].*$', flags=re.DOTALL
)

# Title-based ShareASale pattern (fallback when Tags are null in Databricks view)
SHAREASALE_TITLE_PATTERN = re.compile(r'\b(shareasale|sas)\b', re.IGNORECASE)

LANGUAGE_MAP: dict[str, str] = {}


# ---------------------------------------------------------------------------
# Helpers shared by both format mappers and ingestion pipeline
# ---------------------------------------------------------------------------

def strip_css_artifacts(text: str) -> str:
    """Remove Doc360 editor-injected CSS blocks from article body."""
    if not text:
        return text
    return CSS_ARTIFACT_PATTERN.sub('', text).strip()


def derive_workspace(url: str) -> str:
    """
    Derive the Doc360 workspace from the article URL.

    Databricks view (new): all current articles are /docs/en/ → "advertisers"
    Raw Doc360 API (legacy):
      /advertisers/docs/ → "advertisers"
      /developers/docs/ → "developers"
      /docs/            → "general" (legacy; now mapped to "advertisers")
    """
    if not url:
        return "unknown"
    if "/developers/docs/" in url:
        return "developers"
    if "/advertisers/docs/" in url:
        return "advertisers"
    # All other help.awin.com URLs (including /docs/en/) are advertiser content
    if "help.awin.com" in url:
        return "advertisers"
    return "unknown"


def parse_description(raw: dict) -> Optional[str]:
    """
    Extract the internal description from the article row.

    Doc360 raw API: stored in settings_json["description"].
    Databricks view: Summary field or plain "description" key.
    Hard limit: 250 characters.
    """
    # Try settings_json first (raw Doc360 API format)
    settings_raw = raw.get("settings_json")
    if settings_raw:
        try:
            settings = json.loads(settings_raw) if isinstance(settings_raw, str) else settings_raw
            desc = (settings.get("description") or "").strip()
            if desc:
                return desc[:250]
        except (json.JSONDecodeError, AttributeError):
            pass

    # Try Summary field (Databricks view format)
    for field in ("Summary", "description"):
        val = (raw.get(field) or "").strip()
        if val and val.lower() not in ("null", "-", "none"):
            return val[:250]

    return None


def is_published(raw: dict) -> bool:
    """
    Determine if an article is published and safe to index.

    Supports both:
      - Raw Doc360 API: hidden + status_name / status code
      - Databricks view: PublishStatus + isPublished__c (IsVisibleInPkb is often null)
    """
    # --- Databricks view format ---
    publish_status = (raw.get("PublishStatus") or "").strip().lower()
    if publish_status == "published":
        return True
    if publish_status and publish_status != "":
        # Has PublishStatus but not "Published" → not ready
        return False

    # --- Raw Doc360 API format ---
    hidden_raw = raw.get("hidden")
    if hidden_raw is None or str(hidden_raw).strip().lower() in ("", "null", "none"):
        hidden = False
    else:
        hidden = str(hidden_raw).strip().lower() == "true"
    if hidden:
        return False

    status_name = (raw.get("status_name") or "").strip().lower()
    if status_name:
        return status_name == "published"

    status_code = raw.get("status")
    if isinstance(status_code, str):
        try:
            status_code = int(status_code)
        except ValueError:
            status_code = None
    if status_code is not None:
        return status_code == 3

    return True


def _parse_tags(tags_raw) -> list:
    """Normalise the tags field to a plain Python list."""
    if isinstance(tags_raw, list):
        return tags_raw
    if not tags_raw or str(tags_raw).strip().lower() in ("null", "none", ""):
        return []
    s = str(tags_raw).strip()
    if s.startswith("["):
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            return []
    return []


def detect_shareasale(raw: dict) -> bool:
    """
    Detect ShareASale articles.
    Primary:  Tags field contains "ShareASale"
    Fallback: Title contains "ShareASale" or "SaS" (used when Tags are null
              in the Databricks view export).
    """
    tags = _parse_tags(raw.get("tags") or raw.get("Tags"))
    if any(SHAREASALE_TAG.lower() in str(t).lower() for t in tags):
        return True

    title = raw.get("title") or raw.get("Title") or ""
    if SHAREASALE_TITLE_PATTERN.search(title):
        return True

    return False


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------

def _is_databricks_view_format(raw: dict) -> bool:
    """Return True if the row uses Databricks view column names."""
    view_markers = {"Id", "Answer__c", "Title", "PublishStatus", "Language"}
    return view_markers.issubset(set(raw.keys()))


# ---------------------------------------------------------------------------
# Databricks view format mapper  (new CSV — production format)
# ---------------------------------------------------------------------------

def _map_view_row(raw: dict) -> dict:
    """Map a Databricks vw_document360_knowledge__kav row to pipeline contract."""
    language = (raw.get("Language") or "en").strip()
    visible = is_published(raw)

    clean_body = strip_css_artifacts(raw.get("Answer__c") or "")
    source_url = (raw.get("article_url__c") or raw.get("Article_Link__c") or "").strip()
    article_id = (raw.get("Id") or "").strip()
    title = (raw.get("Title") or "").strip()

    is_shareasale = detect_shareasale(raw)
    region = "en_US" if is_shareasale else language

    workspace = derive_workspace(source_url)
    description = parse_description(raw)

    return {
        "Id": article_id,
        "Title": title,
        "Question__c": raw.get("Question__c") or title,
        "Answer__c": clean_body,
        "Related_Questions__c": None,
        "Language": language,
        "IsMasterLanguage": True,
        "IsVisibleInPkb": visible,
        "RecordTypeId": ADVERTISER_RECORD_TYPE_ID,
        "PublishStatus": raw.get("PublishStatus", "Published"),
        "UrlName": (raw.get("UrlName") or "").strip(),
        "ArticleCreatedDate": raw.get("ArticleCreatedDate"),
        "LastModifiedDate": raw.get("LastModifiedDate") or raw.get("LastPublishedDate"),
        "source": source_url,
        "account_type": "advertiser",
        "region": region,
        "client": [],
        "Tags": _parse_tags(raw.get("Tags")),
        "is_shareasale": is_shareasale,
        "workspace": workspace,
        "description": description,
        "from_doc360": True,
    }


# ---------------------------------------------------------------------------
# Raw Doc360 API format mapper  (legacy 15-sample / future API ingestion)
# ---------------------------------------------------------------------------

def map_doc360_row(raw: dict) -> dict:
    """
    Transform a single raw Doc360 row to the pipeline contract.
    Auto-detects format: Databricks view vs raw Doc360 API.
    """
    if _is_databricks_view_format(raw):
        return _map_view_row(raw)

    # --- Raw Doc360 API format ---
    lang_code = (raw.get("lang_code") or "en").strip()
    language = LANGUAGE_MAP.get(lang_code, lang_code)

    visible = is_published(raw)
    clean_body = strip_css_artifacts(raw.get("content_text") or "")
    source_url = (raw.get("url") or "").strip()
    article_id = (raw.get("article_id") or "").strip()
    title = (raw.get("title") or "").strip()

    is_shareasale = detect_shareasale(raw)
    region = "en_US" if is_shareasale else language

    workspace = derive_workspace(source_url)
    description = parse_description(raw)

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
        "PublishStatus": raw.get("status_name", "Published"),
        "UrlName": (raw.get("slug") or "").strip(),
        "ArticleCreatedDate": raw.get("created_at"),
        "LastModifiedDate": raw.get("modified_at"),
        "source": source_url,
        "account_type": "advertiser",
        "region": region,
        "client": [],
        "Tags": _parse_tags(raw.get("tags")),
        "is_shareasale": is_shareasale,
        "workspace": workspace,
        "description": description,
        "from_doc360": True,
    }


# ---------------------------------------------------------------------------
# CSV loaders
# ---------------------------------------------------------------------------

def _filter_empty_rows(rows: list[dict]) -> list[dict]:
    """Remove blank Excel rows (where all key fields are empty/null)."""
    key_fields = {"Id", "article_id", "Title", "title", "Answer__c", "content_text"}
    return [
        r for r in rows
        if any(
            r.get(f, "").strip() and r.get(f, "").strip().lower() not in ("null", "-")
            for f in key_fields
            if f in r
        )
    ]


def load_and_map_doc360_csv(csv_path: str) -> list[dict]:
    """Load a Doc360 CSV (either format) and return mapped article dicts."""
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        raw_rows = list(csv.DictReader(f))
    raw_rows = _filter_empty_rows(raw_rows)
    return [map_doc360_row(row) for row in raw_rows]


def load_salesforce_csv(csv_path: str) -> list[dict]:
    """Load a Salesforce-shaped CSV and return article dicts."""
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        raw_rows = list(csv.DictReader(f))

    articles = []
    for row in raw_rows:
        row["from_doc360"] = False

        for bool_field in ("IsMasterLanguage", "IsVisibleInPkb", "IsDeleted"):
            val = row.get(bool_field)
            if isinstance(val, str):
                row[bool_field] = val.strip().upper() == "TRUE"

        if not isinstance(row.get("client"), list):
            try:
                row["client"] = json.loads(row.get("client", "[]"))
            except (json.JSONDecodeError, TypeError):
                row["client"] = []

        if not isinstance(row.get("Tags"), list):
            try:
                row["Tags"] = json.loads(row.get("Tags") or "[]")
            except (json.JSONDecodeError, TypeError):
                row["Tags"] = []

        articles.append(row)

    return articles
