"""
Doc360 → Ava Pipeline Field Mapper

Transforms raw Document360 article rows into the Salesforce-shaped contract
that the indexing pipeline expects.

Changes based on content team (Magda) responses – SAG-769:

  Q1 – Language: Doc360 has a single English locale ("en") with no GB/US split.
       Most articles are global English (served to both GB and US users).
       ShareASale articles carry a "ShareASale" tag and are US-only — their
       "region" is set to "en_US" so that GB users never receive them.
       Non-ShareASale articles keep region="en" (global English).

  Q2 – Visibility: hidden=False AND status_name="Published" are both required.
       Editing a published article temporarily sets it to draft in the admin
       view; the status_name guard prevents indexing in-progress edits.

  Q3 – Related_Questions__c has no Doc360 equivalent. The FAQ section at the
       bottom of articles is already captured inside content_text / Answer__c,
       so the field is set to None and excluded from document assembly for
       Doc360 articles.

  Q4 – Workspace is derived from the article URL path for future multi-workspace
       support (advertisers / developers / general). Publisher workspace content
       is not yet available but the groundwork is in place.

  Q5 – Internal description field (admin-facing, ≤250 chars per Doc360 limits)
       is parsed from settings_json and surfaced in article metadata.
"""

import re
import json
import csv
from typing import Optional


ADVERTISER_RECORD_TYPE_ID = "0122p000000C4WUAA0"

SHAREASALE_TAG = "ShareASale"

CSS_ARTIFACT_PATTERN = re.compile(
    r'\s*p\[data-block-id\].*$', flags=re.DOTALL
)

# Q1: No forced language remapping — Doc360 uses plain "en" for all articles.
# Keep the dict in case future workspaces introduce other locales.
LANGUAGE_MAP: dict[str, str] = {}


def strip_css_artifacts(text: str) -> str:
    """Remove Doc360 editor-injected CSS blocks from content_text."""
    if not text:
        return text
    return CSS_ARTIFACT_PATTERN.sub('', text).strip()


def derive_workspace(url: str) -> str:
    """
    Derive the Doc360 workspace from the article URL path.

    Known patterns (Q4):
      help.awin.com/advertisers/docs/...  → "advertisers"
      help.awin.com/developers/docs/...   → "developers"
      help.awin.com/docs/...              → "general"
    """
    if not url:
        return "unknown"
    if "/advertisers/docs/" in url:
        return "advertisers"
    if "/developers/docs/" in url:
        return "developers"
    return "general"


def parse_description(raw: dict) -> Optional[str]:
    """
    Extract the internal (admin-facing) description from the article row.

    Doc360 stores this in the settings_json column under key "description".
    The field has a hard limit of 250 characters in the Doc360 admin UI (Q5).
    Returns None when the field is absent or empty.
    """
    settings_raw = raw.get("settings_json")
    if settings_raw:
        try:
            settings = json.loads(settings_raw) if isinstance(settings_raw, str) else settings_raw
            desc = (settings.get("description") or "").strip()
            if desc:
                return desc[:250]
        except (json.JSONDecodeError, AttributeError):
            pass

    desc = (raw.get("description") or "").strip()
    return desc[:250] if desc else None


def is_published(raw: dict) -> bool:
    """
    Q2: An article is considered published and safe to index when:
      1. hidden is False (or absent — defaults to False)
      2. status_name == "Published"  (status code 3 in Doc360)

    When a published article is edited, its status reverts to "draft" in the
    admin view even though it was previously live. Checking both fields prevents
    partially-edited content from being indexed.
    """
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

    # Fall back to numeric status code (3 = Published in Doc360)
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
    if isinstance(tags_raw, str) and tags_raw.strip().startswith("["):
        try:
            return json.loads(tags_raw)
        except json.JSONDecodeError:
            return []
    return []


def map_doc360_row(raw: dict) -> dict:
    """
    Transform a single raw Doc360 row dict into the pipeline-compatible
    article dict that indexing.py / preprocess_articles expects.
    """
    # Q1 — Language: use the raw lang_code ("en") without remapping
    lang_code = (raw.get("lang_code") or "en").strip()
    language = LANGUAGE_MAP.get(lang_code, lang_code)

    # Q2 — Visibility: both hidden=False AND status=Published required
    visible = is_published(raw)

    content_text = raw.get("content_text") or ""
    clean_body = strip_css_artifacts(content_text)

    source_url = (raw.get("url") or "").strip()
    article_id = (raw.get("article_id") or "").strip()
    title = (raw.get("title") or "").strip()

    # Q1 — Tags: "ShareASale" tag marks US-only articles
    tags = _parse_tags(raw.get("tags"))
    is_shareasale = SHAREASALE_TAG in tags

    # Q1 — Region: ShareASale articles are US-only → region="en_US".
    #       All other Doc360 articles are global English → region="en"
    #       (served to both GB and US users).
    region = "en_US" if is_shareasale else language

    # Q4 — Workspace derived from URL for multi-workspace support
    workspace = derive_workspace(source_url)

    # Q5 — Internal description (≤250 chars)
    description = parse_description(raw)

    return {
        "Id": article_id,
        "Title": title,
        "Question__c": title,
        "Answer__c": clean_body,
        # Q3: No Related_Questions__c equivalent — FAQ content is already
        #     captured inside Answer__c (content_text) for Doc360 articles.
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
        "Tags": tags,
        "is_shareasale": is_shareasale,
        "workspace": workspace,
        "description": description,
        "from_doc360": True,
    }


def load_and_map_doc360_csv(csv_path: str) -> list[dict]:
    """Load a raw Doc360 CSV and return mapped article dicts."""
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        raw_rows = list(reader)

    return [map_doc360_row(row) for row in raw_rows]


def load_salesforce_csv(csv_path: str) -> list[dict]:
    """
    Load a Salesforce-shaped CSV and return article dicts,
    adding the from_doc360 flag for consistency.
    """
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        raw_rows = list(reader)

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
