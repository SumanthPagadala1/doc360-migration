"""
Doc360 Filter Chain

Production-grade filter pipeline for Doc360 articles.
Each filter is a pure function: list[dict] -> list[dict].

Filter order:
  1. Visibility  (hidden=False AND status=Published)
  2. Workspace   (drop "developers")
  3. Question__c backfill from Title
  4. Region / ShareASale tagging (pre-computed by mapper)
  5. Account type (always advertiser for current phase)
"""

import logging
from typing import Optional

import yaml

logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def apply_visibility_filter(articles: list[dict]) -> list[dict]:
    """
    Keep only articles where IsVisibleInPkb is True.

    For Doc360 articles this is pre-computed by is_published() in the mapper:
    hidden=False AND status_name="Published" (status code 3).
    """
    before = len(articles)
    kept = [
        a for a in articles
        if a.get("IsVisibleInPkb") is True
        or str(a.get("IsVisibleInPkb", "false")).lower() == "true"
    ]
    logger.info("Visibility filter: %d -> %d", before, len(kept))
    return kept


def apply_workspace_filter(
    articles: list[dict],
    excluded_workspaces: Optional[list[str]] = None,
) -> list[dict]:
    """
    Drop articles from excluded workspaces.
    Default exclusion: ["developers"] (SAG-769 decision).
    """
    if excluded_workspaces is None:
        excluded_workspaces = ["developers"]

    excluded = set(excluded_workspaces)
    before = len(articles)
    kept = []
    for a in articles:
        ws = a.get("workspace", "")
        if ws in excluded:
            logger.debug(
                "Excluded [workspace=%s] %s | %s",
                ws, a.get("Id", "?")[:20], a.get("Title", "?")[:50],
            )
            continue
        kept.append(a)
    logger.info("Workspace filter: %d -> %d (excluded: %s)", before, len(kept), excluded)
    return kept


def backfill_question(articles: list[dict]) -> list[dict]:
    """Copy Title into Question__c when Question__c is empty."""
    count = 0
    for a in articles:
        q = a.get("Question__c") or ""
        if not q.strip():
            a["Question__c"] = a.get("Title", "")
            count += 1
    if count:
        logger.info("Backfilled Question__c from Title for %d articles", count)
    return articles


def run_filter_chain(
    articles: list[dict],
    config: Optional[dict] = None,
) -> list[dict]:
    """
    Execute the full Doc360 filter chain in order.
    Returns the surviving articles ready for document assembly.
    """
    excluded_ws = None
    if config:
        excluded_ws = config.get("workspace", {}).get("excluded")

    logger.info("Starting filter chain with %d articles", len(articles))

    articles = apply_visibility_filter(articles)
    articles = apply_workspace_filter(articles, excluded_workspaces=excluded_ws)
    articles = backfill_question(articles)

    logger.info("Filter chain complete: %d articles survived", len(articles))
    return articles
