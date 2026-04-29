

import os
import sys
import json
import logging
import yaml

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PIPELINE_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

sys.path.insert(0, SCRIPT_DIR)
sys.path.insert(0, PIPELINE_ROOT)

from doc360_mapper import (
    load_and_map_doc360_csv,
    load_salesforce_csv,
    strip_css_artifacts,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# CSV files live one level up (migration root), not inside mapping/
MIGRATION_ROOT = os.path.dirname(SCRIPT_DIR)
DOC360_CSV = os.path.join(MIGRATION_ROOT, "doc360-sample-new.csv")
SF_CSV = os.path.join(SCRIPT_DIR, "salesforce-sample.csv")
FILTER_CONFIG = os.path.join(PIPELINE_ROOT, "filter_config.yaml")


def load_filter_config():
    with open(FILTER_CONFIG, "r") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Filter chain
# ---------------------------------------------------------------------------

def apply_yaml_rules(articles: list[dict], config: dict) -> list[dict]:
    """Replicate the YAML rule-based filter from preprocess_articles."""
    rules = config.get("rules", [])
    if not config.get("enforce_strict_rules", False) or not rules:
        return articles

    kept = []
    for article in articles:
        for rule in rules:
            if (article.get("RecordTypeId") == rule["record_type_id"]
                    and article.get("Language") in rule["languages"]):
                kept.append(article)
                break
    return kept


def apply_exclusions(articles: list[dict], config: dict) -> list[dict]:
    """Replicate manual exclusion logic."""
    exclusion_ids = set(config.get("manual_exclusions", {}).get("article_ids", []))
    if not exclusion_ids:
        return articles
    return [a for a in articles if a.get("Id") not in exclusion_ids]


def apply_visibility_filter(articles: list[dict]) -> list[dict]:
    """
    Replicate IsVisibleInPkb filter from indexing.py.

    For Doc360 articles this value is pre-computed by is_published()
    (hidden=False AND status_name=Published). For SF articles it comes
    from the raw CSV field.
    """
    return [
        a for a in articles
        if str(a.get("IsVisibleInPkb", "false")).lower() == "true"
        or a.get("IsVisibleInPkb") is True
    ]


def apply_workspace_filter(articles: list[dict]) -> list[dict]:
    """
    Exclude developer workspace articles from the Ava pipeline.

    Decision (SAG-769): developer articles must NOT feed Ava.
    Only "advertisers" and "general" workspace articles are kept.
    Non-Doc360 (SF) articles are unaffected.
    """
    kept = []
    for a in articles:
        if a.get("from_doc360") and a.get("workspace") == "developers":
            logger.info(
                "  [EXCLUDED workspace=developers] %s | %s",
                a.get("Id", "?")[:20], a.get("Title", "?")[:50],
            )
            continue
        kept.append(a)
    return kept


def apply_regional_filter(articles: list[dict]) -> list[dict]:
    """
    Q1 — Updated regional filter.

    Doc360 articles: language is "en" (single English locale, no GB/US split).
      All "en" Doc360 articles pass unconditionally.

    SF articles: retain previous logic —
      en_GB  → always pass
      en_US  → pass only if IsMasterLanguage=True
      other  → drop (non-English content not yet in scope)
    """
    def is_master(article):
        val = article.get("IsMasterLanguage", False)
        return val is True or str(val).lower() == "true"

    kept = []
    for a in articles:
        if a.get("from_doc360"):
            kept.append(a)
            continue

        lang = a.get("Language", "")
        if lang == "en_GB":
            kept.append(a)
        elif lang in ("en_US", "en") and is_master(a):
            kept.append(a)

    return kept


def apply_account_type(articles: list[dict]) -> list[dict]:
    """Replicate account_type derivation."""
    for a in articles:
        a["account_type"] = (
            "advertiser"
            if a.get("RecordTypeId") == "0122p000000C4WUAA0"
            else "publisher"
        )
    return articles


def apply_region(articles: list[dict]) -> list[dict]:
    """
    Set region on each article.

    Doc360 articles: region is pre-computed in the mapper —
      - ShareASale-tagged → "en_US"  (US-only; never served to GB users)
      - All others        → "en"     (global English, both GB and US)

    SF articles: region mirrors Language (existing behaviour).
    """
    for a in articles:
        if a.get("from_doc360"):
            if not a.get("region"):
                a["region"] = a.get("Language")
        else:
            a["region"] = a.get("Language")
    return articles


def backfill_question(articles: list[dict]) -> list[dict]:
    """Replicate Question__c backfill from Title."""
    for a in articles:
        q = a.get("Question__c") or ""
        if not q.strip():
            a["Question__c"] = a.get("Title", "")
    return articles


def compute_source_urls(articles: list[dict], config: dict) -> list[dict]:
    """Replicate source URL computation from preprocess_articles for SF rows."""
    base_url = "https://success.awin.com/s/article"
    rules = config.get("rules", [])
    adv_rule = next((r for r in rules if r["name"] == "Advertiser Content"), None)
    pub_rule = next((r for r in rules if r["name"] == "Publisher Content"), None)

    for a in articles:
        if a.get("from_doc360"):
            continue
        if a.get("source"):
            continue
        url_name = a.get("UrlName", "")
        rt = a.get("RecordTypeId", "")
        if adv_rule and rt == adv_rule["record_type_id"]:
            a["source"] = base_url.replace("https://", "https://advertiser-") + "/" + url_name
        elif pub_rule and rt == pub_rule["record_type_id"]:
            a["source"] = base_url + "/" + url_name
        if a.get("source") and a.get("Language"):
            a["source"] += "?language=" + a["Language"]

    return articles


def build_documents(articles: list[dict]) -> list[dict]:

    sf_content_columns = ["Question__c", "Answer__c", "Related_Questions__c"]
    doc360_content_columns = ["Question__c", "Answer__c"]

    metadata_columns = [
        "Id", "ArticleCreatedDate", "Language", "RecordTypeId",
        "Title", "account_type", "region", "client",
        "workspace", "description",
    ]

    documents = []
    for article in articles:
        content_cols = (
            doc360_content_columns
            if article.get("from_doc360")
            else sf_content_columns
        )

        content_parts = []
        for col in content_cols:
            value = article.get(col)
            if isinstance(value, list):
                content_parts.append(" ".join(map(str, value)))
            elif value:
                content_parts.append(str(value))

        page_content = "\n".join(content_parts)

        metadata = {col: article.get(col) for col in metadata_columns}
        if not isinstance(metadata.get("client"), list):
            metadata["client"] = []
        metadata["source"] = article.get("source")
        metadata["from_doc360"] = article.get("from_doc360", False)
        metadata["is_shareasale"] = article.get("is_shareasale", False)

        documents.append({
            "page_content": page_content,
            "metadata": metadata,
        })
    return documents


# ---------------------------------------------------------------------------
# Validation checks
# ---------------------------------------------------------------------------

def validate_css_stripping(articles: list[dict]) -> int:
    """Check that no Doc360 article body contains CSS artifacts."""
    failures = 0
    for a in articles:
        if not a.get("from_doc360"):
            continue
        body = a.get("Answer__c", "")
        if "p[data-block-id]" in body:
            logger.error("CSS ARTIFACT FOUND in %s: %s", a["Id"], a["Title"])
            failures += 1
    return failures


def validate_source_urls(articles: list[dict]) -> int:
    """Check source URL correctness per source type."""
    failures = 0
    for a in articles:
        source = a.get("source", "")
        if a.get("from_doc360"):
            if not source.startswith("https://help.awin.com/"):
                logger.error("BAD SOURCE URL (Doc360) %s: %s", a["Id"], source)
                failures += 1
        else:
            if source and "awin.com" not in source and "awin.lightning" not in source:
                logger.error("BAD SOURCE URL (SF) %s: %s", a["Id"], source)
                failures += 1
    return failures


def validate_publish_status(articles: list[dict]) -> int:
    """
    Q2: Confirm no Draft or hidden articles passed the visibility filter.
    IsVisibleInPkb must be True for every article that reaches this point.
    """
    failures = 0
    for a in articles:
        visible = a.get("IsVisibleInPkb")
        if visible is not True and str(visible).lower() != "true":
            logger.error(
                "DRAFT/HIDDEN ARTICLE PASSED FILTER %s: %s | status=%s hidden=%s",
                a.get("Id"), a.get("Title"),
                a.get("PublishStatus"), a.get("IsVisibleInPkb"),
            )
            failures += 1
    return failures


def report_shareasale_articles(articles: list[dict]) -> None:

    sas = [a for a in articles if a.get("is_shareasale")]
    non_sas_en = [
        a for a in articles
        if a.get("from_doc360") and not a.get("is_shareasale")
    ]
    if sas:
        logger.info(
            "  ShareASale (US-only, region=en_US): %d articles", len(sas)
        )
        for a in sas:
            logger.info(
                "    [SaS] %s | %s | region=%s",
                a.get("Id", "?")[:20], a.get("Title", "?")[:50], a.get("region"),
            )
    else:
        logger.info("  ShareASale articles:    0")
    if non_sas_en:
        logger.info(
            "  Global English (region=en, GB+US): %d articles", len(non_sas_en)
        )


def report_workspace_distribution(articles: list[dict]) -> None:
    """Q4: Log workspace breakdown for Doc360 articles."""
    doc360 = [a for a in articles if a.get("from_doc360")]
    if not doc360:
        return
    workspaces: dict[str, int] = {}
    for a in doc360:
        ws = a.get("workspace", "unknown")
        workspaces[ws] = workspaces.get(ws, 0) + 1
    logger.info("  Workspace distribution: %s", workspaces)


def report_description_coverage(articles: list[dict]) -> None:
    """Q5: Log how many Doc360 articles have an internal description populated."""
    doc360 = [a for a in articles if a.get("from_doc360")]
    if not doc360:
        return
    with_desc = sum(1 for a in doc360 if a.get("description"))
    logger.info(
        "  Description coverage:   %d / %d Doc360 articles have internal description",
        with_desc, len(doc360),
    )


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

def run_pipeline(label: str, articles: list[dict], config: dict) -> list[dict]:
    """Run the full filter chain and report counts at each stage."""
    logger.info("")
    logger.info("=" * 60)
    logger.info("  %s: %d articles loaded", label, len(articles))
    logger.info("=" * 60)

    articles = apply_exclusions(articles, config)
    logger.info("  After exclusions:       %d", len(articles))

    articles = apply_yaml_rules(articles, config)
    logger.info("  After YAML rules:       %d", len(articles))

    articles = backfill_question(articles)
    articles = compute_source_urls(articles, config)
    articles = apply_account_type(articles)
    articles = apply_region(articles)

    articles = apply_visibility_filter(articles)
    logger.info("  After visibility:       %d", len(articles))

    articles = apply_workspace_filter(articles)
    logger.info("  After workspace filter: %d", len(articles))

    articles = apply_regional_filter(articles)
    logger.info("  After regional filter:  %d", len(articles))

    css_failures = validate_css_stripping(articles)
    url_failures = validate_source_urls(articles)
    pub_failures = validate_publish_status(articles)

    documents = build_documents(articles)
    logger.info("  Documents assembled:    %d", len(documents))

    if css_failures:
        logger.error("  CSS stripping failures: %d", css_failures)
    else:
        logger.info("  CSS stripping:          PASS")

    if url_failures:
        logger.error("  Source URL failures:    %d", url_failures)
    else:
        logger.info("  Source URLs:            PASS")

    if pub_failures:
        logger.error("  Draft/hidden leaks:     %d", pub_failures)
    else:
        logger.info("  Publish status guard:   PASS")

    report_shareasale_articles(articles)
    report_workspace_distribution(articles)
    report_description_coverage(articles)

    logger.info("")
    for doc in documents:
        m = doc["metadata"]
        content_len = len(doc["page_content"])
        src = "Doc360" if m.get("from_doc360") else "SF"
        sas_flag = " [SaS]" if m.get("is_shareasale") else ""
        logger.info(
            "  [%s%s] %s | %s | %s | %s | content=%d chars | ws=%s | source=%s",
            src,
            sas_flag,
            m.get("Id", "?")[:20],
            m.get("Title", "?")[:40],
            m.get("Language"),
            m.get("account_type"),
            content_len,
            m.get("workspace", "n/a"),
            (m.get("source") or "")[:60],
        )

    return documents


def main():
    config = load_filter_config()
    all_pass = True

    # --- Doc360 ---
    doc360_articles = load_and_map_doc360_csv(DOC360_CSV)
    doc360_docs = run_pipeline(
        f"DOC360 ({len(doc360_articles)} articles)", doc360_articles, config
    )

    for doc in doc360_docs:
        if doc["metadata"]["account_type"] != "advertiser":
            logger.error(
                "FAIL: Doc360 article has wrong account_type: %s", doc["metadata"]
            )
            all_pass = False

    # Confirm all Doc360 docs produced valid content
    empty_docs = [d for d in doc360_docs if not d["page_content"].strip()]
    if empty_docs:
        logger.error("FAIL: %d Doc360 documents have empty content", len(empty_docs))
        all_pass = False
    else:
        logger.info("PASS: All %d Doc360 documents have non-empty content", len(doc360_docs))

    # --- Salesforce ---
    sf_articles = load_salesforce_csv(SF_CSV)
    sf_docs = run_pipeline(
        f"SALESFORCE ({len(sf_articles)} articles)", sf_articles, config
    )

    # SF sample: YAML rules keep en_GB/en_US Advertiser and en_US Publisher.
    # Regional filter drops non-en_US publisher articles without IsMasterLanguage.
    if len(sf_docs) == 0 and len(sf_articles) > 0:
        logger.warning("NOTE: 0 SF docs survived — check YAML rules and language values.")

    # --- Summary ---
    logger.info("")
    logger.info("=" * 60)
    logger.info("  FINAL SUMMARY")
    logger.info("=" * 60)
    logger.info(
        "  Doc360 articles in → documents out:  %d → %d",
        len(doc360_articles), len(doc360_docs),
    )
    logger.info(
        "  SF articles in → documents out:      %d → %d",
        len(sf_articles), len(sf_docs),
    )
    logger.info("  Overall: %s", "ALL CHECKS PASSED" if all_pass else "SOME CHECKS FAILED")

    return doc360_docs, sf_docs


def write_combined_csv(doc360_articles: list[dict], sf_articles: list[dict], config: dict):
    """
    Merge all mapped Doc360 rows and original Salesforce rows into a single CSV.
    Adds a 'from_doc360' column: yes / no.
    """
    import csv as csv_mod

    output_path = os.path.join(SCRIPT_DIR, "combined-samples.csv")

    sf_with_flag = []
    for row in sf_articles:
        row_copy = dict(row)
        row_copy["from_doc360"] = "no"
        sf_with_flag.append(row_copy)

    doc360_with_flag = []
    for row in doc360_articles:
        row_copy = dict(row)
        row_copy["from_doc360"] = "yes"
        doc360_with_flag.append(row_copy)

    all_rows = sf_with_flag + doc360_with_flag

    all_columns = []
    seen = set()
    for row in all_rows:
        for k in row.keys():
            if k not in seen:
                all_columns.append(k)
                seen.add(k)

    if "from_doc360" in all_columns:
        all_columns.remove("from_doc360")
    all_columns.append("from_doc360")

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv_mod.DictWriter(f, fieldnames=all_columns, extrasaction="ignore")
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    logger.info("Combined CSV written to: %s", output_path)
    logger.info(
        "  Total rows: %d (SF=%d, Doc360=%d)",
        len(all_rows), len(sf_with_flag), len(doc360_with_flag),
    )
    return output_path


if __name__ == "__main__":
    doc360_docs, sf_docs = main()

    config = load_filter_config()
    doc360_mapped = load_and_map_doc360_csv(DOC360_CSV)
    sf_raw = load_salesforce_csv(SF_CSV)
    write_combined_csv(doc360_mapped, sf_raw, config)
