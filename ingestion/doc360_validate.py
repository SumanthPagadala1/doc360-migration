"""
Doc360 Pre-Index Validation

Runs safety checks on the filtered article set before indexing.
All checks return an integer failure count (0 = PASS).

Used by the CLI in --dry-run mode and as a gate before live indexing.
"""

import logging

logger = logging.getLogger(__name__)


def validate_css_stripping(articles: list[dict]) -> int:
    """Fail if any Doc360 article body still contains CSS artifacts."""
    failures = 0
    for a in articles:
        body = a.get("Answer__c", "")
        if "p[data-block-id]" in body:
            logger.error(
                "CSS ARTIFACT in %s: %s", a.get("Id", "?")[:20], a.get("Title", "?")[:50]
            )
            failures += 1
    return failures


def validate_source_urls(articles: list[dict]) -> int:
    """Fail if any Doc360 source URL does not point to help.awin.com."""
    failures = 0
    for a in articles:
        source = a.get("source", "")
        if not source.startswith("https://help.awin.com/"):
            logger.error(
                "BAD SOURCE URL %s: %s", a.get("Id", "?")[:20], source
            )
            failures += 1
    return failures


def validate_publish_status(articles: list[dict]) -> int:
    """Fail if any article with IsVisibleInPkb != True made it through."""
    failures = 0
    for a in articles:
        visible = a.get("IsVisibleInPkb")
        if visible is not True and str(visible).lower() != "true":
            logger.error(
                "DRAFT/HIDDEN LEAK %s: %s | visible=%s status=%s",
                a.get("Id", "?")[:20], a.get("Title", "?")[:50],
                visible, a.get("PublishStatus"),
            )
            failures += 1
    return failures


def validate_empty_content(articles: list[dict]) -> int:
    """Fail if any article has an empty Answer__c after processing."""
    failures = 0
    for a in articles:
        body = (a.get("Answer__c") or "").strip()
        if not body:
            logger.error(
                "EMPTY CONTENT %s: %s", a.get("Id", "?")[:20], a.get("Title", "?")[:50]
            )
            failures += 1
    return failures


def validate_shareasale_region(articles: list[dict]) -> int:
    """Fail if a ShareASale article does not have region=en_US."""
    failures = 0
    for a in articles:
        if a.get("is_shareasale") and a.get("region") != "en_US":
            logger.error(
                "SHAREASALE REGION MISMATCH %s: region=%s (expected en_US)",
                a.get("Id", "?")[:20], a.get("region"),
            )
            failures += 1
    return failures


def validate_no_developers(articles: list[dict]) -> int:
    """Fail if any developers workspace article is still present."""
    failures = 0
    for a in articles:
        if a.get("workspace") == "developers":
            logger.error(
                "DEVELOPER ARTICLE LEAK %s: %s",
                a.get("Id", "?")[:20], a.get("Title", "?")[:50],
            )
            failures += 1
    return failures


def run_all_validations(articles: list[dict]) -> dict[str, int]:
    """
    Run every validation check. Returns a dict of check_name -> failure_count.
    Total failures > 0 means the pipeline should NOT proceed to indexing.
    """
    results = {
        "css_stripping": validate_css_stripping(articles),
        "source_urls": validate_source_urls(articles),
        "publish_status": validate_publish_status(articles),
        "empty_content": validate_empty_content(articles),
        "shareasale_region": validate_shareasale_region(articles),
        "no_developers": validate_no_developers(articles),
    }

    total_failures = sum(results.values())

    for name, count in results.items():
        status = "PASS" if count == 0 else f"FAIL ({count})"
        logger.info("  %-25s %s", name, status)

    if total_failures == 0:
        logger.info("All validation checks PASSED")
    else:
        logger.error("VALIDATION FAILED: %d total failures", total_failures)

    return results


def report_distribution(articles: list[dict]) -> None:
    """Log summary statistics about the article set."""
    total = len(articles)
    sas = sum(1 for a in articles if a.get("is_shareasale"))
    global_en = sum(
        1 for a in articles
        if not a.get("is_shareasale") and a.get("region") == "en"
    )

    workspaces: dict[str, int] = {}
    for a in articles:
        ws = a.get("workspace", "unknown")
        workspaces[ws] = workspaces.get(ws, 0) + 1

    with_desc = sum(1 for a in articles if a.get("description"))

    logger.info("")
    logger.info("=" * 55)
    logger.info("  ARTICLE DISTRIBUTION")
    logger.info("=" * 55)
    logger.info("  Total articles:            %d", total)
    logger.info("  Global English (GB+US):    %d", global_en)
    logger.info("  ShareASale (US-only):      %d", sas)
    logger.info("  Workspaces:                %s", workspaces)
    logger.info("  Description populated:     %d / %d", with_desc, total)
    logger.info("=" * 55)
