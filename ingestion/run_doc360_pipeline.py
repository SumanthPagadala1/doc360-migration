"""
Doc360 Indexing Pipeline — CLI Entry Point

Usage:
    python ingestion/run_doc360_pipeline.py <path-to-articles.csv> [options]

Examples:
    # Dry run — validate only, do not index
    python ingestion/run_doc360_pipeline.py mapping/doc360-sample-new.csv --dry-run

    # Index to d360-test (default)
    python ingestion/run_doc360_pipeline.py data/doc360_full_export.csv

    # Index to a custom index name
    python ingestion/run_doc360_pipeline.py data/doc360_full_export.csv --index d360-staging

    # Override cleanup mode
    python ingestion/run_doc360_pipeline.py data/export.csv --cleanup full
"""

import argparse
import logging
import os
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MIGRATION_ROOT = os.path.dirname(SCRIPT_DIR)
PIPELINE_ROOT = os.path.dirname(MIGRATION_ROOT)

sys.path.insert(0, SCRIPT_DIR)
sys.path.insert(0, MIGRATION_ROOT)
sys.path.insert(0, PIPELINE_ROOT)

from dotenv import load_dotenv

from doc360_client import Doc360Client
from doc360_filters import run_filter_chain, load_config
from doc360_documents import build_documents
from doc360_validate import run_all_validations, report_distribution

logger = logging.getLogger("doc360_pipeline")


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Doc360 → Azure AI Search indexing pipeline",
    )
    parser.add_argument(
        "source",
        help="Path to Doc360 articles file (.csv or .json)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run mapping, filters and validation only — do not index",
    )
    parser.add_argument(
        "--index",
        default=None,
        help="Override the Azure AI Search index name (default: from config or d360-test)",
    )
    parser.add_argument(
        "--cleanup",
        choices=["incremental", "full", "none"],
        default=None,
        help="Override the LangChain cleanup mode",
    )
    parser.add_argument(
        "--config",
        default=os.path.join(MIGRATION_ROOT, "doc360_filter_config.yaml"),
        help="Path to the Doc360 filter config YAML",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Path to .env file (default: auto-detect from pipeline root)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    setup_logging(args.verbose)

    env_file = args.env_file
    if not env_file:
        for candidate in [
            os.path.join(MIGRATION_ROOT, ".env"),
            os.path.join(PIPELINE_ROOT, ".env"),
        ]:
            if os.path.isfile(candidate):
                env_file = candidate
                break

    if env_file and os.path.isfile(env_file):
        load_dotenv(env_file)
        logger.info("Loaded env from %s", env_file)
    else:
        logger.info("No .env file found — using system environment variables")

    config = load_config(args.config)
    logger.info("Loaded config from %s", args.config)

    if args.index:
        config["index_name"] = args.index
    if args.cleanup:
        config["cleanup_mode"] = args.cleanup

    start = time.monotonic()

    logger.info("")
    logger.info("=" * 55)
    logger.info("  DOC360 INDEXING PIPELINE")
    logger.info("  Source: %s", args.source)
    logger.info("  Index:  %s", config.get("index_name", "d360-test"))
    logger.info("  Mode:   %s", "DRY RUN" if args.dry_run else "LIVE INDEX")
    logger.info("=" * 55)

    # Step 1: Load and map articles
    logger.info("Step 1/5: Loading and mapping articles...")
    client = Doc360Client(args.source)
    articles = client.load_and_map()
    logger.info("  Loaded %d articles", len(articles))

    # Step 2: Filter chain
    logger.info("Step 2/5: Running filter chain...")
    articles = run_filter_chain(articles, config)
    logger.info("  %d articles after filtering", len(articles))

    if not articles:
        logger.warning("No articles survived filtering. Nothing to index.")
        return

    # Step 3: Validation
    logger.info("Step 3/5: Running validation checks...")
    validation_results = run_all_validations(articles)
    report_distribution(articles)

    total_failures = sum(validation_results.values())
    if total_failures > 0:
        logger.error(
            "ABORTING: %d validation failures. Fix issues before indexing.", total_failures
        )
        sys.exit(1)

    # Step 4: Build documents
    logger.info("Step 4/5: Building LangChain Documents...")
    documents = build_documents(articles)
    logger.info("  %d documents ready for indexing", len(documents))

    if args.dry_run:
        elapsed = time.monotonic() - start
        logger.info("")
        logger.info("=" * 55)
        logger.info("  DRY RUN COMPLETE (%.1fs)", elapsed)
        logger.info("  %d documents would be indexed to '%s'",
                     len(documents), config.get("index_name", "d360-test"))
        logger.info("  All validation checks PASSED")
        logger.info("=" * 55)
        return

    # Step 5: Index
    logger.info("Step 5/5: Indexing to Azure AI Search...")
    from doc360_indexer import index_documents
    result = index_documents(documents, config)

    elapsed = time.monotonic() - start

    logger.info("")
    logger.info("=" * 55)
    logger.info("  INDEXING COMPLETE (%.1fs)", elapsed)
    logger.info("  Index:   %s", config.get("index_name", "d360-test"))
    logger.info("  Result:  %s", result)
    logger.info("=" * 55)


if __name__ == "__main__":
    main()
