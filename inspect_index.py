"""
Inspect Azure AI Search indices for dev / staging / prod.

Usage:
    # List all indices in dev
    python inspect_index.py dev

    # Inspect a specific index
    python inspect_index.py dev d360-test

    # Check kb-articles update status across all stages
    python inspect_index.py all

Credential resolution (in order of priority):
  dev     -> AZURE_AI_SEARCH_ENDPOINT_DEV  / AZURE_AI_SEARCH_API_KEY_DEV
             AZURE_AI_SEARCH_ENDPOINT      / AZURE_AI_SEARCH_API_KEY  (legacy fallback)
  staging -> AZURE_AI_SEARCH_ENDPOINT_STAGING / AZURE_AI_SEARCH_API_KEY_STAGING
  prod    -> AZURE_AI_SEARCH_ENDPOINT_PROD    / AZURE_AI_SEARCH_API_KEY_PROD
"""

import os
import argparse
import logging
import json
from datetime import datetime, timezone
from typing import Optional, Tuple
from dotenv import load_dotenv
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient


def setup_logging():
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    return logging.getLogger(__name__)


def get_credentials_for_stage(stage: str) -> Tuple[str, str]:
    """
    Returns (endpoint, key) for the given stage.
    Supports both _DEV-suffixed naming (doc360 pipeline) and
    plain naming (original pipeline) for dev.
    """
    stage_map = {
        "dev": [
            ("AZURE_AI_SEARCH_ENDPOINT_DEV", "AZURE_AI_SEARCH_API_KEY_DEV"),
            ("AZURE_AI_SEARCH_ENDPOINT", "AZURE_AI_SEARCH_API_KEY"),
        ],
        "staging": [
            ("AZURE_AI_SEARCH_ENDPOINT_STAGING", "AZURE_AI_SEARCH_API_KEY_STAGING"),
        ],
        "prod": [
            ("AZURE_AI_SEARCH_ENDPOINT_PROD", "AZURE_AI_SEARCH_API_KEY_PROD"),
        ],
    }
    if stage not in stage_map:
        raise ValueError("Stage must be one of: dev, staging, prod")

    for endpoint_env, key_env in stage_map[stage]:
        endpoint = os.getenv(endpoint_env)
        key = os.getenv(key_env)
        if endpoint and key:
            return endpoint, key

    tried = ", ".join(f"{e}/{k}" for e, k in stage_map[stage])
    raise ValueError(
        f"No credentials found for stage '{stage}'. "
        f"Tried env var pairs: {tried}. Check your .env file."
    )


def _parse_date(date_str) -> Optional[datetime]:
    if not date_str:
        return None
    if isinstance(date_str, datetime):
        return date_str
    if isinstance(date_str, str):
        date_str_clean = date_str.replace("Z", "+00:00") if date_str.endswith("Z") else date_str
        try:
            return datetime.fromisoformat(date_str_clean)
        except ValueError:
            for fmt in ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"]:
                try:
                    return datetime.strptime(date_str.split(".")[0].split("+")[0].split("Z")[0], fmt)
                except ValueError:
                    continue
    return None


def get_index_last_updated(
    search_client: SearchClient,
    index_client: SearchIndexClient,
    index_name: str,
) -> Optional[Tuple[datetime, str]]:
    try:
        doc_count = search_client.get_document_count()
        if doc_count == 0:
            return None

        sample_size = min(1000, doc_count)
        results = search_client.search(search_text="*", select="metadata", top=sample_size)

        latest_date = None
        date_field_used = None

        for doc in results:
            if "metadata" in doc:
                try:
                    metadata = json.loads(doc["metadata"]) if isinstance(doc["metadata"], str) else doc["metadata"]
                    for field in ["LastModifiedDate", "SystemModstamp", "LastPublishedDate", "ArticleCreatedDate"]:
                        if metadata.get(field):
                            dt = _parse_date(metadata[field])
                            if dt and (latest_date is None or dt > latest_date):
                                latest_date = dt
                                date_field_used = field
                except (json.JSONDecodeError, TypeError, ValueError, AttributeError):
                    pass

        if latest_date:
            return (latest_date, date_field_used or "date field from metadata")
        return None
    except Exception:
        return None


def inspect_index(stage: str, index_name: str):
    logger = setup_logging()
    load_dotenv()

    try:
        endpoint, key = get_credentials_for_stage(stage)
        logger.info("Connecting to %s at %s ...", stage.upper(), endpoint)

        search_client = SearchClient(endpoint=endpoint, index_name=index_name, credential=AzureKeyCredential(key))
        index_client = SearchIndexClient(endpoint=endpoint, credential=AzureKeyCredential(key))

        total_docs = search_client.get_document_count()
        logger.info("--- Index '%s' Summary ---", index_name)
        logger.info("Total document count: %d", total_docs)

        last_updated_info = get_index_last_updated(search_client, index_client, index_name)
        if last_updated_info:
            dt, source = last_updated_info
            logger.info("Last updated: %s (%s)", dt.strftime("%Y-%m-%d %H:%M:%S"), source)
        else:
            logger.info("Last updated: Unable to determine")

        if total_docs == 0:
            logger.info("Index is empty. No further analysis to perform.")
            return

        logger.info("\n--- Breakdown by Account Type ---")
        facet_results = search_client.search(search_text="*", facets=["account_type"])
        account_type_facets = facet_results.get_facets().get("account_type")

        if not account_type_facets:
            logger.warning("No 'account_type' facet. Falling back to 'region' facet.")
            facet_results2 = search_client.search(search_text="*", facets=["region"])
            region_facets = facet_results2.get_facets().get("region", [])
            for item in region_facets:
                logger.info("  Region '%s': %d documents", item["value"], item["count"])
            return

        for account_facet in account_type_facets:
            account_type = account_facet["value"]
            count = account_facet["count"]
            logger.info("  Account Type '%s': %d documents", account_type, count)

            if account_type == "advertiser":
                region_results = search_client.search(
                    search_text="*",
                    filter=f"account_type eq '{account_type}'",
                    facets=["region"],
                )
                region_facets = region_results.get_facets().get("region", [])
                for rf in region_facets:
                    logger.info("    Region '%s': %d documents", rf["value"], rf["count"])

        # Doc360-specific facets
        is_doc360_results = search_client.search(search_text="*", facets=["from_doc360"])
        d360_facets = is_doc360_results.get_facets().get("from_doc360")
        if d360_facets:
            logger.info("\n--- Doc360 vs Salesforce ---")
            for item in d360_facets:
                logger.info("  from_doc360='%s': %d documents", item["value"], item["count"])

        sas_results = search_client.search(search_text="*", facets=["is_shareasale"])
        sas_facets = sas_results.get_facets().get("is_shareasale")
        if sas_facets:
            logger.info("\n--- ShareASale breakdown ---")
            for item in sas_facets:
                logger.info("  is_shareasale='%s': %d documents", item["value"], item["count"])

    except Exception as e:
        logger.error("An error occurred: %s", e, exc_info=True)


def list_indices(stage: str):
    logger = setup_logging()
    load_dotenv()

    try:
        endpoint, key = get_credentials_for_stage(stage)
        logger.info("Listing all indices in %s ...", stage.upper())

        index_client = SearchIndexClient(endpoint=endpoint, credential=AzureKeyCredential(key))

        print("-" * 50)
        for idx in index_client.list_indexes():
            try:
                search_client = SearchClient(endpoint=endpoint, index_name=idx.name, credential=AzureKeyCredential(key))
                doc_count = search_client.get_document_count()
                print(f"Index: {idx.name}\n  Document Count: {doc_count:,}")

                last_updated_info = get_index_last_updated(search_client, index_client, idx.name)
                if last_updated_info:
                    dt, source = last_updated_info
                    print(f"  Last Updated: {dt.strftime('%Y-%m-%d %H:%M:%S')} ({source})")
                else:
                    print("  Last Updated: Unable to determine")

                if doc_count > 0:
                    results = search_client.search("*", select="metadata", top=1)
                    print("  Metadata Fields:")
                    for doc in results:
                        if "metadata" in doc:
                            try:
                                metadata = json.loads(doc["metadata"])
                                for meta_key in metadata.keys():
                                    print(f"    - {meta_key}")
                            except (json.JSONDecodeError, TypeError):
                                print(f"    - Could not parse: {doc['metadata']}")
                        break
                print()
            except Exception as e:
                print(f"Index: {idx.name}\n  Could not retrieve details: {e}\n")
        print("-" * 50)

    except Exception as e:
        logger.error("An error occurred while listing indices: %s", e, exc_info=True)


def check_kb_articles_indices():
    load_dotenv()
    index_names = {
        "dev": "kb-articles-dev",
        "staging": "kb-articles-staging",
        "prod": "kb-articles-prod",
    }

    print("=" * 70)
    print("KB Articles Index Update Status")
    print("=" * 70)

    for stage, index_name in index_names.items():
        try:
            endpoint, key = get_credentials_for_stage(stage)
            search_client = SearchClient(endpoint=endpoint, index_name=index_name, credential=AzureKeyCredential(key))
            index_client = SearchIndexClient(endpoint=endpoint, credential=AzureKeyCredential(key))

            doc_count = search_client.get_document_count()
            last_updated_info = get_index_last_updated(search_client, index_client, index_name)

            print(f"\n[{stage.upper()}] {index_name}")
            print(f"  Document Count: {doc_count:,}")

            if last_updated_info:
                dt, source = last_updated_info
                now = datetime.now(timezone.utc)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                time_ago = now - dt
                days_ago = time_ago.days
                hours_ago = time_ago.seconds // 3600
                minutes_ago = (time_ago.seconds % 3600) // 60

                if days_ago > 0:
                    time_str = f"{days_ago} day{'s' if days_ago != 1 else ''}"
                elif hours_ago > 0:
                    time_str = f"{hours_ago} hour{'s' if hours_ago != 1 else ''}"
                else:
                    time_str = f"{minutes_ago} minute{'s' if minutes_ago != 1 else ''}"

                print(f"  Last Updated: {dt.strftime('%Y-%m-%d %H:%M:%S')} ({time_str} ago)")
                print(f"  Source: {source}")
            else:
                print("  Last Updated: Unable to determine")

        except Exception as e:
            print(f"\n[{stage.upper()}] {index_name}")
            print(f"  Error: {str(e)}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect Azure AI Search indices.")
    parser.add_argument(
        "stage",
        nargs="?",
        choices=["dev", "staging", "prod", "all"],
        help="Environment to check. Use 'all' to check kb-articles across all stages.",
    )
    parser.add_argument(
        "index_name",
        nargs="?",
        default=None,
        help="Optional index name to inspect. If omitted, all indices are listed.",
    )
    args = parser.parse_args()

    if args.stage == "all" or (args.stage is None and args.index_name is None):
        check_kb_articles_indices()
    elif args.index_name:
        inspect_index(args.stage, args.index_name)
    else:
        list_indices(args.stage)
