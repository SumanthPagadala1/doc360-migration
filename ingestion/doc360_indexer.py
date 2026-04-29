"""
Doc360 Azure AI Search Indexer

Handles the final stage of the pipeline: embedding articles with
Azure OpenAI and upserting them into the Azure AI Search index.

Target index: d360-test (staging), configurable via config or env.
Embedding model: text-embedding-3-small (1536 dimensions).
Record manager: SQLRecordManager for incremental de-duplication.
"""

import logging
import os

from langchain.indexes import SQLRecordManager, index
from langchain_community.vectorstores import AzureSearch
from langchain_openai import AzureOpenAIEmbeddings
from langchain_core.documents import Document
from azure.search.documents.indexes.models import (
    SimpleField,
    SearchableField,
    SearchField,
    SearchFieldDataType,
)

logger = logging.getLogger(__name__)

DEFAULT_INDEX_NAME = "d360-test"
DEFAULT_EMBEDDING_DEPLOYMENT = "text-embedding-3-small"
DEFAULT_API_VERSION = "2024-02-15-preview"
DEFAULT_VECTOR_DIMS = 1536
DEFAULT_HNSW_PROFILE = "myHnswProfile"


def _get_index_fields() -> list:
    """
    Define the Azure AI Search index schema for Doc360 articles.

    Extends the existing kb-articles schema with Doc360-specific fields:
    workspace, description, is_shareasale, from_doc360.
    """
    metadata_simple_fields = [
        "ArticleCreatedDate",
        "Language",
        "RecordTypeId",
        "Title",
        "account_type",
        "region",
        "workspace",
        "description",
        "is_shareasale",
        "from_doc360",
    ]

    return [
        SimpleField(
            name="id",
            type=SearchFieldDataType.String,
            key=True,
            filterable=True,
        ),
        SimpleField(name="metadata", type=SearchFieldDataType.String),
        SearchableField(
            name="content",
            type=SearchFieldDataType.String,
            searchable=True,
        ),
        SearchField(
            name="content_vector",
            type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
            searchable=True,
            vector_search_dimensions=DEFAULT_VECTOR_DIMS,
            vector_search_profile_name=DEFAULT_HNSW_PROFILE,
        ),
        *[
            SimpleField(
                name=col,
                type=SearchFieldDataType.String,
                filterable=True,
                facetable=True,
            )
            for col in metadata_simple_fields
        ],
        SearchField(
            name="client",
            type=SearchFieldDataType.Collection(SearchFieldDataType.String),
            filterable=True,
            facetable=True,
            searchable=True,
        ),
    ]


def _get_stage_suffix(config: dict | None = None) -> str:
    """Get the env var suffix based on stage (DEV, STAGING, PROD)."""
    stage = "DEV"
    if config:
        stage = config.get("stage", "DEV").upper()
    return stage


def _parse_embedding_endpoint(full_url: str) -> tuple[str, str, str]:
    """
    Parse the full Azure embedding URL into (base_endpoint, deployment, api_version).

    Input:  https://host.openai.azure.com/openai/deployments/NAME/embeddings?api-version=VER
    Output: (https://host.openai.azure.com/, NAME, VER)
    """
    import re as _re
    match = _re.match(
        r"(https://[^/]+)/openai/deployments/([^/]+)/embeddings\?api-version=(.+)",
        full_url,
    )
    if match:
        return match.group(1), match.group(2), match.group(3)
    return full_url, DEFAULT_EMBEDDING_DEPLOYMENT, DEFAULT_API_VERSION


def setup_embeddings(config: dict | None = None) -> AzureOpenAIEmbeddings:
    """
    Initialise Azure OpenAI embeddings from env vars.

    Env var naming: AZURE_EMBEDDING_ENDPOINT_{STAGE}, AZURE_EMBEDDING_API_KEY_{STAGE}
    where STAGE is DEV, STAGING, or PROD (set in config or defaults to DEV).
    """
    suffix = _get_stage_suffix(config)

    full_endpoint = os.getenv(f"AZURE_EMBEDDING_ENDPOINT_{suffix}")
    api_key = os.getenv(f"AZURE_EMBEDDING_API_KEY_{suffix}")

    if not full_endpoint or not api_key:
        raise ValueError(
            f"Missing AZURE_EMBEDDING_ENDPOINT_{suffix} or AZURE_EMBEDDING_API_KEY_{suffix}. "
            f"Set them in your .env file."
        )

    azure_endpoint, deployment, api_version = _parse_embedding_endpoint(full_endpoint)

    logger.info(
        "Embeddings: endpoint=%s deployment=%s version=%s",
        azure_endpoint, deployment, api_version,
    )

    return AzureOpenAIEmbeddings(
        azure_deployment=deployment,
        openai_api_version=api_version,
        azure_endpoint=azure_endpoint,
        api_key=api_key,
    )


def setup_vectorstore(
    index_name: str,
    embeddings: AzureOpenAIEmbeddings,
    config: dict | None = None,
    fields: list | None = None,
) -> AzureSearch:
    """
    Initialise the Azure AI Search vectorstore.

    Env var naming: AZURE_AI_SEARCH_ENDPOINT_{STAGE}, AZURE_AI_SEARCH_API_KEY_{STAGE}
    """
    suffix = _get_stage_suffix(config)

    search_endpoint = os.getenv(f"AZURE_AI_SEARCH_ENDPOINT_{suffix}")
    search_key = os.getenv(f"AZURE_AI_SEARCH_API_KEY_{suffix}")

    if not search_endpoint or not search_key:
        raise ValueError(
            f"Missing AZURE_AI_SEARCH_ENDPOINT_{suffix} or AZURE_AI_SEARCH_API_KEY_{suffix}. "
            f"Set them in your .env file."
        )

    logger.info("AI Search: endpoint=%s index=%s", search_endpoint, index_name)

    return AzureSearch(
        index_name=index_name,
        azure_search_endpoint=search_endpoint,
        azure_search_key=search_key,
        embedding_function=embeddings,
        fields=fields or _get_index_fields(),
        async_=True,
    )


def setup_record_manager(index_name: str, db_dir: str = "local_records") -> SQLRecordManager:
    """
    Initialise a SQLRecordManager backed by SQLite for
    incremental de-duplication.
    """
    namespace = f"azure-ai-search/{index_name}"
    os.makedirs(db_dir, exist_ok=True)
    db_path = os.path.join(db_dir, f"{index_name}.db")
    db_url = f"sqlite:///{db_path}"

    record_manager = SQLRecordManager(namespace, db_url=db_url)
    record_manager.create_schema()
    logger.info("Record manager ready: %s (db=%s)", namespace, db_path)
    return record_manager


def index_documents(
    documents: list[Document],
    config: dict | None = None,
) -> dict:
    """
    Full indexing pipeline: embed documents and upsert into Azure AI Search.

    Returns the LangChain index() result dict with keys:
      num_added, num_updated, num_skipped, num_deleted
    """
    index_name = DEFAULT_INDEX_NAME
    cleanup_mode = "incremental"
    if config:
        index_name = config.get("index_name", index_name)
        cleanup_mode = config.get("cleanup_mode", cleanup_mode)

    logger.info("Setting up indexing components for index: %s", index_name)

    embeddings = setup_embeddings(config)
    fields = _get_index_fields()
    vectorstore = setup_vectorstore(index_name, embeddings, config, fields)

    db_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "local_records",
    )
    record_manager = setup_record_manager(index_name, db_dir=db_dir)

    logger.info(
        "Indexing %d documents into '%s' (cleanup=%s)",
        len(documents), index_name, cleanup_mode,
    )

    result = index(
        documents,
        record_manager,
        vectorstore,
        cleanup=cleanup_mode,
        source_id_key="Id",
    )

    logger.info("Indexing result: %s", result)
    return result
