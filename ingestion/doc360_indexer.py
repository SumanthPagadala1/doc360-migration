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


def setup_embeddings(config: dict | None = None) -> AzureOpenAIEmbeddings:
    """Initialise Azure OpenAI embeddings from env vars."""
    deployment = DEFAULT_EMBEDDING_DEPLOYMENT
    api_version = DEFAULT_API_VERSION
    if config:
        deployment = config.get("embedding_model", deployment)
        api_version = config.get("embedding_api_version", api_version)

    azure_endpoint = os.getenv("AZURE_EMBEDDING_ENDPOINT")
    api_key = os.getenv("AZURE_EMBEDDING_API_KEY")

    if not azure_endpoint or not api_key:
        raise ValueError(
            "Missing AZURE_EMBEDDING_ENDPOINT or AZURE_EMBEDDING_API_KEY. "
            "Set them in your .env file."
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
    fields: list | None = None,
) -> AzureSearch:
    """Initialise the Azure AI Search vectorstore."""
    search_endpoint = os.getenv("AZURE_AI_SEARCH_ENDPOINT")
    search_key = os.getenv("AZURE_AI_SEARCH_API_KEY")

    if not search_endpoint or not search_key:
        raise ValueError(
            "Missing AZURE_AI_SEARCH_ENDPOINT or AZURE_AI_SEARCH_API_KEY. "
            "Set them in your .env file."
        )

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
    vectorstore = setup_vectorstore(index_name, embeddings, fields)

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
