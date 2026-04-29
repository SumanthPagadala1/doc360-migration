"""
Doc360 Document Assembly

Builds LangChain Document objects from filtered Doc360 articles.

Content = Question__c + Answer__c (no Related_Questions__c — FAQ is
already part of Answer__c for Doc360).

No chunking — full articles are indexed as-is.
"""

import logging
from langchain_core.documents import Document

logger = logging.getLogger(__name__)

CONTENT_COLUMNS = ["Question__c", "Answer__c"]

METADATA_COLUMNS = [
    "Id",
    "ArticleCreatedDate",
    "Language",
    "RecordTypeId",
    "Title",
    "account_type",
    "region",
    "client",
    "workspace",
    "description",
    "is_shareasale",
    "from_doc360",
]


def build_documents(articles: list[dict]) -> list[Document]:
    """
    Convert filtered article dicts into LangChain Document objects.
    Each article becomes exactly one Document (no chunking).
    """
    documents = []

    for article in articles:
        content_parts = []
        for col in CONTENT_COLUMNS:
            value = article.get(col)
            if isinstance(value, list):
                content_parts.append(" ".join(map(str, value)))
            elif value:
                content_parts.append(str(value))

        page_content = "\n".join(content_parts)

        metadata = {}
        for col in METADATA_COLUMNS:
            val = article.get(col)
            if col == "client" and not isinstance(val, list):
                val = []
            if col == "is_shareasale":
                val = str(bool(val)).lower() if val is not None else "false"
            if col == "from_doc360":
                val = str(bool(val)).lower() if val is not None else "true"
            metadata[col] = val

        metadata["source"] = article.get("source")

        documents.append(
            Document(page_content=page_content, metadata=metadata)
        )

    logger.info("Assembled %d LangChain Documents", len(documents))
    return documents
