# doc360-migration

Doc360 to Ava indexing pipeline — SAG-767 / SAG-769.

## Structure

```
doc360-migration/
├── mapping/                         # SAG-769: Field mapping and validation
│   ├── doc360_mapper.py             # Raw Doc360 row -> pipeline contract
│   └── validate_mapping.py          # Filter chain validation + sample CSV tests
├── ingestion/                       # SAG-767: Indexing pipeline
│   ├── doc360_client.py             # Data loading (CSV/JSON now, Databricks later)
│   ├── doc360_filters.py            # Production filter chain
│   ├── doc360_documents.py          # LangChain Document assembly
│   ├── doc360_indexer.py            # Azure AI Search indexing
│   ├── doc360_validate.py           # Pre-index validation checks
│   └── run_doc360_pipeline.py       # CLI entry point
├── doc360_filter_config.yaml        # Pipeline configuration
├── requirements.txt                 # Python dependencies
└── README.md
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Dry run (validate only, no indexing)
python ingestion/run_doc360_pipeline.py mapping/doc360-sample-new.csv --dry-run

# Index to d360-test (requires .env with Azure credentials)
python ingestion/run_doc360_pipeline.py data/doc360_export.csv
```

## Environment Variables

Create a `.env` file in this directory or the pipeline root:

```
AZURE_AI_SEARCH_ENDPOINT=https://your-search.search.windows.net
AZURE_AI_SEARCH_API_KEY=your-key
AZURE_EMBEDDING_ENDPOINT=https://your-openai.openai.azure.com/
AZURE_EMBEDDING_API_KEY=your-key
```

## Pipeline Flow

1. **Load** — Read Doc360 articles from CSV or JSON
2. **Map** — Transform raw rows to pipeline contract (doc360_mapper)
3. **Filter** — Visibility (published + not hidden) -> Workspace (drop developers) -> Question backfill
4. **Validate** — CSS artifacts, source URLs, publish status, empty content, ShareASale region
5. **Index** — Embed with Azure OpenAI (text-embedding-3-small) and upsert to Azure AI Search

## Filter Chain

| Filter | Rule |
|--------|------|
| Visibility | hidden=False AND status_name="Published" |
| Workspace | Drop "developers" workspace articles |
| Region | ShareASale tagged -> region="en_US" (US-only); all others -> region="en" (GB+US) |
| Account type | Always "advertiser" (current phase) |
| Content | Question__c + Answer__c (no Related_Questions__c; FAQ is in Answer__c) |
