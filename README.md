# doc360-migration

Doc360 → Ava indexing pipeline migration code.

## Structure

```
doc360-migration/
└── mapping/
    ├── doc360_mapper.py      # Maps raw Doc360 article rows to the pipeline contract
    └── validate_mapping.py   # Runs the full filter chain and validates field mapping
```

## Usage

```bash
python mapping/validate_mapping.py
```

Requires `filter_config.yaml` to be present at the `chatbot-indexing-pipeline` root.
