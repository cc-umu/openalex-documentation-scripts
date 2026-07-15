These scripts flatten a local OpenAlex snapshot and load it into PostgreSQL. They target the current standard snapshot layout:

    openalex-snapshot/data/jsonl/{entity}/updated_date=*/part_*.gz

Profiles

The flattener supports three profiles:

    --profile core
        Smaller analytical subset intended for a PostgreSQL load on a single machine. It drops the largest optional works tables, removes `abstract_inverted_index`, and avoids repeated descriptive fields that can be joined from dimension tables.

    --profile extended
        Broad table coverage with some repeated/large columns removed.

    --profile full
        Exhaustive export matching the original generated schema. This can be very large.

ID modes

    --id-mode full
        Preserve full OpenAlex URL IDs in all ID columns.

    --id-mode numeric
        Add numeric keys on entity tables and use numeric `*_key` columns for OpenAlex entity references in generated tables. Dimension tables still keep the full OpenAlex `id`/`openalex` values for traceability.

Recommended validation run

    python3 openalex-documentation-scripts/flatten-openalex-jsonl.py --snapshot-dir /externalDB/openalex-snapshot --output-dir /tmp/openalex-core-sample --entities works,authors,institutions,sources,topics --profile core --id-mode numeric --limit 10000

Recommended full core flattening run

    python3 openalex-documentation-scripts/flatten-openalex-jsonl.py --snapshot-dir /externalDB/openalex-snapshot --output-dir /externalDB/csv-files-core-numeric --profile core --id-mode numeric

Core PostgreSQL load order

Create or use a database/tablespace on the large disk, not the root filesystem. Then load tables before indexes:

    createdb -U postgres openalex_core
    psql -U postgres -d openalex_core -f openalex-documentation-scripts/openalex-pg-schema-core-numeric.sql
    psql -U postgres -d openalex_core -v csv_dir=/externalDB/csv-files-core-numeric -f openalex-documentation-scripts/copy-openalex-csv-core-numeric.sql
    psql -U postgres -d openalex_core -f openalex-documentation-scripts/create-openalex-indexes-core-numeric.sql

Original exhaustive run

    python3 openalex-documentation-scripts/flatten-openalex-jsonl.py --snapshot-dir openalex-snapshot --output-dir csv-files --profile full --id-mode full

The original SQL files are generated from the full/full profile:

    python3 openalex-documentation-scripts/flatten-openalex-jsonl.py --write-sql

Profile-specific SQL files can be generated with the same options used for flattening:

    python3 openalex-documentation-scripts/flatten-openalex-jsonl.py --profile core --id-mode numeric --write-sql
