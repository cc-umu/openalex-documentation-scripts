These scripts flatten a local OpenAlex snapshot and load it into PostgreSQL. They target the current standard snapshot layout:

    openalex-snapshot/data/jsonl/{entity}/updated_date=*/part_*.gz

Typical validation run:

    python3 flatten-openalex-jsonl.py --entities works,authors --limit 1000 --output-dir /tmp/openalex-csv-sample

Full run from the repository root:

    python3 openalex-documentation-scripts/flatten-openalex-jsonl.py --snapshot-dir openalex-snapshot --output-dir csv-files

Load order in PostgreSQL:

    createdb -U postgres openalex
    psql -U postgres -d openalex -f openalex-documentation-scripts/openalex-pg-schema.sql
    psql -U postgres -d openalex -v csv_dir=csv-files -f openalex-documentation-scripts/copy-openalex-csv.sql
    psql -U postgres -d openalex -f openalex-documentation-scripts/create-openalex-indexes.sql

`openalex-pg-schema.sql`, `copy-openalex-csv.sql`, and `create-openalex-indexes.sql` are generated from the table specs in `flatten-openalex-jsonl.py`:

    python3 openalex-documentation-scripts/flatten-openalex-jsonl.py --write-sql
