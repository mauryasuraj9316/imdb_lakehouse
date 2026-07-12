"""
Load partitioned Parquet lake data into ClickHouse OLAP tables.

Applies DDL, truncates tables, bulk-loads from /data/lake/, and prints row counts.
"""

import glob
import os
import sys
from pathlib import Path

import clickhouse_connect
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT_DIR = Path(__file__).parent
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
LAKE_DIR = os.environ.get("IMDB_LAKE_DIR", str(PROJECT_DIR / "data" / "lake"))
DDL_DIR = os.environ.get("DDL_DIR", str(PROJECT_DIR / "ddl"))

BATCH_SIZE = 100_000

LAKE_TABLES = {
    "titles": {
        "table": "imdb.titles",
        "columns": [
            "tconst",
            "title_type",
            "primary_title",
            "original_title",
            "start_year",
            "end_year",
            "runtime_minutes",
            "genres",
            "primary_genre",
            "average_rating",
            "num_votes",
        ],
        "required": True,
    },
    "episodes": {
        "table": "imdb.episodes",
        "columns": [
            "episode_tconst",
            "parent_tconst",
            "season_number",
            "episode_number",
            "episode_title",
            "series_title",
            "start_year",
            "primary_genre",
            "average_rating",
            "num_votes",
            "runtime_minutes",
        ],
        "required": False,
    },
    "names": {
        "table": "imdb.names",
        "columns": [
            "nconst",
            "primary_name",
            "birth_year",
            "death_year",
            "primary_profession",
            "primary_role",
            "known_for_titles",
        ],
        "required": False,
    },
    "akas": {
        "table": "imdb.akas",
        "columns": [
            "title_id",
            "ordering",
            "title",
            "region",
            "language",
            "types",
            "is_original_title",
        ],
        "required": False,
    },
    "principals": {
        "table": "imdb.principals",
        "columns": [
            "tconst",
            "ordering",
            "nconst",
            "primary_name",
            "category",
            "job",
            "characters",
            "title_type",
            "primary_title",
            "primary_genre",
            "birth_year",
            "primary_role",
        ],
        "required": False,
    },
}


def get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username="default",
        password="",
    )


def run_ddl(client):
    ddl_files = sorted(glob.glob(os.path.join(DDL_DIR, "*.sql")))
    if not ddl_files:
        raise FileNotFoundError(f"No DDL files found in {DDL_DIR}")

    for ddl_file in ddl_files:
        print(f"Applying {os.path.basename(ddl_file)} ...")
        statements = Path(ddl_file).read_text().strip().split(";")
        for stmt in statements:
            stmt = stmt.strip()
            if stmt:
                client.command(stmt)


def insert_table(client, table_name: str, dataset_path: str, columns: list[str]):
    """Load parquet files in batches using PyArrow (no pandas required)."""
    files = sorted(
        glob.glob(os.path.join(dataset_path, "**", "*.parquet"), recursive=True)
    )
    if not files:
        raise FileNotFoundError(f"No Parquet files found under {dataset_path}")

    total = 0
    for parquet_file in files:
        table = pq.read_table(parquet_file).select(columns)
        if table.num_rows == 0:
            continue
        for batch in table.to_batches(max_chunksize=BATCH_SIZE):
            batch_table = pa.Table.from_batches([batch])
            client.insert_arrow(table_name, batch_table)
            total += batch.num_rows

    if total == 0:
        print(f"  {table_name}: no rows to load")
        return

    print(f"  {table_name}: loaded {total:,} rows")


def load_lake(client):
    for lake_name, config in LAKE_TABLES.items():
        client.command(f"TRUNCATE TABLE IF EXISTS {config['table']}")

    for lake_name, config in LAKE_TABLES.items():
        lake_path = os.path.join(LAKE_DIR, lake_name)
        if not os.path.isdir(lake_path):
            if config["required"]:
                raise FileNotFoundError(f"Required lake dataset missing: {lake_path}")
            continue

        print(f"Loading {lake_name} from {lake_path} ...")
        insert_table(client, config["table"], lake_path, config["columns"])


def print_counts(client):
    print("\nValidation:")
    for lake_name, config in LAKE_TABLES.items():
        table = config["table"]
        try:
            count = client.query(f"SELECT count() FROM {table}").first_item
            print(f"  {table}: {count:,} rows")
        except Exception:
            print(f"  {table}: not loaded")


def main():
    if not os.path.isdir(os.path.join(LAKE_DIR, "titles")):
        print(
            f"ERROR: titles lake not found under {LAKE_DIR}/\n"
            "Run etl_job.py first to generate Parquet files.",
            file=sys.stderr,
        )
        sys.exit(1)

    client = get_client()
    print(f"Connected to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")

    run_ddl(client)
    load_lake(client)
    print_counts(client)
    print("\nLoad complete.")


if __name__ == "__main__":
    main()
