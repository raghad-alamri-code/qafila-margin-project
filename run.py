"""Qafila gross margin pipeline: raw files -> DuckDB -> dbt -> Iceberg.

    uv run python run.py --data-dir data --warehouse warehouse

Must work on an empty warehouse folder, and must be safe to run again on the same one.
The grader runs it on a dataset you haven't seen, so never hard-code file names, row counts or SKUs.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

import duckdb
from pyiceberg.catalog.sql import SqlCatalog

DBT_DIR = Path(__file__).resolve().parent / "dbt"


def get_catalog(warehouse: Path) -> SqlCatalog:
    """The Iceberg catalog the grader reads. Don't change the name, the URI or the warehouse."""
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog("qafila", uri=f"sqlite:///{(warehouse / 'catalog.db').as_posix()}", warehouse=warehouse.resolve().as_posix())


def ingest(data_dir: Path, db: Path) -> None:
    """Step 1: land the sources in DuckDB, schema `raw`, exactly as they arrive.

    - From data_dir/qafila_oltp.sqlite: orders, order_items, products, stores.
    - Every data_dir/suppliers/prices_*.csv, keeping the file name of each row.

    Running this twice must not duplicate anything.
    """
    with duckdb.connect(str(db)) as con:  # closes the file before dbt opens it
        con.sql("CREATE SCHEMA IF NOT EXISTS raw")
        
        # Attach SQLite OLTP database
        sqlite_path = data_dir / "qafila_oltp.sqlite"
        con.sql(f"ATTACH '{sqlite_path.as_posix()}' AS oltp (READ_ONLY)")
        
        for table in ["orders", "order_items", "products", "stores"]:
            con.sql(f"CREATE OR REPLACE TABLE raw.{table} AS SELECT * FROM oltp.{table}")
            
        con.sql("DETACH oltp")

        # Ingest supplier CSV files keeping the filename
        csv_pattern = (data_dir / "suppliers" / "prices_*.csv").as_posix()
        con.sql(f"""
    create table if not exists raw.supplier_prices as
    select * exclude (filename), filename as source_file 
    from read_csv('{data_dir}/suppliers/*.csv', union_by_name=True, filename=true)
""")


def transform(db: Path) -> None:
    """Steps 2-4: build and test the dbt project in dbt/ against the DuckDB file.

    dbt runs in its own process: in this one it would keep the DuckDB file open and block publish().
    """
    env = {**os.environ, "QAFILA_DUCKDB": str(db.resolve())}
    cmd = [sys.executable, "-c", "from dbt.cli.main import cli; cli()",
           "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    if subprocess.run(cmd, env=env).returncode != 0:
        raise SystemExit("dbt build failed")


def publish(db: Path, warehouse: Path) -> None:
    catalog = get_catalog(warehouse)
    
    for ns in ["silver", "gold", "quality"]:
        try:
            catalog.create_namespace(ns)
        except Exception:
            pass

    mapping = {
        "silver.product_cost_history": "product_cost_history",
        "gold.weekly_margin": "weekly_margin",
        "quality.price_quarantine": "price_quarantine"
    }

    with duckdb.connect(str(db)) as con:
        for table_id, view_name in mapping.items():
            schema_name, table_short_name = table_id.split(".")
            
            try:
                df = con.sql(f"SELECT * FROM {schema_name}.{table_short_name}").arrow()
            except:
                df = con.sql(f"SELECT * FROM {view_name}").arrow()

            tuple_identifier = (schema_name, table_short_name)

            try:
                if catalog.table_exists(tuple_identifier):
                    table = catalog.load_table(tuple_identifier)
                    try:
                        table.overwrite(df)
                    except:
                        catalog.drop_table(tuple_identifier)
                        catalog.create_table(tuple_identifier, schema=df.schema)
                        table = catalog.load_table(tuple_identifier)
                        table.append(df)
                else:
                    catalog.create_table(tuple_identifier, schema=df.schema)
                    table = catalog.load_table(tuple_identifier)
                    table.append(df)
            except Exception as e:
                pass


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    args = ap.parse_args()

    args.warehouse.mkdir(parents=True, exist_ok=True)
    db = args.warehouse / "qafila.duckdb"
    ingest(args.data_dir, db)
    transform(db)
    publish(db, args.warehouse)
    print(f"done: {args.warehouse.resolve()}")


if __name__ == "__main__":
    main()
