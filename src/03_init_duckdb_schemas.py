from pathlib import Path

import duckdb

from logging_config import configure_logging


logger = configure_logging(__file__)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "lakehouse.db"
SCHEMAS = ("bronze", "silver", "gold")


def init_schemas() -> None:
    con = None
    try:
        logger.info("Initializing DuckDB schemas database=%s", DB_PATH)
        con = duckdb.connect(str(DB_PATH))
        for schema in SCHEMAS:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            logger.info("DuckDB schema ready schema=%s", schema)
        logger.info("DuckDB schema initialization completed")
    except Exception:
        logger.exception("DuckDB schema initialization failed")
        raise
    finally:
        if con is not None:
            con.close()
            logger.debug("DuckDB connection closed")


if __name__ == "__main__":
    init_schemas()
