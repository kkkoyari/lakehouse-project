import duckdb
from pathlib import Path

from logging_config import configure_logging


logger = configure_logging(__file__)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "lakehouse.db"


def init_duckdb() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for layer in ("bronze", "silver", "gold"):
        (DATA_DIR / layer).mkdir(parents=True, exist_ok=True)

    con = None
    try:
        logger.info("Connecting to DuckDB path=%s", DB_PATH)
        con = duckdb.connect(str(DB_PATH))
        logger.info("Installing/loading DuckDB postgres extension")
        con.execute("INSTALL postgres;")
        con.execute("LOAD postgres;")
        logger.info("DuckDB database initialized")
    except Exception:
        logger.exception("DuckDB initialization failed")
        raise
    finally:
        if con is not None:
            con.close()
            logger.debug("DuckDB connection closed")


if __name__ == "__main__":
    init_duckdb()
