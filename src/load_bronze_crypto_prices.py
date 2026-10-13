import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import duckdb
from dotenv import load_dotenv
from bronze_naming import part_file_name
from bronze_lock import with_bronze_lock
from logging_config import configure_logging

# Absolute paths relative to the project root (src/.. = lakehouse-project)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")
logger = configure_logging(__file__)

DB_PATH = PROJECT_ROOT / "data" / "lakehouse.db"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "crypto_prices"
BRONZE_GLOB = str(BRONZE_DIR / "**" / "*.parquet")

PG_SCHEMA = "api_raw"
PG_TABLE = "crypto_prices"
INCREMENTAL_COL = "price_id"
PARTITION_FMT = "%Y-%m"   # Monthly; use "%Y-%m-%d" for daily partitions.
PARTITION_TIME_SQL = "COALESCE(last_updated_at, fetched_at)"
BRONZE_SOURCE = "market_data_api"
SCHEDULE_TZ = ZoneInfo("UTC")

PG_CONN_STR = (
    f"dbname={os.getenv('DB_NAME', 'crypto_analytics')} "
    f"user={os.getenv('DB_USER', 'crypto_writer')} "
    f"password={os.getenv('DB_PASSWORD', '')} "
    f"host={os.getenv('DB_HOST', 'localhost')} "
    f"port={os.getenv('DB_PORT', '5432')}"
)


@with_bronze_lock
def load_bronze_crypto_prices():
    load_started = time.monotonic()
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Starting Bronze load duckdb_path=%s", DB_PATH)
    con = None

    try:
        con = duckdb.connect(str(DB_PATH))
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"ATTACH '{PG_CONN_STR}' AS pg_db (TYPE postgres, READ_ONLY);")

        # 1. Read the watermark from files, not from whether the view exists.
        has_files = any(BRONZE_DIR.rglob("*.parquet"))
        max_id = 0
        if has_files:
            max_id = con.execute(f"""
                SELECT COALESCE(MAX({INCREMENTAL_COL}), 0)
                FROM read_parquet('{BRONZE_GLOB}', union_by_name = true, hive_partitioning = false)
            """).fetchone()[0]
        logger.info("Current incremental watermark %s=%s", INCREMENTAL_COL, max_id)

        # 2. Partition by the source update time. Older rows without it fall
        # back to fetched_at so they still land in a valid monthly partition.
        batch_loaded_at = datetime.now(timezone.utc)
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE new_batch AS
            SELECT *,
                   ?::TIMESTAMPTZ AS _loaded_at,
                   ?::VARCHAR AS _source,
                   {PARTITION_TIME_SQL} AS _partition_at,
                   strftime({PARTITION_TIME_SQL}, '{PARTITION_FMT}') AS _part
            FROM pg_db.{PG_SCHEMA}.{PG_TABLE}
            WHERE {INCREMENTAL_COL} > {max_id}
        """, [batch_loaded_at, BRONZE_SOURCE])
        new_rows = con.execute("SELECT COUNT(*) FROM new_batch").fetchone()[0]

        if new_rows == 0:
            logger.info("No new PostgreSQL rows; Bronze load skipped")
            return

        logger.info("New PostgreSQL rows available rows=%d", new_rows)

        # 3. Append-only write: existing Parquet files are not overwritten.
        # Small-file compaction is handled by a separate process.
        parts = [r[0] for r in con.execute(
            "SELECT DISTINCT _part FROM new_batch ORDER BY 1").fetchall()]

        for part in parts:
            part_dir = BRONZE_DIR / f"period={part}"
            part_dir.mkdir(exist_ok=True)
            min_timestamp, max_timestamp, part_rows = con.execute("""
                SELECT MIN(_partition_at), MAX(_partition_at), COUNT(*)
                FROM new_batch
                WHERE _part = ?
            """, [part]).fetchone()
            file_id = uuid4().hex
            target = part_dir / part_file_name(min_timestamp, max_timestamp, file_id)
            tmp = part_dir / f".part-{file_id}.parquet.tmp"

            new_part_sql = (
                f"SELECT * EXCLUDE (_part, _partition_at) "
                f"FROM new_batch WHERE _part = '{part}'"
            )
            # Write to a temporary file first, then publish it atomically.
            con.execute(f"""
                COPY ({new_part_sql} ORDER BY {INCREMENTAL_COL})
                TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
            os.replace(tmp, target)
            logger.info(
                "Bronze partition written period=%s rows=%d file=%s",
                part,
                part_rows,
                target.name,
            )

        # 4. The Hive-style period column is inferred from each partition folder.
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        con.execute(f"""
            CREATE OR REPLACE VIEW bronze.crypto_prices AS
            SELECT * FROM read_parquet('{BRONZE_GLOB}',
                                       union_by_name = true,
                                       hive_partitioning = true)
        """)
        logger.info(
            "Bronze load completed rows=%d partitions=%d elapsed_seconds=%.2f",
            new_rows,
            len(parts),
            time.monotonic() - load_started,
        )

    except Exception:
        logger.exception("Bronze load failed")
        raise  # Re-raise so cron or Airflow can detect the failure.
    finally:
        if con is not None:
            try:
                con.execute("DETACH pg_db;")
            except Exception:
                logger.debug("PostgreSQL attachment was not active during cleanup", exc_info=True)
            con.close()
            logger.debug("Bronze load connection closed")


def run_every_15_minutes():
    logger.info("Bronze loader started interval=15 minutes timezone=%s", SCHEDULE_TZ.key)

    try:
        while True:
            logger.info(
                "Starting Bronze load cycle started_at=%s",
                datetime.now(SCHEDULE_TZ).isoformat(),
            )
            try:
                load_bronze_crypto_prices()
            except Exception as error:
                logger.warning(
                    "Scheduled Bronze load failed; retrying at the next interval error=%s",
                    error,
                )

            now = datetime.now(SCHEDULE_TZ)
            minutes_until_next_run = 15 - (now.minute % 15)
            next_run = now.replace(second=0, microsecond=0) + timedelta(
                minutes=minutes_until_next_run
            )
            wait_seconds = max((next_run - now).total_seconds(), 0)
            logger.info("Next Bronze load scheduled_at=%s", next_run.isoformat())
            time.sleep(wait_seconds)
    except KeyboardInterrupt:
        logger.info("Bronze loader stopped by user")


if __name__ == "__main__":
    run_every_15_minutes()
