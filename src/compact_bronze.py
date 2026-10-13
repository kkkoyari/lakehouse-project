import os
import re
from datetime import datetime, time as datetime_time, timedelta
from pathlib import Path
import time
from uuid import uuid4
from zoneinfo import ZoneInfo

import duckdb
from dotenv import load_dotenv

from bronze_naming import PART_FILE_RE, part_file_name
from bronze_lock import with_bronze_lock
from logging_config import configure_logging


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")
logger = configure_logging(__file__)

DB_PATH = PROJECT_ROOT / "data" / "lakehouse.db"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "crypto_prices"
BRONZE_GLOB = str(BRONZE_DIR / "**" / "*.parquet")
INCREMENTAL_COL = "price_id"
BACKUP_RE = re.compile(r"^\.(?P<source>.+\.parquet)\.compacting-(?P<run_id>[0-9a-f]+)$")
SCHEDULE_TZ = ZoneInfo("UTC")
SCHEDULE_TIME = datetime_time(hour=5, minute=0)


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _file_paths_sql(files: list[Path]) -> str:
    return "[" + ", ".join(_sql_string(str(path)) for path in files) + "]"


def _partition_time_bounds(
    con: duckdb.DuckDBPyConnection,
    files: list[Path],
) -> tuple[datetime, datetime]:
    paths_sql = _file_paths_sql(files)
    min_timestamp, max_timestamp = con.execute(f"""
        SELECT MIN(COALESCE(last_updated_at, fetched_at)),
               MAX(COALESCE(last_updated_at, fetched_at))
        FROM read_parquet(
            {paths_sql},
            union_by_name = true,
            hive_partitioning = false
        )
    """).fetchone()
    if min_timestamp is None or max_timestamp is None:
        raise ValueError(f"No rows found in Parquet files: {files}")
    return min_timestamp, max_timestamp


def _recover_interrupted_compaction(partition_dir: Path) -> None:
    """Restore inputs or finish cleanup after an interrupted file swap."""
    for tmp in partition_dir.glob(".part-*.parquet.tmp"):
        tmp.unlink()
        logger.warning("Removed incomplete compaction output file=%s", tmp)

    for backup in partition_dir.glob(".*.parquet.compacting-*"):
        match = BACKUP_RE.match(backup.name)
        if not match:
            continue

        output_files = list(partition_dir.glob(f"part-*-*-{match.group('run_id')}.parquet"))
        if output_files:
            # The compacted file was published; the hidden originals are obsolete.
            backup.unlink()
            logger.warning("Removed stale compaction backup file=%s", backup)
            continue

        original = partition_dir / match.group("source")
        if original.exists():
            original = partition_dir / f"recovered-{uuid4().hex}-{match.group('source')}"
        os.replace(backup, original)
        logger.warning("Restored file after interrupted compaction path=%s", original)


def _normalize_single_file(
    con: duckdb.DuckDBPyConnection,
    partition_dir: Path,
    source: Path,
) -> Path:
    if PART_FILE_RE.fullmatch(source.name):
        return source

    min_timestamp, max_timestamp = _partition_time_bounds(con, [source])
    target = partition_dir / part_file_name(min_timestamp, max_timestamp)
    os.replace(source, target)
    logger.info(
        "Normalized single-file partition=%s source=%s target=%s",
        partition_dir.name,
        source.name,
        target.name,
    )
    return target


def _compact_partition(con: duckdb.DuckDBPyConnection, partition_dir: Path, files: list[Path]) -> None:
    run_id = uuid4().hex
    tmp = partition_dir / f".part-{run_id}.parquet.tmp"
    backups: list[tuple[Path, Path]] = []
    published = False

    paths_sql = _file_paths_sql(files)
    logger.info(
        "Compacting partition=%s input_files=%d",
        partition_dir.name,
        len(files),
    )
    try:
        con.execute(f"""
            COPY (
                SELECT *
                FROM read_parquet(
                    {paths_sql},
                    union_by_name = true,
                    hive_partitioning = false
                )
                ORDER BY {INCREMENTAL_COL}
            )
            TO {_sql_string(str(tmp))}
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        min_timestamp, max_timestamp = _partition_time_bounds(con, [tmp])
        target = partition_dir / part_file_name(min_timestamp, max_timestamp, run_id)

        # Hide the source files before publishing the merged file, so readers
        # never see both the source data and its compacted copy at the same time.
        for source in files:
            backup = partition_dir / f".{source.name}.compacting-{run_id}"
            os.replace(source, backup)
            backups.append((source, backup))

        os.replace(tmp, target)
        published = True
    except BaseException:
        # If publishing failed, restore every source file already moved aside.
        if not published:
            for source, backup in backups:
                if backup.exists():
                    restore_path = source
                    if restore_path.exists():
                        restore_path = partition_dir / f"recovered-{uuid4().hex}-{source.name}"
                    os.replace(backup, restore_path)
        raise
    finally:
        if tmp.exists():
            tmp.unlink()

    # If cleanup is interrupted, the next run recognizes these transaction
    # backups and removes them because the compacted file has been published.
    for _, backup in backups:
        try:
            backup.unlink()
        except OSError as e:
            logger.warning("Could not remove compaction backup file=%s error=%s", backup, e)

    logger.info(
        "Partition compaction completed partition=%s input_files=%d output_file=%s",
        partition_dir.name,
        len(files),
        target.name,
    )


@with_bronze_lock
def compact_bronze() -> None:
    compact_started = time.monotonic()
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Starting Bronze compaction path=%s", BRONZE_DIR)
    con = duckdb.connect(str(DB_PATH))
    compacted_partitions = 0

    try:
        partition_dirs = sorted(BRONZE_DIR.glob("period=*"))
        logger.info("Bronze partitions discovered count=%d", len(partition_dirs))
        for partition_dir in partition_dirs:
            if not partition_dir.is_dir():
                continue

            _recover_interrupted_compaction(partition_dir)
            files = [path for path in partition_dir.glob("*.parquet") if path.is_file()]
            files.sort()
            if len(files) == 1:
                if _normalize_single_file(con, partition_dir, files[0]) != files[0]:
                    compacted_partitions += 1
                continue
            if len(files) < 2:
                continue

            _compact_partition(con, partition_dir, files)
            compacted_partitions += 1

        if compacted_partitions:
            con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
            con.execute(f"""
                CREATE OR REPLACE VIEW bronze.crypto_prices AS
                SELECT * FROM read_parquet(
                    '{BRONZE_GLOB}',
                    union_by_name = true,
                    hive_partitioning = true
                )
            """)
        logger.info(
            "Bronze compaction completed partitions_processed=%d elapsed_seconds=%.2f",
            compacted_partitions,
            time.monotonic() - compact_started,
        )
    except Exception:
        logger.exception("Bronze compaction failed")
        raise
    finally:
        con.close()


def run_daily_at_9am() -> None:
    """Run Bronze compaction once a day at 09:00 UTC."""
    logger.info("Bronze compaction scheduler started schedule=09:00 timezone=UTC")
    try:
        while True:
            now = datetime.now(SCHEDULE_TZ)
            next_run = now.replace(
                hour=SCHEDULE_TIME.hour,
                minute=SCHEDULE_TIME.minute,
                second=0,
                microsecond=0,
            )
            if next_run <= now:
                next_run += timedelta(days=1)

            wait_seconds = max((next_run - now).total_seconds(), 0)
            logger.info("Next Bronze compaction scheduled_at=%s", next_run.isoformat())
            time.sleep(wait_seconds)

            try:
                logger.info(
                    "Running scheduled Bronze compaction scheduled_at=%s started_at=%s",
                    next_run.isoformat(),
                    datetime.now(SCHEDULE_TZ).isoformat(),
                )
                compact_bronze()
            except Exception:
                logger.exception("Scheduled Bronze compaction failed; next run is tomorrow")
    except KeyboardInterrupt:
        logger.info("Bronze compaction scheduler stopped by user")


if __name__ == "__main__":
    run_daily_at_9am()
