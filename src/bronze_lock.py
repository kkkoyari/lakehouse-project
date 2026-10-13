from functools import wraps
import logging
from pathlib import Path
import time

import fcntl


PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOCK_PATH = PROJECT_ROOT / "data" / "bronze" / ".crypto_prices.lock"
logger = logging.getLogger(__name__)


def with_bronze_lock(func):
    """Serialize writes and compaction of the crypto prices Parquet files."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOCK_PATH.open("a") as lock_file:
            waiting_started = time.monotonic()
            logger.info("Waiting for Bronze file lock path=%s", LOCK_PATH)
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            waited_seconds = time.monotonic() - waiting_started
            if waited_seconds >= 5:
                logger.warning("Acquired Bronze file lock after waiting %.2f seconds", waited_seconds)
            else:
                logger.debug("Acquired Bronze file lock wait_seconds=%.2f", waited_seconds)
            try:
                return func(*args, **kwargs)
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                logger.debug("Released Bronze file lock")

    return wrapper
