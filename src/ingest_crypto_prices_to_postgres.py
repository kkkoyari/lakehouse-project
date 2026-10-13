import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv

from logging_config import configure_logging


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")
logger = configure_logging(__file__)

API_BASE_URL = os.getenv("API_BASE_URL", "").rstrip("/")
API_KEY = os.getenv("API_KEY")
API_URL = f"{API_BASE_URL}/simple/price" if API_BASE_URL else None
API_HEADERS = {"x-cg-demo-api-key": API_KEY} if API_KEY else {}
API_PARAMS = {
    "ids": "bitcoin,ethereum,solana,toncoin,the-open-network",
    "vs_currencies": "usd,eur",
    "include_market_cap": "true",
    "include_24hr_vol": "true",
    "include_24hr_change": "true",
    "include_last_updated_at": "true",
}

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
POLL_INTERVAL_SECONDS = 60

INSERT_QUERY = """
    INSERT INTO api_raw.crypto_prices
        (coin_id, target_currency, price, market_cap, volume_24h, change_24h, last_updated_at)
    VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s))
"""


def fetch_and_insert() -> int:
    """Fetch and insert one snapshot; errors propagate to a scheduler or caller."""
    cycle_started = time.monotonic()
    conn = None
    cursor = None

    try:
        if not API_URL or not API_KEY:
            raise ValueError("API_BASE_URL and API_KEY must be configured in .env")

        logger.info("Requesting current crypto prices from the configured API")
        response = requests.get(
            API_URL,
            headers=API_HEADERS,
            params=API_PARAMS,
            timeout=(5, 20),
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError("The price API returned an invalid payload")

        logger.info("Price API response received assets=%d", len(data))
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME,
            connect_timeout=10,
        )
        cursor = conn.cursor()

        rows_inserted = 0
        for coin_id, metrics in data.items():
            last_updated_at = metrics.get("last_updated_at")
            for currency in ("usd", "eur"):
                if currency not in metrics:
                    continue

                cursor.execute(
                    INSERT_QUERY,
                    (
                        coin_id,
                        currency,
                        metrics.get(currency),
                        metrics.get(f"{currency}_market_cap"),
                        metrics.get(f"{currency}_24h_vol"),
                        metrics.get(f"{currency}_24h_change"),
                        last_updated_at,
                    ),
                )
                rows_inserted += 1

        conn.commit()
        logger.info(
            "Crypto price snapshot stored rows_inserted=%d elapsed_seconds=%.2f",
            rows_inserted,
            time.monotonic() - cycle_started,
        )
        return rows_inserted

    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

def run_every_minute() -> None:
    logger.info(
        "Crypto price collector started interval_seconds=%d",
        POLL_INTERVAL_SECONDS,
    )
    try:
        while True:
            cycle_started = time.monotonic()
            cycle_started_at = datetime.now(timezone.utc)
            logger.info(
                "Starting price API poll cycle started_at=%s",
                cycle_started_at.isoformat(),
            )
            try:
                fetch_and_insert()
            except requests.exceptions.RequestException as error:
                logger.warning("Price API request failed error=%s", error)
            except psycopg2.Error:
                logger.exception("PostgreSQL write failed")
            except Exception:
                logger.exception("Unexpected ingestion failure")

            wait_seconds = max(
                0,
                POLL_INTERVAL_SECONDS - (time.monotonic() - cycle_started),
            )
            next_poll_at = datetime.now(timezone.utc) + timedelta(seconds=wait_seconds)
            logger.info("Next price API poll scheduled_at=%s", next_poll_at.isoformat())
            time.sleep(wait_seconds)
    except KeyboardInterrupt:
        logger.info("Crypto price collector stopped by user")


if __name__ == "__main__":
    run_every_minute()
