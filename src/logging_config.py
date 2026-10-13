import logging
import os
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Union

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_ROOT / "logs"


def configure_logging(script_path: Union[str, Path]) -> logging.Logger:
    """Configure console and per-script rotating-file logging."""
    load_dotenv(PROJECT_ROOT / ".env")

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    if not isinstance(level, int):
        level = logging.INFO

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    if not getattr(root_logger, "_lakehouse_logging_configured", False):
        LOG_DIR.mkdir(parents=True, exist_ok=True)

        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S UTC",
        )
        formatter.converter = time.gmtime

        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)

        log_file = LOG_DIR / f"{Path(script_path).stem}.log"
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)

        root_logger.addHandler(console_handler)
        root_logger.addHandler(file_handler)
        root_logger._lakehouse_logging_configured = True

    logger = logging.getLogger(Path(script_path).stem)
    logger.info(
        "Logging initialized level=%s file=%s",
        logging.getLevelName(level),
        LOG_DIR / f"{Path(script_path).stem}.log",
    )
    return logger
