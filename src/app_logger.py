import logging
import os
from pathlib import Path


def create_application_logger(data_path: str, logs_dir: str = None) -> logging.Logger:
    """
    Create a logger that writes to console and to file.
    Log filename is based on the configured data folder name.
    """
    resolved_data_path = Path(data_path).resolve()
    log_file_name = f"{resolved_data_path.name or 'data'}.log"

    if logs_dir is None:
        logs_root = Path(__file__).resolve().parents[1] / "log"
    else:
        logs_root = Path(logs_dir).resolve()
    logs_root.mkdir(parents=True, exist_ok=True)

    log_file_path = logs_root / log_file_name
    logger_name = f"price_fox.{str(log_file_path).lower()}"
    logger = logging.getLogger(logger_name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
