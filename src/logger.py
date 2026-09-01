"""Application-wide logger singleton.

Import it anywhere and log -- nothing needs to accept or forward a logger:

    from logger import logger
    logger.info("...")

The logger is usable straight from import: this module configures it against
the default data path on first import, so a module that only ever imports it
still gets console + file output. Entry points that resolve a *different* data
path (``--data-path``, the one-time URL runner, the test suite) call
``configure_logging(data_path)`` once at startup to re-point the file handler at
the matching ``log/<data_dir_name>_<yyyymmdd>.log``.
"""
import json
import logging
import logging.config
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

LOGGER_NAME = "price_fox"

#: The one logger for the whole application. Handlers are attached by
#: ``configure_logging``; the object itself never changes identity, so
#: ``from logger import logger`` is safe to do before configuration.
logger = logging.getLogger(LOGGER_NAME)

_configured_log_file_path: Path | None = None


def _default_logging_config_path() -> Path:
    return Path(__file__).resolve().parents[1] / "config" / "logging.json"


def _load_logging_configuration(config_path: str | None) -> dict[str, Any]:
    resolved_config_path = (
        Path(config_path).resolve()
        if config_path is not None
        else _default_logging_config_path()
    )
    with resolved_config_path.open(encoding="utf-8") as file:
        return json.load(file)


def _format_with_context(value: Any, context: dict[str, str]) -> Any:
    if isinstance(value, str):
        return value.format(**context)
    if isinstance(value, list):
        return [_format_with_context(item, context) for item in value]
    if isinstance(value, dict):
        return {
            _format_with_context(key, context): _format_with_context(item, context)
            for key, item in value.items()
        }
    return value


def _default_data_path() -> str:
    # Imported lazily: config.settings must not depend on import order here.
    from config.settings import default_data_path

    return default_data_path()


def resolve_log_file_path(data_path: str | None = None, logs_dir: str | None = None) -> Path:
    """Where logs for ``data_path`` land: ``log/<data_dir_name>_<yyyymmdd>.log``."""
    logging_settings = _load_logging_configuration(None)
    project_root = Path(__file__).resolve().parents[1]
    resolved_data_path = Path(data_path if data_path is not None else _default_data_path()).resolve()

    context = {"project_root": str(project_root), "data_path": str(resolved_data_path)}
    logs_root = (
        Path(logs_dir).resolve()
        if logs_dir
        else Path(logging_settings["logs_root"].format(**context)).resolve()
    )
    dated_suffix = datetime.now().strftime("%Y%m%d")
    return logs_root / f"{resolved_data_path.name or 'data'}_{dated_suffix}.log"


def configure_logging(
    data_path: str | None = None,
    logs_dir: str | None = None,
    config_path: str | None = None,
) -> logging.Logger:
    """Point the singleton's handlers at the log file for ``data_path``.

    Idempotent: calling it again with the same resolved target is a no-op, and
    calling it with a different target replaces the handlers rather than
    stacking a second set on top.
    """
    global _configured_log_file_path

    log_file_path = resolve_log_file_path(data_path=data_path, logs_dir=logs_dir)
    if log_file_path == _configured_log_file_path:
        return logger

    log_file_path.parent.mkdir(parents=True, exist_ok=True)

    logging_settings = _load_logging_configuration(config_path)
    project_root = Path(__file__).resolve().parents[1]
    resolved_data_path = Path(data_path if data_path is not None else _default_data_path()).resolve()
    context = {
        "project_root": str(project_root),
        "data_path": str(resolved_data_path),
        "logs_root": str(log_file_path.parent),
        "log_file_name": log_file_path.name,
        "log_file_path": str(log_file_path),
        "log_file_path_lower": str(log_file_path).lower(),
        "logger_name": LOGGER_NAME,
    }

    # Release the previous file handler before dictConfig drops it, otherwise
    # the old log file stays open for the rest of the process.
    for handler in logger.handlers[:]:
        with_close = getattr(handler, "close", None)
        logger.removeHandler(handler)
        if with_close is not None:
            try:
                handler.close()
            except Exception:
                pass

    logging.config.dictConfig(_format_with_context(logging_settings["dict_config"], context))
    _configured_log_file_path = log_file_path
    return logger


def _log_uncaught(exc_type, exc_value, exc_traceback) -> None:
    """Last-resort net: log anything that escapes without reaching a handler."""
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.critical(
        "💥 Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback)
    )


def _log_uncaught_in_thread(args: threading.ExceptHookArgs) -> None:
    if issubclass(args.exc_type, SystemExit):
        return

    thread_name = args.thread.name if args.thread is not None else "<unknown>"
    logger.critical(
        f"💥 Uncaught exception in thread {thread_name}",
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
    )


configure_logging()
sys.excepthook = _log_uncaught
threading.excepthook = _log_uncaught_in_thread
logging.captureWarnings(True)
