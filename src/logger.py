import json
import logging
import logging.config
from pathlib import Path
from typing import Any


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


def create_application_logger(
    data_path: str,
    logs_dir: str | None = None,
    config_path: str | None = None,
) -> logging.Logger:
    logging_settings = _load_logging_configuration(config_path)
    project_root = Path(__file__).resolve().parents[1]
    resolved_data_path = Path(data_path).resolve()

    context = {
        "project_root": str(project_root),
        "data_path": str(resolved_data_path),
    }

    logs_root_template = logging_settings["logs_root"]
    logs_root = Path(logs_dir).resolve() if logs_dir else Path(logs_root_template.format(**context)).resolve()
    logs_root.mkdir(parents=True, exist_ok=True)

    log_file_name = f"{resolved_data_path.name or 'data'}.log"
    log_file_path = logs_root / log_file_name
    context.update(
        {
            "logs_root": str(logs_root),
            "log_file_name": log_file_name,
            "log_file_path": str(log_file_path),
            "log_file_path_lower": str(log_file_path).lower(),
        }
    )

    logger_name = logging_settings["logger_name"].format(**context)
    context["logger_name"] = logger_name
    dict_config = _format_with_context(logging_settings["dict_config"], context)
    logging.config.dictConfig(dict_config)
    return logging.getLogger(logger_name)
