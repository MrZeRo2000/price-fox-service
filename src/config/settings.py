import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ResolvedConfigurationSettings:
    data_path: str
    product_catalog_path: str
    product_catalog_db_path: str


def _project_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))


def default_data_path() -> str:
    return os.path.join(_project_root(), "data")


def default_product_catalog_path() -> str:
    return os.path.join(_project_root(), "config", "product-catalog.json")


def default_product_catalog_db_path() -> str:
    return os.path.join(_project_root(), "db", "database", "product-catalog.sqlite")


def resolve_configuration_settings(
    data_path: str = None, config_path: str = None, db_path: str = None
) -> ResolvedConfigurationSettings:
    resolved_data_path = default_data_path() if data_path is None else os.path.abspath(data_path)
    resolved_product_catalog_path = (
        default_product_catalog_path()
        if config_path is None
        else os.path.abspath(config_path)
    )
    resolved_product_catalog_db_path = (
        default_product_catalog_db_path() if db_path is None else os.path.abspath(db_path)
    )

    return ResolvedConfigurationSettings(
        data_path=resolved_data_path,
        product_catalog_path=resolved_product_catalog_path,
        product_catalog_db_path=resolved_product_catalog_db_path,
    )
