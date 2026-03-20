import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ResolvedConfigurationSettings:
    data_path: str
    product_catalog_path: str
    product_catalog_db_path: str


def resolve_configuration_settings(
    data_path: str = None, config_path: str = None, db_path: str = None
) -> ResolvedConfigurationSettings:
    resolved_data_path = (
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/"))
        if data_path is None
        else data_path
    )
    resolved_product_catalog_path = (
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../config/product-catalog.json"))
        if config_path is None
        else config_path
    )
    resolved_product_catalog_db_path = (
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/db/product-catalog.sqlite"))
        if db_path is None
        else db_path
    )

    return ResolvedConfigurationSettings(
        data_path=resolved_data_path,
        product_catalog_path=resolved_product_catalog_path,
        product_catalog_db_path=resolved_product_catalog_db_path,
    )
