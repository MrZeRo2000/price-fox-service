import os
from dataclasses import dataclass

from logger import create_application_logger
from config.catalog_loader import load_catalog_from_database, load_catalog_from_json
from config.settings import resolve_configuration_settings
from models import CatalogData
from turso_sync import load_turso_sync_configuration


@dataclass(frozen=True)
class TursoConfiguration:
    enabled: bool
    url: str | None
    auth_token: str | None
    config_path: str


class Configuration:
    """Class for configuration"""
    def __init__(
        self,
        data_path: str = None,
        config_path: str = None,
        db_path: str = None,
    ):
        settings = resolve_configuration_settings(
            data_path=data_path,
            config_path=config_path,
            db_path=db_path,
        )
        data_path = settings.data_path
        product_catalog_path = settings.product_catalog_path
        product_catalog_db_path = settings.product_catalog_db_path

        if not os.path.exists(data_path):
            os.makedirs(data_path, exist_ok=True)

        self._data_path = data_path
        self._logger = create_application_logger(data_path=data_path)
        self._product_catalog_path = product_catalog_path if config_path is not None else None
        self._product_catalog_db_path = product_catalog_db_path if config_path is None else None
        self._product_catalog_data = (
            self.load_configuration_from_json(product_catalog_path)
            if config_path is not None
            else self.load_configuration_from_database(product_catalog_db_path)
        )
        self._turso = self.load_turso_configuration()

    @staticmethod
    def load_configuration_from_json(product_catalog_path: str) -> CatalogData:
        return load_catalog_from_json(product_catalog_path)

    @staticmethod
    def load_configuration_from_database(product_catalog_db_path: str) -> CatalogData:
        return load_catalog_from_database(product_catalog_db_path)

    @staticmethod
    def load_turso_configuration() -> TursoConfiguration:
        resolved = load_turso_sync_configuration()
        return TursoConfiguration(
            enabled=resolved.enabled,
            url=resolved.url,
            auth_token=resolved.auth_token,
            config_path=resolved.config_path,
        )

    @property
    def data_path(self):
        return self._data_path

    @property
    def product_catalog_path(self):
        return self._product_catalog_path

    @property
    def product_catalog_db_path(self):
        return self._product_catalog_db_path

    @property
    def product_catalog_data(self):
        return self._product_catalog_data

    @property
    def logger(self):
        return self._logger

    @property
    def turso(self) -> TursoConfiguration:
        return self._turso