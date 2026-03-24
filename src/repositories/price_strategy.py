import os
import sqlite3
from urllib.parse import urlparse


class PriceStrategyRepository:
    """Repository for domain-to-price-strategy configuration."""

    def __init__(self, db_path: str):
        if not os.path.exists(db_path):
            raise ValueError(f"Product catalog db path {db_path} does not exist")
        self._db_path = db_path

    @property
    def db_path(self) -> str:
        return self._db_path

    def ensure_schema(self) -> None:
        with sqlite3.connect(self._db_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS strategies (
                    id INTEGER PRIMARY KEY,
                    strategy_name TEXT NOT NULL UNIQUE
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS strategy_domains (
                    id INTEGER PRIMARY KEY,
                    domain TEXT NOT NULL UNIQUE,
                    strategy_id INTEGER NOT NULL,
                    FOREIGN KEY (strategy_id) REFERENCES strategies(id)
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_strategy_domains_domain
                ON strategy_domains (domain)
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS strategy_settings (
                    setting_key TEXT PRIMARY KEY,
                    setting_value TEXT NOT NULL
                )
                """
            )
            connection.executemany(
                "INSERT OR IGNORE INTO strategies (strategy_name) VALUES (?)",
                [("gemini_url",), ("playwright",), ("jina",)],
            )
            connection.execute(
                """
                UPDATE strategy_domains
                SET strategy_id = (
                    SELECT id FROM strategies WHERE strategy_name = 'playwright'
                )
                WHERE strategy_id = (
                    SELECT id FROM strategies WHERE strategy_name = 'default'
                )
                """
            )
            connection.execute(
                "DELETE FROM strategies WHERE strategy_name = 'default'"
            )
            connection.executemany(
                "INSERT OR IGNORE INTO strategy_settings (setting_key, setting_value) VALUES (?, ?)",
                [
                    ("default_fetch_strategy", "playwright"),
                    ("jina_rate_limit_rpm", "20"),
                    ("gemini_model", "gemini-2.0-flash"),
                    ("gemini_timeout_seconds", "45"),
                ],
            )

    def load_domain_strategy_overrides(self) -> dict[str, str]:
        self.ensure_schema()
        with sqlite3.connect(self._db_path) as connection:
            rows = connection.execute(
                """
                SELECT sd.domain, s.strategy_name
                FROM strategy_domains sd
                JOIN strategies s ON s.id = sd.strategy_id
                ORDER BY sd.id
                """
            ).fetchall()
        mapping: dict[str, str] = {}
        for domain, strategy_name in rows:
            domain_key = self._normalize_domain(domain)
            strategy = str(strategy_name or "").strip().lower()
            if not domain_key or not strategy:
                continue
            mapping[domain_key] = strategy
        return mapping

    @staticmethod
    def _normalize_domain(domain_value) -> str:
        raw = str(domain_value or "").strip().lower()
        if not raw:
            return ""
        if "://" in raw:
            host = (urlparse(raw).hostname or "").strip().lower()
            return host
        return raw

    def load_settings(self) -> dict[str, str]:
        self.ensure_schema()
        with sqlite3.connect(self._db_path) as connection:
            rows = connection.execute(
                """
                SELECT setting_key, setting_value
                FROM strategy_settings
                """
            ).fetchall()
        settings: dict[str, str] = {}
        for key, value in rows:
            settings[str(key).strip().lower()] = str(value).strip()
        return settings
