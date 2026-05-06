import sqlite3
from pathlib import Path

import pytest

from repositories import VersionsRepository


def _create_db_with_versions_table(db_path: Path) -> None:
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE versions (
                config_version INTEGER NOT NULL,
                scrape_version INTEGER NOT NULL
            )
            """
        )


def test_touch_scrape_version_updates_single_row(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog.sqlite"
    _create_db_with_versions_table(db_path)
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "INSERT INTO versions (config_version, scrape_version) VALUES (?, ?)",
            (1, 0),
        )

    repository = VersionsRepository(str(db_path))
    result = repository.touch_scrape_version()

    assert result == {"table": "versions", "updated_rows": 1, "row_count": 1}
    with sqlite3.connect(db_path) as connection:
        scrape_version = connection.execute(
            "SELECT scrape_version FROM versions"
        ).fetchone()[0]
    assert scrape_version > 0


def test_touch_scrape_version_requires_single_row(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog.sqlite"
    _create_db_with_versions_table(db_path)
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "INSERT INTO versions (config_version, scrape_version) VALUES (?, ?)",
            (1, 0),
        )
        connection.execute(
            "INSERT INTO versions (config_version, scrape_version) VALUES (?, ?)",
            (2, 0),
        )

    repository = VersionsRepository(str(db_path))
    with pytest.raises(
        RuntimeError, match="Table 'versions' must contain exactly one row"
    ):
        repository.touch_scrape_version()
