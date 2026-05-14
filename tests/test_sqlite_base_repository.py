from pathlib import Path

import pytest

from repositories import BaseSqliteRepository


class _DummyRepository(BaseSqliteRepository):
    pass


def test_sqlite_base_repository_raises_for_missing_db_file(tmp_path: Path) -> None:
    missing_path = str(tmp_path / "missing.sqlite")

    with pytest.raises(
        ValueError, match=r"SQLite database path ([A-Za-z]:\\[\w\\.\-]+)missing.sqlite does not exist"
    ):
        _DummyRepository(missing_path)


def test_sqlite_base_repository_supports_custom_error_message(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing.sqlite"

    with pytest.raises(ValueError, match="Custom db missing message"):
        _DummyRepository(
            str(missing_path),
            missing_db_message="Custom db missing message",
        )


def test_sqlite_base_repository_connect_returns_sqlite_connection(tmp_path: Path) -> None:
    db_path = tmp_path / "test.sqlite"
    db_path.touch()
    repository = _DummyRepository(str(db_path))

    with repository._connect() as connection:
        value = connection.execute("SELECT 1").fetchone()[0]

    assert value == 1
