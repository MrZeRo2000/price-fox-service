import sqlite3

from repositories import BaseSqliteRepository


class _DummyRepository(BaseSqliteRepository):
    pass


def test_sqlite_base_repository_stores_and_uses_connection() -> None:
    connection = sqlite3.connect(":memory:")
    repository = _DummyRepository(connection)

    value = repository._connection.execute("SELECT 1").fetchone()[0]

    assert value == 1
