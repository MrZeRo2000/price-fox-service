from abc import ABC
from collections.abc import Iterator
from contextlib import contextmanager
import os
import sqlite3


class BaseSqliteRepository(ABC):
    """Shared SQLite helpers for repositories."""

    def __init__(self, db_path: str, *, missing_db_message: str | None = None):
        if not os.path.exists(db_path):
            raise ValueError(
                missing_db_message
                or f"SQLite database path {db_path} does not exist"
            )
        self._db_path = db_path

    @property
    def db_path(self) -> str:
        return self._db_path

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        with sqlite3.connect(self._db_path) as connection:
            yield connection
