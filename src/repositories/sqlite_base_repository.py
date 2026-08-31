from abc import ABC


class BaseSqliteRepository(ABC):
    """Shared base for repositories using the shared product-catalog DB connection."""

    def __init__(self, connection):
        self._connection = connection
