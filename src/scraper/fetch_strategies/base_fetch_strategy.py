from abc import ABC, abstractmethod
import logging
from typing import Optional


class BaseFetchStrategy(ABC):
    @abstractmethod
    def fetch_batch(
        self,
        urls: list[str],
        output_dir: str,
    ) -> list[dict]:
        raise NotImplementedError
