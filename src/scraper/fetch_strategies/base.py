from abc import ABC, abstractmethod
import logging
from typing import Optional


class FetchStrategy(ABC):
    @abstractmethod
    def fetch_batch(
        self,
        urls: list[str],
        output_dir: str,
        logger: Optional[logging.Logger] = None,
    ) -> list[dict]:
        raise NotImplementedError
