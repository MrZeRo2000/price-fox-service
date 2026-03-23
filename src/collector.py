from __future__ import annotations

import json
import logging
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Optional

from session import resolve_latest_scrape_session_folder

class ScrapeDetailedCollector:
    """Collect scrape_detailed rows from latest scrape session folder."""

    def __init__(self, data_path: str, logger: Optional[logging.Logger] = None):
        self._data_path = Path(data_path)
        self._logger = logger or logging.getLogger("price_fox")

        if not self._data_path.exists():
            raise ValueError(f"Data path does not exist: {self._data_path}")

    def _resolve_latest_session_folder(self) -> Optional[Path]:
        return resolve_latest_scrape_session_folder(self._data_path)

    @staticmethod
    def _load_json(file_path: Path) -> dict:
        return json.loads(file_path.read_text(encoding="utf-8"))

    @staticmethod
    def _to_session_date(session_folder_name: str) -> int:
        return int(session_folder_name[:8])

    @staticmethod
    def _to_parsed_status(raw_status: Optional[str]) -> int:
        return 1 if raw_status == "success" else 0

    @staticmethod
    def _to_parsed_value(raw_price: Optional[object]) -> Optional[int]:
        if raw_price is None:
            return None
        try:
            decimal_price = Decimal(str(raw_price))
        except Exception:
            return None
        return int((decimal_price * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))

    def collect_latest_session_rows(
        self,
    ) -> tuple[Optional[int], list[tuple[int, int, int, str, int, Optional[int]]]]:
        latest_session_folder = self._resolve_latest_session_folder()
        if latest_session_folder is None:
            self._logger.info("No scrape session folders found in data/scrape.")
            return None, []

        session_date = self._to_session_date(latest_session_folder.name)
        rows: list[tuple[int, int, int, str, int, Optional[int]]] = []

        for product_folder in sorted([item for item in latest_session_folder.iterdir() if item.is_dir()]):
            if not product_folder.name.isdigit():
                continue
            product_id = int(product_folder.name)

            for url_folder in sorted([item for item in product_folder.iterdir() if item.is_dir()]):
                if not url_folder.name.isdigit():
                    continue
                url_id = int(url_folder.name)

                metadata_path = url_folder / "metadata.json"
                parsed_path = url_folder / "parsed.json"
                if not metadata_path.exists() or not parsed_path.exists():
                    self._logger.warning(
                        "Skipping product_id=%s, url_id=%s because metadata.json or parsed.json is missing.",
                        product_id,
                        url_id,
                    )
                    continue

                metadata = self._load_json(metadata_path)
                parsed = self._load_json(parsed_path)

                rows.append(
                    (
                        session_date,
                        product_id,
                        url_id,
                        str(metadata.get("url") or "").strip(),
                        self._to_parsed_status(parsed.get("status")),
                        self._to_parsed_value(parsed.get("price")),
                    )
                )

        return session_date, rows
