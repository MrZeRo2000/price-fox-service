from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

from .constants import DATA_SESSION_FOLDER_DATETIME_FORMAT


def is_session_folder_name(folder_name: str) -> bool:
    try:
        datetime.strptime(folder_name, DATA_SESSION_FOLDER_DATETIME_FORMAT)
        return True
    except ValueError:
        return False


def resolve_latest_scrape_session_folder(data_path: Path) -> Optional[Path]:
    scrape_root = data_path / "scrape"
    if not scrape_root.exists():
        return None

    session_folders = sorted(
        [
            folder
            for folder in scrape_root.iterdir()
            if folder.is_dir() and is_session_folder_name(folder.name)
        ],
        key=lambda folder: folder.name,
    )
    return session_folders[-1] if session_folders else None


def resolve_parser_data_root(base_data_root: Path) -> Path:
    # Prefer newer layout (`data/scrape/<timestamp>`) while keeping compatibility
    # with the previous (`data/<timestamp>`) structure.
    candidate_roots = [base_data_root / "scrape", base_data_root]
    for root in candidate_roots:
        if not root.exists():
            continue
        session_folders = sorted(
            [
                folder
                for folder in root.iterdir()
                if folder.is_dir() and is_session_folder_name(folder.name)
            ],
            key=lambda folder: folder.name,
        )
        if session_folders:
            return session_folders[-1]
    return base_data_root
