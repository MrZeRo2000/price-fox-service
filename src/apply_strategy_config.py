import argparse
import logging
import sqlite3
from pathlib import Path

from config.settings import default_product_catalog_db_path

logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Apply strategy configuration SQL to the product catalog SQLite database."
    )
    parser.add_argument(
        "--db-path",
        default=default_product_catalog_db_path(),
        help="Path to SQLite database file.",
    )
    parser.add_argument(
        "--sql-path",
        default=str(
            Path(__file__).resolve().parent.parent
            / "db"
            / "ensure_strategy_config.sql"
        ),
        help="Path to SQL script that provisions strategy configuration.",
    )
    return parser


def apply_sql_script(db_path: Path, sql_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database file does not exist: {db_path}")
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL script file does not exist: {sql_path}")

    script = sql_path.read_text(encoding="utf-8")
    if not script.strip():
        raise ValueError(f"SQL script file is empty: {sql_path}")

    with sqlite3.connect(str(db_path)) as connection:
        connection.executescript(script)
        connection.commit()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    parser = _build_parser()
    args = parser.parse_args()

    db_path = Path(args.db_path).resolve()
    sql_path = Path(args.sql_path).resolve()

    try:
        apply_sql_script(db_path=db_path, sql_path=sql_path)
    except Exception as exc:
        logger.error(f"Failed to apply strategy config: {exc}")
        return 1

    logger.info("Strategy config applied successfully.")
    logger.info(f"Database: {db_path}")
    logger.info(f"Script: {sql_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
