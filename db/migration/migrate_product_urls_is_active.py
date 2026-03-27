import argparse
import sqlite3
from pathlib import Path


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def migrate(db_path: Path) -> None:
    with sqlite3.connect(str(db_path)) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("BEGIN")
        try:
            if not _table_exists(connection, "product_urls"):
                connection.execute("COMMIT")
                return

            columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(product_urls)").fetchall()
            }
            if "is_active" in columns:
                connection.execute("COMMIT")
                return

            connection.execute(
                """
                CREATE TABLE product_urls_new (
                    product_id INTEGER NOT NULL,
                    url_id INTEGER NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
                    PRIMARY KEY (product_id, url_id),
                    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
                    FOREIGN KEY (url_id) REFERENCES urls(id) ON DELETE CASCADE
                )
                """
            )
            connection.execute(
                """
                INSERT INTO product_urls_new (product_id, url_id, is_active)
                SELECT product_id, url_id, 1
                FROM product_urls
                """
            )
            connection.execute("DROP TABLE product_urls")
            connection.execute("ALTER TABLE product_urls_new RENAME TO product_urls")
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.execute("PRAGMA foreign_keys = ON")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Add is_active flag to product_urls table."
    )
    parser.add_argument(
        "--db-path",
        default=str(
            Path(__file__).resolve().parent.parent / "data" / "db" / "product-catalog.sqlite"
        ),
        help="Path to SQLite database file.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    db_path = Path(args.db_path).resolve()
    if not db_path.exists():
        parser.error(f"SQLite database file does not exist: {db_path}")
    migrate(db_path)
    print(f"Migration completed: {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
