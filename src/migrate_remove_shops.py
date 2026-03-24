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
            if not _table_exists(connection, "urls"):
                connection.execute(
                    """
                    CREATE TABLE urls (
                        id INTEGER PRIMARY KEY,
                        url TEXT NOT NULL UNIQUE
                    )
                    """
                )

            if _table_exists(connection, "product_urls"):
                columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(product_urls)").fetchall()
                }
                if "url" in columns:
                    # Old schema: (product_id, shop_id, url)
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO urls (url)
                        SELECT DISTINCT url
                        FROM product_urls
                        WHERE url IS NOT NULL AND TRIM(url) <> ''
                        """
                    )
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
                        INSERT INTO product_urls_new (product_id, url_id)
                        SELECT pu.product_id, u.id
                        FROM product_urls pu
                        JOIN urls u ON u.url = pu.url
                        """
                    )
                    connection.execute("DROP TABLE product_urls")
                    connection.execute("ALTER TABLE product_urls_new RENAME TO product_urls")

            if _table_exists(connection, "scrape_detailed"):
                columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(scrape_detailed)"
                    ).fetchall()
                }
                if "shop_id" in columns or "shop_url" in columns:
                    connection.execute(
                        """
                        CREATE TABLE scrape_detailed_new (
                            session_date INTEGER NOT NULL,
                            product_id INTEGER NOT NULL,
                            url_id INTEGER NOT NULL,
                            url TEXT NOT NULL,
                            parsed_status INTEGER NOT NULL,
                            parsed_value INTEGER
                        )
                        """
                    )
                    connection.execute(
                        """
                        INSERT INTO scrape_detailed_new (
                            session_date, product_id, url_id, url, parsed_status, parsed_value
                        )
                        SELECT
                            sd.session_date,
                            sd.product_id,
                            COALESCE(u.id, 0) AS url_id,
                            sd.shop_url AS url,
                            sd.parsed_status,
                            sd.parsed_value
                        FROM scrape_detailed sd
                        LEFT JOIN urls u ON u.url = sd.shop_url
                        """
                    )
                    connection.execute("DROP TABLE scrape_detailed")
                    connection.execute("ALTER TABLE scrape_detailed_new RENAME TO scrape_detailed")
                    connection.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_scrape_detailed_session_date
                        ON scrape_detailed (session_date)
                        """
                    )
                    connection.execute(
                        """
                        DELETE FROM scrape_detailed
                        WHERE url_id = 0
                        """
                    )

            if _table_exists(connection, "scrape_consolidated"):
                columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(scrape_consolidated)"
                    ).fetchall()
                }
                if "best_shop_id" in columns or "best_shop_url" in columns:
                    connection.execute(
                        """
                        CREATE TABLE scrape_consolidated_new (
                            session_date INTEGER NOT NULL,
                            product_id INTEGER NOT NULL,
                            best_url_id INTEGER NOT NULL,
                            best_url TEXT NOT NULL,
                            best_value INTEGER,
                            PRIMARY KEY (session_date, product_id)
                        )
                        """
                    )
                    connection.execute(
                        """
                        INSERT INTO scrape_consolidated_new (
                            session_date, product_id, best_url_id, best_url, best_value
                        )
                        SELECT
                            sc.session_date,
                            sc.product_id,
                            COALESCE(u.id, 0) AS best_url_id,
                            sc.best_shop_url AS best_url,
                            sc.best_value
                        FROM scrape_consolidated sc
                        LEFT JOIN urls u ON u.url = sc.best_shop_url
                        """
                    )
                    connection.execute("DROP TABLE scrape_consolidated")
                    connection.execute(
                        "ALTER TABLE scrape_consolidated_new RENAME TO scrape_consolidated"
                    )
                    connection.execute(
                        """
                        DELETE FROM scrape_consolidated
                        WHERE best_url_id = 0
                        """
                    )

            if _table_exists(connection, "shops"):
                connection.execute("DROP TABLE shops")

            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.execute("PRAGMA foreign_keys = ON")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Migrate SQLite schema from shop-based to url-based model."
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
