import argparse
import logging
import sqlite3
from pathlib import Path

from config.settings import default_product_catalog_db_path

logger = logging.getLogger(__name__)


TABLES_TO_EXPORT = [
    "categories",
    "product_categories",
    "urls",
    "products",
    "product_urls",
]


def _sql_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, bytes):
        return f"X'{value.hex()}'"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _write_ddl(connection: sqlite3.Connection, output_path: Path) -> None:
    rows = connection.execute(
        """
        SELECT type, name, sql
        FROM sqlite_master
        WHERE sql IS NOT NULL
          AND name NOT LIKE 'sqlite_%'
        ORDER BY
          CASE type
              WHEN 'table' THEN 0
              WHEN 'index' THEN 1
              WHEN 'trigger' THEN 2
              WHEN 'view' THEN 3
              ELSE 4
          END,
          name
        """
    ).fetchall()

    statements: list[str] = []
    for _, _, sql in rows:
        sql_text = sql.strip()
        if not sql_text.endswith(";"):
            sql_text = f"{sql_text};"
        statements.append(sql_text)

    output_path.write_text("\n\n".join(statements) + "\n", encoding="utf-8")


def _build_table_order_clause(connection: sqlite3.Connection, table_name: str) -> str:
    pragma_rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    pk_columns = [row[1] for row in sorted(pragma_rows, key=lambda item: item[5]) if row[5] > 0]
    if pk_columns:
        return " ORDER BY " + ", ".join(pk_columns)
    return " ORDER BY rowid"


def _write_table_data(connection: sqlite3.Connection, table_name: str, output_path: Path) -> None:
    pragma_rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    if not pragma_rows:
        raise ValueError(f"Table '{table_name}' does not exist in the database.")

    column_names = [row[1] for row in pragma_rows]
    columns_csv = ", ".join(column_names)
    order_clause = _build_table_order_clause(connection, table_name)
    query = f"SELECT {columns_csv} FROM {table_name}{order_clause}"
    rows = connection.execute(query).fetchall()

    inserts: list[str] = []
    for row in rows:
        values_csv = ", ".join(_sql_literal(value) for value in row)
        inserts.append(
            f"INSERT INTO {table_name} ({columns_csv}) VALUES ({values_csv});"
        )

    output_path.write_text("\n".join(inserts) + ("\n" if inserts else ""), encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export SQLite DDL and selected tables into db/*.sql files."
    )
    parser.add_argument(
        "--db-path",
        default=default_product_catalog_db_path(),
        help="Path to SQLite database file.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).resolve().parent.parent / "db"),
        help="Directory where SQL files will be written.",
    )
    return parser


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    parser = _build_parser()
    args = parser.parse_args()

    db_path = Path(args.db_path).resolve()
    output_dir = Path(args.output_dir).resolve()

    if not db_path.exists():
        parser.error(f"SQLite database file does not exist: {db_path}")

    output_dir.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(str(db_path)) as connection:
        _write_ddl(connection=connection, output_path=output_dir / "ddl.sql")
        for table_name in TABLES_TO_EXPORT:
            _write_table_data(
                connection=connection,
                table_name=table_name,
                output_path=output_dir / f"{table_name}.sql",
            )

    logger.info(f"Updated SQL files in: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
