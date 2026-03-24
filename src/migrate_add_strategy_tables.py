import argparse
from pathlib import Path

from repositories import PriceStrategyRepository


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create strategy tables for domain-based parser strategy config."
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

    repository = PriceStrategyRepository(str(db_path))
    repository.ensure_schema()
    print(f"Strategy tables ensured: {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
