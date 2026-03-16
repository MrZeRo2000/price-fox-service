import argparse
import json
import sys
from pathlib import Path

from cfg import Configuration
from scraper import Parser, Scraper


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the price fox scraping pipeline (fetch + parse)."
    )
    parser.add_argument(
        "--data-path",
        default=None,
        help="Path to data directory. Defaults to project data folder.",
    )
    parser.add_argument(
        "--config-path",
        default=None,
        help="Path to product catalog JSON. Defaults to config/product-catalog.json.",
    )
    parser.add_argument(
        "--print-json",
        action="store_true",
        help="Print full scraper output JSON at the end.",
    )
    parser.add_argument(
        "--parse-only",
        action="store_true",
        help="Skip fetching and parse only the latest fetched session folder.",
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()

    try:
        configuration = Configuration(
            data_path=args.data_path,
            config_path=args.config_path,
        )
        if args.parse_only:
            parser = Parser(configuration)
            parse_results = parser.execute()
            result = {
                "fetch_results": [],
                "parse_results": parse_results,
            }
        else:
            scraper = Scraper(configuration)
            result = scraper.execute()
    except Exception as exc:
        print(f"Scraper failed: {exc}")
        return 1

    fetch_results = result.get("fetch_results", [])
    parse_results = result.get("parse_results", [])
    successful_parses = sum(1 for item in parse_results if item.get("status") == "success")

    if args.parse_only:
        print("Parse-only run completed.")
    else:
        print("Scraper run completed.")
    print(f"Fetched records: {len(fetch_results)}")
    print(f"Parsed records: {len(parse_results)}")
    print(f"Successful parses: {successful_parses}")

    session_root = None
    if fetch_results:
        first_html = fetch_results[0].get("result", {}).get("html")
        if first_html:
            session_root = Path(first_html).parents[2]
    if session_root is not None:
        print(f"Session output folder: {session_root}")

    if args.print_json:
        print(json.dumps(result, indent=2, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    sys.exit(main())
