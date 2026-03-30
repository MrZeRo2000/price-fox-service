import argparse
import sys

from turso_sync import TursoSyncClient, load_turso_sync_configuration


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Push local SQLite DB to Turso (initial load)."
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to local SQLite DB. Defaults to config.settings value.",
    )
    parser.add_argument(
        "--turso-config-path",
        default=None,
        help="Path to Turso config JSON. Defaults to config/turso.json.",
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    config = load_turso_sync_configuration(config_path=args.turso_config_path)
    sync_client = TursoSyncClient(config=config, db_path=args.db_path)
    try:
        result = sync_client.push_to_remote()
    except (RuntimeError, ValueError) as exc:
        print(f"Initial load failed: {exc}")
        return 1

    if result["status"] == "skipped":
        print(
            f"Initial load skipped: {result['reason']}. "
            f"Check '{config.config_path}' and set enabled/url/auth_token."
        )
        return 1

    print(
        f"Initial load complete. Local DB '{result['db_path']}' "
        f"synced to '{result['remote_url']}' via mode '{result.get('mode', 'unknown')}'."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
