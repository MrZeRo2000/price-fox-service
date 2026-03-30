import json
import os
import sqlite3
import sys
from dataclasses import dataclass

from config.settings import default_product_catalog_db_path


@dataclass(frozen=True)
class TursoSyncConfiguration:
    enabled: bool
    url: str | None
    auth_token: str | None
    config_path: str

    @property
    def is_ready(self) -> bool:
        return bool(self.enabled and self.url and self.auth_token)


def _project_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))


def default_turso_config_path() -> str:
    return os.path.join(_project_root(), "config", "turso.json")


def load_turso_sync_configuration(config_path: str | None = None) -> TursoSyncConfiguration:
    resolved_config_path = (
        default_turso_config_path()
        if config_path is None
        else os.path.abspath(config_path)
    )
    if not os.path.exists(resolved_config_path):
        return TursoSyncConfiguration(
            enabled=False,
            url=None,
            auth_token=None,
            config_path=resolved_config_path,
        )

    with open(resolved_config_path, encoding="utf-8") as handle:
        data = json.load(handle)

    enabled = bool(data.get("enabled", False))
    url = data.get("url")
    auth_token = data.get("auth_token")

    if url is not None:
        url = str(url).strip()
    if auth_token is not None:
        auth_token = str(auth_token).strip()

    return TursoSyncConfiguration(
        enabled=enabled,
        url=url or None,
        auth_token=auth_token or None,
        config_path=resolved_config_path,
    )


class TursoSyncClient:
    def __init__(self, config: TursoSyncConfiguration, db_path: str | None = None):
        self._config = config
        self._db_path = default_product_catalog_db_path() if db_path is None else db_path

    @property
    def db_path(self) -> str:
        return self._db_path

    def pull_from_remote(self) -> dict:
        return self._sync(direction="pull")

    def push_to_remote(self) -> dict:
        return self._sync(direction="push")

    def replace_remote_with_local(self) -> dict:
        """
        Force remote Turso DB to match local SQLite exactly.
        Drops remote user objects and uploads full local dump.
        """
        if not self._config.enabled:
            return {
                "status": "skipped",
                "direction": "push",
                "reason": "disabled",
            }
        if not self._config.is_ready:
            raise ValueError(
                f"Turso is enabled, but url/auth_token are missing in '{self._config.config_path}'."
            )
        os.makedirs(os.path.dirname(self._db_path), exist_ok=True)

        _push_local_sqlite_to_remote(
            db_path=self._db_path,
            sync_url=self._config.url,
            auth_token=self._config.auth_token,
        )
        return {
            "status": "success",
            "direction": "push",
            "mode": "direct_upload",
            "db_path": self._db_path,
            "remote_url": self._config.url,
        }

    def _sync(self, direction: str) -> dict:
        if not self._config.enabled:
            return {
                "status": "skipped",
                "direction": direction,
                "reason": "disabled",
            }
        if not self._config.is_ready:
            raise ValueError(
                f"Turso is enabled, but url/auth_token are missing in '{self._config.config_path}'."
            )
        os.makedirs(os.path.dirname(self._db_path), exist_ok=True)

        sync_mode = "replica_sync"
        connection = None
        try:
            try:
                connection = _connect_libsql(
                    db_path=self._db_path,
                    sync_url=self._config.url,
                    auth_token=self._config.auth_token,
                )
                # libSQL sync is bi-directional; we expose pull/push wrappers for pipeline intent.
                connection.sync()
            except Exception as exc:
                if direction == "push" and _is_missing_replica_metadata_error(exc):
                    _push_local_sqlite_to_remote(
                        db_path=self._db_path,
                        sync_url=self._config.url,
                        auth_token=self._config.auth_token,
                    )
                    sync_mode = "direct_upload"
                else:
                    raise RuntimeError(
                        f"Turso {direction} sync failed: {exc}"
                    ) from exc
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass

        return {
            "status": "success",
            "direction": direction,
            "mode": sync_mode,
            "db_path": self._db_path,
            "remote_url": self._config.url,
        }


def _connect_libsql(db_path: str, sync_url: str, auth_token: str):
    try:
        import libsql  # type: ignore

        try:
            return libsql.connect(db_path, sync_url=sync_url, auth_token=auth_token)
        except TypeError as exc:
            raise RuntimeError(
                "Installed 'libsql' package does not support Turso embedded replica "
                "arguments (sync_url/auth_token). Upgrade with: "
                f"'{sys.executable} -m pip install --upgrade libsql'."
            ) from exc
    except ImportError as exc:
        raise RuntimeError(
            "Turso sync dependencies are not installed for this Python interpreter "
            f"('{sys.executable}'). Install them with: "
            f"'{sys.executable} -m pip install libsql'."
        ) from exc


def _is_missing_replica_metadata_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "metadata file does not" in message and "db file exists" in message
    ) or "invalid local state" in message


def _push_local_sqlite_to_remote(db_path: str, sync_url: str, auth_token: str) -> None:
    if not os.path.exists(db_path):
        raise RuntimeError(f"Local SQLite database does not exist: '{db_path}'.")

    try:
        import libsql  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "libsql is required for Turso upload. Install with: "
            f"'{sys.executable} -m pip install libsql'."
        ) from exc

    local_conn = sqlite3.connect(db_path)
    remote_conn = libsql.connect(sync_url, auth_token=auth_token)
    try:
        remote_conn.execute("PRAGMA foreign_keys = OFF;")
        _drop_remote_user_objects(remote_conn)

        for statement in local_conn.iterdump():
            sql = statement.strip()
            if (
                not sql
                or sql.startswith("BEGIN TRANSACTION")
                or sql.startswith("COMMIT")
                or sql.startswith("PRAGMA")
            ):
                continue
            remote_conn.execute(sql)

        remote_conn.commit()
    except Exception as exc:
        raise RuntimeError(f"Failed to upload local SQLite DB to Turso: {exc}") from exc
    finally:
        remote_conn.close()
        local_conn.close()


def _drop_remote_user_objects(remote_conn) -> None:
    rows = remote_conn.execute(
        "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%';"
    ).fetchall()
    for object_type, object_name in rows:
        if object_type == "view":
            remote_conn.execute(f'DROP VIEW IF EXISTS "{_escape_identifier(object_name)}";')
        elif object_type == "table":
            remote_conn.execute(
                f'DROP TABLE IF EXISTS "{_escape_identifier(object_name)}";'
            )


def _escape_identifier(value: str) -> str:
    return str(value).replace('"', '""')
