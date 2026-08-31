"""
Turso embedded-replica sync for the product catalog SQLite DB.

``TursoReplicaConnection`` opens a single libsql connection for the whole
run. When Turso is enabled, that connection is a libSQL embedded replica
(``sync_url``/``auth_token``): reads are served locally, writes are forwarded
to the remote transparently as they happen, and ``.sync()`` at open time
pulls any remote changes made since the last run. No separate "push" step
is needed. Every repository/processor in the pipeline must use this same
connection for reads and writes -- writing through a different connection
(e.g. plain ``sqlite3.connect``) desyncs the replica's ``-info`` bookkeeping
and forces the next open to fall back to a full resync.
"""
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlparse

_module_logger = logging.getLogger(__name__)

BACKUP_DATE_FOLDER_FORMAT = "%Y_%m_%d"
BACKUP_RETENTION_COUNT = 10
_BACKUP_DIR_NAME_PATTERN = re.compile(r"^\d{4}_\d{2}_\d{2}$")
_REPLICA_SIDECAR_SUFFIXES = ("-info", "-wal", "-shm")


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


def describe_sync_url_for_logs(url: str | None) -> str:
    """Host/scheme for logs (no credentials)."""
    if not url:
        return "(none)"
    try:
        parsed = urlparse(str(url).strip())
        if parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        return str(url).strip()[:120]
    except Exception:
        return "(unparseable url)"


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


def flush_sqlite_to_disk(db_path: str, *, logger: logging.Logger | None = None) -> None:
    """
    Merge WAL into the main DB file and truncate the WAL so the on-disk file
    is a complete snapshot (important before copying it to a backup).
    """
    abs_path = os.path.abspath(db_path)
    if not os.path.exists(abs_path):
        return
    if logger is not None:
        logger.info("Checkpointing WAL into '%s'.", abs_path)
    deadline = time.monotonic() + 120.0
    while time.monotonic() < deadline:
        with sqlite3.connect(abs_path, timeout=120.0) as conn:
            row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if row is None or row[0] == 0:
                return
        time.sleep(0.05)
    raise RuntimeError(
        f"SQLite WAL checkpoint remained busy after waiting; '{abs_path}' may be locked."
    )


def _replica_files_present(db_path: str) -> bool:
    return os.path.exists(db_path) and os.path.exists(db_path + "-info")


def _remove_replica_files(db_path: str, *, logger: logging.Logger) -> None:
    removed = []
    for suffix in ("",) + _REPLICA_SIDECAR_SUFFIXES:
        path = db_path + suffix
        if os.path.exists(path):
            os.remove(path)
            removed.append(path)
    if removed:
        logger.info("Removed incomplete local replica file(s), a full resync will run: %s", removed)


def _backups_root_for_db(db_path: str) -> str:
    return os.path.join(os.path.dirname(os.path.abspath(db_path)), "backups")


def _prune_old_backups(backups_root: str, keep: int) -> None:
    if not os.path.isdir(backups_root):
        return
    dated_dirs = sorted(
        (
            name
            for name in os.listdir(backups_root)
            if _BACKUP_DIR_NAME_PATTERN.match(name)
            and os.path.isdir(os.path.join(backups_root, name))
        ),
        reverse=True,
    )
    for name in dated_dirs[keep:]:
        shutil.rmtree(os.path.join(backups_root, name), ignore_errors=True)


def _backup_replica_files(db_path: str, *, logger: logging.Logger) -> None:
    """Copy the (checkpointed) DB file and its -info sidecar into backups/yyyy_mm_dd/."""
    abs_path = os.path.abspath(db_path)
    backups_root = _backups_root_for_db(abs_path)
    date_folder_name = datetime.now().strftime(BACKUP_DATE_FOLDER_FORMAT)
    dest_dir = os.path.join(backups_root, date_folder_name)
    os.makedirs(dest_dir, exist_ok=True)
    copied = []
    for path in (abs_path, abs_path + "-info"):
        if os.path.exists(path):
            dest_path = os.path.join(dest_dir, os.path.basename(path))
            shutil.copy2(path, dest_path)
            copied.append(dest_path)
    logger.info("Copied database file(s) to backup folder '%s': %s", dest_dir, copied)
    _prune_old_backups(backups_root, BACKUP_RETENTION_COUNT)


_STALE_STREAM_MARKERS = ("stream not found", "stream_expired", "stream expired")


def _is_stale_stream_error(exc: Exception) -> bool:
    """True for Hrana errors meaning the replica's remote write stream has
    gone away (e.g. it sat idle through a long fetch/parse phase before its
    first write and Turso recycled it server-side). Reconnecting gets a
    fresh stream; the failed call is then safe to retry once."""
    message = str(exc).lower()
    return any(marker in message for marker in _STALE_STREAM_MARKERS)


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


def _connect_local(db_path: str):
    import libsql  # type: ignore

    return libsql.connect(db_path)


class TursoReplicaConnection:
    """Owns the single DB connection used for a whole pipeline run.

    Use as a context manager: opening pulls remote changes (or bootstraps a
    fresh full replica when the local state is missing/incomplete); closing
    checkpoints the WAL and backs up both DB files.

    ``connection`` returns this object itself: repositories call ``execute``/
    ``executemany`` on it exactly like a raw DB-API connection, but a call
    that fails because the remote Hrana stream went stale (idle too long
    between opening the connection and its first write) transparently
    reconnects and retries once instead of failing the whole run.
    """

    def __init__(
        self,
        db_path: str,
        config: TursoSyncConfiguration,
        logger: logging.Logger | None = None,
    ):
        self._db_path = db_path
        self._config = config
        self._logger = logger or _module_logger
        self._raw_connection = None

    @property
    def connection(self) -> "TursoReplicaConnection":
        return self

    def execute(self, *args, **kwargs):
        return self._with_stale_stream_retry(lambda conn: conn.execute(*args, **kwargs))

    def executemany(self, *args, **kwargs):
        return self._with_stale_stream_retry(lambda conn: conn.executemany(*args, **kwargs))

    def _with_stale_stream_retry(self, call):
        try:
            return call(self._raw_connection)
        except Exception as exc:
            if not _is_stale_stream_error(exc):
                raise
            self._logger.warning(
                "Turso remote stream went stale (%s); reconnecting and retrying once.", exc
            )
            self._reconnect()
            return call(self._raw_connection)

    def open(self) -> "TursoReplicaConnection":
        os.makedirs(os.path.dirname(self._db_path), exist_ok=True)

        if not self._config.enabled:
            self._logger.info(
                "Opening product catalog DB '%s' (Turso sync disabled, local only).",
                self._db_path,
            )
            if not os.path.exists(self._db_path):
                raise ValueError(f"SQLite database path {self._db_path} does not exist")
            self._raw_connection = _connect_local(self._db_path)
            return self

        if not self._config.is_ready:
            raise ValueError(
                f"Turso is enabled, but url/auth_token are missing in '{self._config.config_path}'."
            )

        self._logger.info(
            "Opening product catalog DB '%s' as a Turso embedded replica of %s.",
            self._db_path,
            describe_sync_url_for_logs(self._config.url),
        )

        if not _replica_files_present(self._db_path):
            self._logger.info(
                "Local replica files for '%s' are missing or incomplete; "
                "a full resync from Turso will run instead of an incremental sync.",
                self._db_path,
            )
            _remove_replica_files(self._db_path, logger=self._logger)

        self._raw_connection = self._connect_and_sync()
        return self

    def _reconnect(self) -> None:
        if self._raw_connection is not None:
            try:
                self._raw_connection.close()
            except Exception as exc:
                self._logger.debug("Ignoring error while closing stale connection: %s", exc)
        self._raw_connection = (
            self._connect_and_sync() if self._config.enabled else _connect_local(self._db_path)
        )

    def _connect_and_sync(self):
        self._logger.info("Syncing with Turso remote %s ...", describe_sync_url_for_logs(self._config.url))
        started = time.monotonic()
        connection = _connect_libsql(self._db_path, self._config.url, self._config.auth_token)
        connection.sync()
        self._logger.info(
            "Turso sync completed in %sms.",
            int((time.monotonic() - started) * 1000),
        )
        return connection

    def close(self) -> None:
        if self._raw_connection is None:
            return
        self._logger.info("Closing product catalog DB connection '%s'.", self._db_path)
        try:
            self._raw_connection.close()
        except Exception as exc:
            self._logger.debug("Ignoring error while closing replica connection: %s", exc)
        self._raw_connection = None
        flush_sqlite_to_disk(self._db_path, logger=self._logger)
        _backup_replica_files(self._db_path, logger=self._logger)

    def __enter__(self) -> "TursoReplicaConnection":
        return self.open()

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        self.close()
        return False
