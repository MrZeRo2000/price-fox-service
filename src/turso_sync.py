"""
Turso embedded-replica sync for the product catalog SQLite DB.

Before calling ``pull_from_remote``, callers that have an existing local DB file
should run ``backup_sqlite_before_cloud_pull`` so a known-good copy exists if
remote data is bad (see ``main``).
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

from config.settings import default_product_catalog_db_path

logger = logging.getLogger(__name__)

BACKUP_DATE_FOLDER_FORMAT = "%Y_%m_%d"
BACKUP_RETENTION_COUNT = 10
_BACKUP_DIR_NAME_PATTERN = re.compile(r"^\d{4}_\d{2}_\d{2}$")
# Retries after the first attempt (4 tries total) for transient Turso/network failures.
TURSO_SYNC_RETRY_COUNT = 3


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


def flush_sqlite_to_disk(db_path: str) -> None:
    """
    Merge WAL into the main DB file and truncate the WAL so the on-disk file
    is a complete snapshot (important before Turso upload or file copies).
    """
    abs_path = os.path.abspath(db_path)
    if not os.path.exists(abs_path):
        return
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


def backup_sqlite_before_cloud_pull(db_path: str) -> dict:
    """
    Copy the local DB into backups/yyyy_mm_dd/<filename>, overwriting if the
    same calendar-day folder already exists. Then drop dated folders beyond
    the retention limit (newest first).

    Call this before any Turso pull/sync so local data survives if the remote
    side is wrong or corrupted.
    """
    abs_path = os.path.abspath(db_path)
    if not os.path.exists(abs_path):
        return {"status": "skipped", "reason": "no_local_file"}

    flush_sqlite_to_disk(abs_path)
    backups_root = _backups_root_for_db(abs_path)
    date_folder_name = datetime.now().strftime(BACKUP_DATE_FOLDER_FORMAT)
    dest_dir = os.path.join(backups_root, date_folder_name)
    os.makedirs(dest_dir, exist_ok=True)
    dest_file = os.path.join(dest_dir, os.path.basename(abs_path))
    shutil.copy2(abs_path, dest_file)
    _prune_old_backups(backups_root, BACKUP_RETENTION_COUNT)
    return {
        "status": "success",
        "backup_path": dest_file,
        "backups_root": backups_root,
    }


class TursoSyncClient:
    def __init__(self, config: TursoSyncConfiguration, db_path: str | None = None):
        self._config = config
        self._db_path = default_product_catalog_db_path() if db_path is None else db_path

    @property
    def db_path(self) -> str:
        return self._db_path

    def pull_from_remote(self) -> dict:
        """
        Sync remote Turso state into the local embedded replica.

        Does not create a local backup; call ``backup_sqlite_before_cloud_pull``
        first when ``db_path`` already exists (``main`` does this before pull).
        """
        logger.info(
            "Turso pull starting: db_path=%s remote=%s config=%s",
            self._db_path,
            describe_sync_url_for_logs(self._config.url),
            self._config.config_path,
        )
        result = self._sync(direction="pull")
        if result.get("status") == "success":
            logger.info(
                "Turso pull finished: mode=%s db_path=%s",
                result.get("mode"),
                result.get("db_path"),
            )
        else:
            logger.warning(
                "Turso pull did not complete: status=%s direction=%s reason=%s",
                result.get("status"),
                result.get("direction"),
                result.get("reason"),
            )
        return result

    def push_to_remote(self) -> dict:
        logger.info(
            "Turso push starting (replace remote with local): db_path=%s remote=%s config=%s",
            self._db_path,
            describe_sync_url_for_logs(self._config.url),
            self._config.config_path,
        )
        result = self.replace_remote_with_local()
        if result.get("status") == "success":
            logger.info(
                "Turso push finished: mode=%s db_path=%s",
                result.get("mode"),
                result.get("db_path"),
            )
        else:
            logger.warning(
                "Turso push did not complete: status=%s reason=%s",
                result.get("status"),
                result.get("reason"),
            )
        return result

    def replace_remote_with_local(self) -> dict:
        """
        Force remote Turso DB to match local SQLite exactly.
        Drops remote user objects and uploads full local dump.
        """
        if not self._config.enabled:
            logger.warning(
                "Turso replace_remote_with_local skipped: enabled=false config=%s",
                self._config.config_path,
            )
            return {
                "status": "skipped",
                "direction": "push",
                "reason": "disabled",
            }
        if not self._config.is_ready:
            logger.error(
                "Turso replace_remote_with_local refused: incomplete config %s",
                self._config.config_path,
            )
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
            logger.warning(
                "Turso sync skipped: enabled=false direction=%s config=%s",
                direction,
                self._config.config_path,
            )
            return {
                "status": "skipped",
                "direction": direction,
                "reason": "disabled",
            }
        if not self._config.is_ready:
            logger.error(
                "Turso sync refused: missing url or auth_token direction=%s config=%s",
                direction,
                self._config.config_path,
            )
            raise ValueError(
                f"Turso is enabled, but url/auth_token are missing in '{self._config.config_path}'."
            )

        os.makedirs(os.path.dirname(self._db_path), exist_ok=True)

        sync_mode = "replica_sync"
        connection = None
        started = time.monotonic()
        try:
            try:
                logger.debug(
                    "Turso opening libsql replica: db_path=%s remote=%s",
                    self._db_path,
                    describe_sync_url_for_logs(self._config.url),
                )
                connection = _connect_libsql(
                    db_path=self._db_path,
                    sync_url=self._config.url,
                    auth_token=self._config.auth_token,
                )
                # libSQL sync is bi-directional; we expose pull/push wrappers for pipeline intent.
                logger.debug("Turso calling connection.sync() direction=%s", direction)
                connection.sync()
            except Exception as exc:
                elapsed_ms = int((time.monotonic() - started) * 1000)
                if direction == "push" and _is_missing_replica_metadata_error(exc):
                    logger.warning(
                        "Turso replica metadata missing after %sms; falling back to direct upload: %s",
                        elapsed_ms,
                        exc,
                    )
                    _push_local_sqlite_to_remote(
                        db_path=self._db_path,
                        sync_url=self._config.url,
                        auth_token=self._config.auth_token,
                    )
                    sync_mode = "direct_upload"
                else:
                    logger.error(
                        "Turso %s sync failed after %sms: %s",
                        direction,
                        elapsed_ms,
                        exc,
                        exc_info=True,
                    )
                    raise RuntimeError(
                        f"Turso {direction} sync failed: {exc}"
                    ) from exc
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception as close_exc:
                    logger.debug("Turso connection.close() ignored: %s", close_exc)

        elapsed_ms = int((time.monotonic() - started) * 1000)
        logger.info(
            "Turso libsql sync OK: direction=%s mode=%s db_path=%s elapsed_ms=%s",
            direction,
            sync_mode,
            self._db_path,
            elapsed_ms,
        )

        return {
            "status": "success",
            "direction": direction,
            "mode": sync_mode,
            "db_path": self._db_path,
            "remote_url": self._config.url,
        }


def _log_turso_sync_event(
    *, logger: logging.Logger, event: str, phase: str, operation: str, **payload
) -> None:
    details = ", ".join(f"{key}={value!r}" for key, value in sorted(payload.items()))
    if details:
        logger.info(
            "Turso sync event=%s phase=%s operation=%s %s",
            event,
            phase,
            operation,
            details,
        )
        return
    logger.info("Turso sync event=%s phase=%s operation=%s", event, phase, operation)


def _local_backup_note_before_turso_pull(db_path: str) -> str:
    """If the catalog DB exists, back it up and return a log suffix; else ``''``."""
    if not os.path.exists(db_path):
        return ""
    backup = backup_sqlite_before_cloud_pull(db_path)
    if backup.get("status") == "success":
        return f" Local backup: '{backup['backup_path']}'."
    return ""


def _turso_pull_with_retries(
    turso_sync_client: TursoSyncClient,
    *,
    phase: str,
    logger: logging.Logger,
) -> dict:
    total_attempts = 1 + TURSO_SYNC_RETRY_COUNT
    last_exc: Exception | None = None
    last_result: dict | None = None
    for i in range(total_attempts):
        attempt = i + 1
        try:
            result = turso_sync_client.pull_from_remote()
        except Exception as exc:
            last_exc = exc
            last_result = None
            logger.warning(
                "Turso %s pull raised (attempt %s/%s): %s",
                phase,
                attempt,
                total_attempts,
                exc,
            )
            if attempt < total_attempts:
                delay = min(2**i, 30)
                logger.info("Retrying Turso %s pull in %s second(s).", phase, delay)
                time.sleep(delay)
            continue
        last_exc = None
        last_result = result
        if result.get("status") == "success":
            return result
        if result.get("status") == "skipped":
            return result
        logger.warning(
            "Turso %s pull non-success (attempt %s/%s): %s",
            phase,
            attempt,
            total_attempts,
            result,
        )
        if attempt < total_attempts:
            delay = min(2**i, 30)
            logger.info("Retrying Turso %s pull in %s second(s).", phase, delay)
            time.sleep(delay)
    if last_exc is not None:
        logger.error("Turso %s pull failed after %s attempts.", phase, total_attempts)
        raise last_exc
    assert last_result is not None
    return last_result


def _turso_push_with_retries(
    turso_sync_client: TursoSyncClient,
    *,
    phase: str,
    logger: logging.Logger,
) -> dict:
    total_attempts = 1 + TURSO_SYNC_RETRY_COUNT
    last_exc: Exception | None = None
    last_result: dict | None = None
    for i in range(total_attempts):
        attempt = i + 1
        try:
            result = turso_sync_client.push_to_remote()
        except Exception as exc:
            last_exc = exc
            last_result = None
            logger.warning(
                "Turso %s push raised (attempt %s/%s): %s",
                phase,
                attempt,
                total_attempts,
                exc,
            )
            if attempt < total_attempts:
                delay = min(2**i, 30)
                logger.info("Retrying Turso %s push in %s second(s).", phase, delay)
                time.sleep(delay)
            continue
        last_exc = None
        last_result = result
        if result.get("status") == "success":
            return result
        if result.get("status") == "skipped":
            return result
        logger.warning(
            "Turso %s push non-success (attempt %s/%s): %s",
            phase,
            attempt,
            total_attempts,
            result,
        )
        if attempt < total_attempts:
            delay = min(2**i, 30)
            logger.info("Retrying Turso %s push in %s second(s).", phase, delay)
            time.sleep(delay)
    if last_exc is not None:
        logger.error("Turso %s push failed after %s attempts.", phase, total_attempts)
        raise last_exc
    assert last_result is not None
    return last_result


def _require_turso_pull_success(
    pull_result: dict,
    *,
    phase: str,
    db_path: str,
    turso_config: TursoSyncConfiguration,
    logger: logging.Logger,
) -> None:
    if pull_result.get("status") == "success":
        return
    reason = pull_result.get("reason", "unknown")
    msg = (
        f"Turso {phase} pull was required but did not succeed "
        f"(status={pull_result.get('status')}, reason={reason}). "
        f"Local catalog DB path: '{db_path}'. "
        f"Update '{turso_config.config_path}' (set enabled=true and valid url/auth_token), "
        f"or pass --config-path to use a JSON catalog and skip the SQLite DB."
    )
    logger.error(msg)
    raise RuntimeError(msg)


def _require_turso_push_success(
    push_result: dict, *, turso_config: TursoSyncConfiguration, logger: logging.Logger
) -> None:
    if push_result.get("status") == "success":
        return
    reason = push_result.get("reason", "unknown")
    msg = (
        f"Turso push was required but did not complete "
        f"(status={push_result.get('status')}, reason={reason}). "
        f"Config: '{turso_config.config_path}'. "
        f"Fix Turso settings or resolve the error above so the local DB is pushed to the remote."
    )
    logger.error(msg)
    raise RuntimeError(msg)


def bootstrap_turso_pull_if_missing(
    *,
    turso_sync_client: TursoSyncClient,
    db_path: str,
    turso_config: TursoSyncConfiguration,
    logger: logging.Logger,
) -> bool:
    """Pull from Turso when local DB file is missing. Returns whether a pull ran."""
    if os.path.exists(db_path):
        return False

    _log_turso_sync_event(
        logger=logger,
        event="start",
        phase="bootstrap",
        operation="pull",
        db_path=db_path,
    )
    logger.info(
        "Local catalog DB is missing at '%s'. Attempting bootstrap pull from Turso.",
        db_path,
    )
    pull_result = _turso_pull_with_retries(
        turso_sync_client, phase="bootstrap", logger=logger
    )
    _log_turso_sync_event(
        logger=logger,
        event="progress",
        phase="bootstrap",
        operation="pull",
        status=pull_result.get("status"),
        direction=pull_result.get("direction"),
        mode=pull_result.get("mode", "unknown"),
    )
    if pull_result["status"] == "success":
        logger.info(
            "Turso bootstrap pull completed (%s) for DB '%s' via mode '%s'.",
            pull_result["direction"],
            pull_result["db_path"],
            pull_result.get("mode", "unknown"),
        )
    _require_turso_pull_success(
        pull_result,
        phase="bootstrap",
        db_path=db_path,
        turso_config=turso_config,
        logger=logger,
    )
    if not os.path.exists(db_path):
        raise RuntimeError(
            f"Turso bootstrap pull reported success but local DB is still missing at '{db_path}'."
        )
    _log_turso_sync_event(
        logger=logger,
        event="completed",
        phase="bootstrap",
        operation="pull",
        db_path=db_path,
    )
    return True


def run_turso_pre_sync_pull(
    *,
    turso_sync_client: TursoSyncClient,
    db_path: str,
    turso_config: TursoSyncConfiguration,
    logger: logging.Logger,
) -> None:
    _log_turso_sync_event(
        logger=logger,
        event="start",
        phase="pre-run",
        operation="pull",
        db_path=db_path,
    )
    backup_note = _local_backup_note_before_turso_pull(db_path)
    pull_result = _turso_pull_with_retries(turso_sync_client, phase="pre-run", logger=logger)
    _log_turso_sync_event(
        logger=logger,
        event="progress",
        phase="pre-run",
        operation="pull",
        status=pull_result.get("status"),
        direction=pull_result.get("direction"),
        mode=pull_result.get("mode", "unknown"),
    )
    if pull_result["status"] == "success":
        logger.info(
            "Turso pre-sync completed (%s) for DB '%s' via mode '%s'.%s",
            pull_result["direction"],
            pull_result["db_path"],
            pull_result.get("mode", "unknown"),
            backup_note,
        )
    _require_turso_pull_success(
        pull_result,
        phase="pre-run",
        db_path=db_path,
        turso_config=turso_config,
        logger=logger,
    )
    _log_turso_sync_event(
        logger=logger,
        event="completed",
        phase="pre-run",
        operation="pull",
        db_path=db_path,
    )


def run_turso_post_sync_push(
    *,
    turso_sync_client: TursoSyncClient,
    turso_config: TursoSyncConfiguration,
    logger: logging.Logger,
) -> None:
    _log_turso_sync_event(
        logger=logger,
        event="start",
        phase="post-run",
        operation="push",
        db_path=turso_sync_client.db_path,
    )
    push_result = _turso_push_with_retries(turso_sync_client, phase="post-run", logger=logger)
    _log_turso_sync_event(
        logger=logger,
        event="progress",
        phase="post-run",
        operation="push",
        status=push_result.get("status"),
        direction=push_result.get("direction"),
        mode=push_result.get("mode", "unknown"),
    )
    if push_result["status"] == "success":
        logger.info(
            "Turso post-sync completed (%s) for DB '%s' via mode '%s'.",
            push_result["direction"],
            push_result["db_path"],
            push_result.get("mode", "unknown"),
        )
    _require_turso_push_success(
        push_result,
        turso_config=turso_config,
        logger=logger,
    )
    _log_turso_sync_event(
        logger=logger,
        event="completed",
        phase="post-run",
        operation="push",
        db_path=turso_sync_client.db_path,
    )


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

    flush_sqlite_to_disk(db_path)

    try:
        import libsql  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "libsql is required for Turso upload. Install with: "
            f"'{sys.executable} -m pip install libsql'."
        ) from exc

    logger.info(
        "Turso direct upload starting: db_path=%s remote=%s",
        db_path,
        describe_sync_url_for_logs(sync_url),
    )
    started = time.monotonic()
    local_conn = sqlite3.connect(db_path)
    remote_conn = libsql.connect(sync_url, auth_token=auth_token)
    statements_run = 0
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
            statements_run += 1

        remote_conn.commit()
    except Exception as exc:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        logger.error(
            "Turso direct upload failed after %sms (%s statements applied): %s",
            elapsed_ms,
            statements_run,
            exc,
            exc_info=True,
        )
        raise RuntimeError(f"Failed to upload local SQLite DB to Turso: {exc}") from exc
    finally:
        remote_conn.close()
        local_conn.close()

    elapsed_ms = int((time.monotonic() - started) * 1000)
    logger.info(
        "Turso direct upload finished: statements=%s elapsed_ms=%s",
        statements_run,
        elapsed_ms,
    )


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
