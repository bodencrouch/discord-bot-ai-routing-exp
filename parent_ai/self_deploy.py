"""Durable self-deployment handed to a separate systemd user service.

The copied runner uses only the standard library. Its code, proposal and original
bytes live alongside the parent database, outside the source tree it replaces.
"""

import asyncio
import base64
import fcntl
import hashlib
import http.cookiejar
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path

TERMINAL = {"done", "failed", "cancelled", "stale", "blocked", "launch_failed"}


class OwnershipChanged(RuntimeError):
    """A concurrent revision or cancellation won deployment ownership."""


class FinalizationPending(RuntimeError):
    """The job committed and its filesystem receipt still needs repair."""


PRIVATE_FIELDS = {
    "discord_token_file",
    "api_key_file",
    "token_file",
    "database",
    "dashboard_login_file",
}


def _directory(config):
    database = Path(config["database"]).expanduser().resolve()
    namespace = hashlib.sha256(str(database).encode()).hexdigest()[:16]
    return database.parent / "self-deploy" / namespace


def _transaction_paths(config):
    # This initial deployment format is namespaced by database from its first
    # supported release. No pre-release transaction format is resumed.
    return sorted(_directory(config).glob("*/transaction.json"))


def _shared_directory(config):
    return Path(config["target"]).expanduser().resolve() / ".parent-deployment"


def _register(path, record):
    shared = Path(record["root"]) / ".parent-deployment"
    shared.mkdir(mode=0o700, exist_ok=True)
    shared.chmod(0o700)
    identity = hashlib.sha256(str(path).encode()).hexdigest()
    _atomic(
        shared / "transactions" / (identity + ".json"),
        json.dumps({"manifest": str(path)}).encode(),
    )
    return shared


def _deployment_barrier(shared, own_path):
    if (shared / "blocked.json").exists():
        return (
            "A previous rollback requires operator recovery. Deployment queue stopped."
        )
    for entry in (shared / "transactions").glob("*.json"):
        try:
            other_path = Path(json.loads(entry.read_text())["manifest"])
            if other_path == own_path:
                continue
            other = json.loads(other_path.read_text())
        except (OSError, ValueError, KeyError):
            return "A deployment ownership record is unreadable. Operator recovery is required."
        if other["phase"] == "blocked":
            return "A previous rollback requires operator recovery. Deployment queue stopped."
        if other.get("install_started") and other["phase"] not in TERMINAL:
            raise RuntimeError(
                "An interrupted deployment must recover before another can install"
            )
    return None


def list_transactions(config):
    records = []
    for path in _transaction_paths(config):
        try:
            record = json.loads(path.read_text())
            records.append(
                inspect_transaction(config, record["job_id"], record["version"])
            )
        except (OSError, ValueError, KeyError):
            continue
    return records


def notification(config, job_id, version):
    path = _path(config, job_id, version).with_suffix(".delivery")
    if path.exists():
        return json.loads(path.read_text())
    identity = hashlib.sha256(str(path).encode()).hexdigest()[:20]
    record = {
        "marker": f"Deployment receipt `{identity}`",
        "nonce": str(int(identity[:15], 16)),
        "message_id": None,
    }
    _atomic(path, json.dumps(record).encode())
    return record


def save_notification(config, job_id, version, record):
    _atomic(
        _path(config, job_id, version).with_suffix(".delivery"),
        json.dumps(record).encode(),
    )


def notification_lock(config, job_id, version):
    return _path(config, job_id, version).with_suffix(".notification-lock").open("a")


def _path(config, job_id, version):
    return _directory(config) / f"{int(job_id)}-v{int(version)}" / "transaction.json"


def _atomic(path, data, mode=0o600):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, name = tempfile.mkstemp(prefix=".deploy-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchmod(stream.fileno(), mode)
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def _save(path, transaction, phase=None, note=None):
    if phase is not None:
        transaction["phase"] = phase
    if note is not None:
        transaction["note"] = note
    transaction["updated_at"] = time.time()
    _atomic(path, json.dumps(transaction, ensure_ascii=False).encode())


def inspect_transaction(config, job_id, version):
    path = _path(config, job_id, version)
    if not path.exists():
        return None
    try:
        record = json.loads(path.read_text())
        return {
            "phase": record["phase"],
            "terminal": record["phase"] in TERMINAL,
            "note": record.get("note", ""),
            "notified": path.with_suffix(".notified").exists(),
            "manifest": str(path),
            "unit": record["unit"],
            "job_id": record["job_id"],
            "version": record["version"],
            "created_at": record.get("created_at", 0),
        }
    except (ValueError, KeyError, OSError):
        return {
            "phase": "blocked",
            "terminal": True,
            "notified": False,
            "note": "Deployment record is unreadable. Operator recovery is required.",
            "manifest": str(path),
            "unit": "",
            "job_id": job_id,
            "version": version,
        }


status = inspect_transaction


def mark_notified(config, job_id, version):
    _atomic(_path(config, job_id, version).with_suffix(".notified"), b"notified\n")


def _command(*arguments, timeout=60):
    result = subprocess.run(
        arguments,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"{Path(arguments[0]).name} operation failed")
    return result.stdout.decode().strip()


def _active(unit):
    try:
        state = _command(
            "systemctl", "--user", "show", unit, "--property=ActiveState", "--value"
        )
        return state in {"active", "activating", "reloading", "deactivating"}
    except (OSError, subprocess.TimeoutExpired, RuntimeError):
        return False


async def _start(path):
    record = json.loads(path.read_text())
    _register(path, record)
    if await asyncio.to_thread(_active, record["unit"]):
        return
    # Never bind this service's lifetime to bot-hub's control group or unit.
    arguments = [
        "systemd-run",
        "--user",
        "--quiet",
        "--collect",
        "--unit=" + record["unit"],
        "--property=Type=exec",
        "--property=UMask=0077",
        "--property=Restart=on-failure",
        "--property=RestartSec=3",
        "--property=StandardOutput=null",
        "--property=StandardError=null",
        record["python"],
        str(path.parent / "runner.py"),
        "--run",
        str(path),
    ]
    try:
        await asyncio.to_thread(_command, *arguments, timeout=30)
    except (OSError, subprocess.TimeoutExpired, RuntimeError) as error:
        # Another launcher can have won the deterministic-unit race.
        if await asyncio.to_thread(_active, record["unit"]):
            return
        raise RuntimeError(
            "Could not launch the independent deployment watchdog. "
            "Check the user systemd manager and systemd-run availability. "
            "The durable transaction can be resumed after that is repaired."
        ) from error


async def launch(config, job_id, version, proposal):
    """Persist the reviewed transaction and launch its watchdog, then return.

    The caller must commit jobs.state='applying' at the supplied version first.
    No caller-side installation, rollback or hub restart should follow handoff.
    """
    from .changes import Changes

    path = _path(config, job_id, version)
    if path.exists():
        record = json.loads(path.read_text())
        if record["proposal_digest"] != _digest(proposal):
            raise RuntimeError("A different deployment already owns this revision.")
        if record["phase"] in TERMINAL:
            return inspect_transaction(config, job_id, version)
        await _start(path)
        return inspect_transaction(config, job_id, version)
    changes = Changes(config)
    try:
        changes.validate(proposal["plan"])
    finally:
        await changes.http.aclose()
    hub_config = config.get("hub_config") or os.environ.get("BOT_HUB_CONFIG")
    if not hub_config:
        raise RuntimeError("Self-deployment requires hub_config or BOT_HUB_CONFIG.")
    hub_config = str(Path(hub_config).expanduser().resolve())
    hub = json.loads(Path(hub_config).read_text())
    if not hub.get("dashboard_login_file"):
        raise RuntimeError(
            "The hub dashboard login file must be configured for deployment health checks."
        )
    private = _directory(config)
    private.mkdir(parents=True, exist_ok=True, mode=0o700)
    private.chmod(0o700)
    with (private / "launch.lock").open("a") as preparation_lock:
        fcntl.flock(preparation_lock, fcntl.LOCK_EX)
        if path.exists():
            existing = json.loads(path.read_text())
            if existing["proposal_digest"] != _digest(proposal):
                raise RuntimeError("A different deployment already owns this revision.")
        else:
            path.parent.mkdir(mode=0o700)
            runner = path.parent / "runner.py"
            _atomic(runner, Path(__file__).read_bytes())
            identifier = hashlib.sha256(str(private).encode()).hexdigest()[:12]
            record = {
                "schema": 2,
                "job_id": int(job_id),
                "version": int(version),
                "database": str(Path(config["database"]).expanduser().resolve()),
                "private_paths": sorted(str(item) for item in changes.private_paths),
                "root": str(Path(config["target"]).expanduser().resolve()),
                "hub_config": hub_config,
                "python": os.path.abspath(sys.executable),
                "uv": shutil.which("uv") or "uv",
                "unit": f"bot-hub-deploy-{identifier}-{int(job_id)}-v{int(version)}.service",
                "proposal": proposal,
                "proposal_digest": _digest(proposal),
                "phase": "prepared",
                "note": "Independent deployment watchdog prepared.",
                "created_at": time.time(),
                "install_started": False,
                "health_timeout": max(
                    30, min(600, int(config.get("deploy_health_timeout", 120)))
                ),
            }
            _save(path, record)
    if json.loads(path.read_text())["phase"] in TERMINAL:
        return inspect_transaction(config, job_id, version)
    # Preserve the prepared record if launching fails so recovery can retry it.
    await _start(path)
    return inspect_transaction(config, job_id, version)


async def recover(config):
    """Resume pending durable handoffs without duplicating active services."""
    records = []
    for path in _transaction_paths(config):
        try:
            record = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if record["phase"] not in TERMINAL:
            try:
                await _start(path)
            except (OSError, ValueError, RuntimeError):
                pass
        records.append(inspect_transaction(config, record["job_id"], record["version"]))
    return records


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def _editable(root, name, private_paths):
    path = Path(name)
    if path.is_absolute() or ".." in path.parts or not path.parts:
        return False
    if not (root / path).resolve().is_relative_to(root):
        return False
    if any(
        part.startswith(".") or part in {"__pycache__", "node_modules"}
        for part in path.parts
    ):
        return False
    absolute = root / path
    if str(absolute.resolve()) in private_paths or any(
        candidate.is_symlink()
        for candidate in (absolute, *absolute.parents)
        if candidate.is_relative_to(root)
    ):
        return False
    if any(
        word in name.lower()
        for word in ("token", "secret", "credential", ".sqlite", ".db")
    ):
        return False
    return path.suffix in {
        ".py",
        ".js",
        ".css",
        ".html",
        ".md",
        ".json",
        ".toml",
        ".lock",
        ".service",
        ".yml",
        ".yaml",
        ".txt",
    } or name in {"together-ai", "bot-hub"}


def _snapshot(root, private_paths):
    files = {}
    for directory, subdirs, names in os.walk(root):
        subdirs[:] = [
            name
            for name in subdirs
            if not name.startswith(".")
            and name not in {"__pycache__", "node_modules"}
            and not (Path(directory) / name).is_symlink()
        ]
        for name in names:
            path = Path(directory) / name
            relative = str(path.relative_to(root))
            if _editable(root, relative, private_paths) and not path.is_symlink():
                try:
                    files[relative] = path.read_text()
                except UnicodeError:
                    continue
    config = json.loads((root / "config.json").read_text())
    return {
        "files": files,
        "config": {
            key: value for key, value in config.items() if key not in PRIVATE_FIELDS
        },
    }


def _current(record):
    with sqlite3.connect(record["database"], timeout=30) as database:
        job = database.execute(
            "SELECT version,state FROM jobs WHERE id=?", (record["job_id"],)
        ).fetchone()
    return bool(job and job == (record["version"], "applying"))


def _finish(path, record, phase, note):
    """Commit the job and its receipt atomically, preserving concurrent edits."""
    if (
        phase in {"failed", "launch_failed"}
        and record.get("install_started")
        and not record.get("rollback_verified")
    ):
        phase = "blocked"
        note = "Rollback needs operator attention. Deployment queue stopped."
    elif phase in {"failed", "launch_failed"}:
        note += " The reviewed change will retry automatically; edit or cancel remains available."
    if phase == "blocked":
        shared = _register(path, record)
        _atomic(
            shared / "blocked.json",
            json.dumps({"manifest": str(path), "note": note}).encode(),
        )
    record["completion_phase"] = phase
    record["completion_note"] = note
    _save(path, record, "finalizing", note)
    with sqlite3.connect(record["database"], timeout=30) as database:
        database.row_factory = sqlite3.Row
        database.execute("BEGIN IMMEDIATE")
        database.execute(
            "CREATE TABLE IF NOT EXISTS self_deploy_receipts "
            "(transaction_path TEXT PRIMARY KEY, phase TEXT NOT NULL, note TEXT NOT NULL)"
        )
        receipt = database.execute(
            "SELECT phase,note FROM self_deploy_receipts WHERE transaction_path=?",
            (str(path),),
        ).fetchone()
        if receipt:
            phase, note = receipt["phase"], receipt["note"]
        else:
            previous = database.execute(
                "SELECT * FROM jobs WHERE id=? AND version=? AND state='applying'",
                (record["job_id"], record["version"]),
            ).fetchone()
            if not previous and phase == "done":
                # Source was applied, but cancellation won the final database race.
                # Keep the transaction recoverable until its source is rolled back.
                raise OwnershipChanged("Deployment ownership changed before completion")
            if previous:
                states = {
                    "blocked": "attention",
                    "stale": "queued",
                    "done": "done",
                    "cancelled": "cancelled",
                    "failed": "waiting",
                    "launch_failed": "waiting",
                }
                state = states[phase]
                retry = phase in {"failed", "launch_failed"}
                # A completed rollback owns a new handoff identity, retaining the
                # exact approved proposal. The old transaction stays terminal.
                new_version = record["version"] + (phase == "stale" or retry)
                columns = {
                    row[1] for row in database.execute("PRAGMA table_info(jobs)")
                }
                for name, definition in {
                    "next_retry": "REAL DEFAULT 0",
                    "recovery_target": "TEXT DEFAULT 'queued'",
                    "card_delivery_pending": "INTEGER DEFAULT 0",
                    "card_delivery_retry": "REAL DEFAULT 0",
                    "card_delivery_attempts": "INTEGER DEFAULT 0",
                    "card_generation": "INTEGER DEFAULT 0",
                    "card_retired": "INTEGER DEFAULT 0",
                }.items():
                    if name not in columns:
                        database.execute(
                            f"ALTER TABLE jobs ADD COLUMN {name} {definition}"
                        )
                database.execute(
                    "INSERT INTO audit(job,snapshot) VALUES(?,?)",
                    (record["job_id"], json.dumps(dict(previous))),
                )
                database.execute(
                    "UPDATE jobs SET state=?,version=?,note=?,next_retry=?,recovery_target=?,"
                    "card_delivery_pending=CASE WHEN card_retired=1 THEN 0 ELSE 1 END,"
                    "card_delivery_retry=0,card_delivery_attempts=0,"
                    "card_generation=COALESCE(card_generation,0)+1 "
                    "WHERE id=? AND version=? AND state='applying'",
                    (
                        state,
                        new_version,
                        note,
                        time.time() + 15 if retry else 0,
                        "approved" if retry else "queued",
                        record["job_id"],
                        record["version"],
                    ),
                )
            elif not previous and phase != "blocked":
                phase = "cancelled"
                note = (
                    "Deployment ownership changed. The newer job state was preserved."
                )
            database.execute(
                "INSERT INTO self_deploy_receipts(transaction_path,phase,note) VALUES(?,?,?)",
                (str(path), phase, note),
            )
    try:
        _save(path, record, phase, note)
    except OSError as error:
        raise FinalizationPending("Deployment receipt requires recovery") from error


def _dashboard(record):
    hub = json.loads(Path(record["hub_config"]).read_text())
    url = Path(hub["dashboard_login_file"]).read_text().strip()
    parsed = urllib.parse.urlsplit(url)
    if (
        parsed.scheme != "http"
        or parsed.hostname != "127.0.0.1"
        or parsed.path != "/login"
    ):
        raise RuntimeError("Deployment health requires a loopback dashboard login URL")
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()),
    )
    with opener.open(url, timeout=5) as response:
        response.read()
    endpoint = urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, "/api/state", "", "")
    )
    with opener.open(endpoint, timeout=10) as response:
        return json.load(response)


def _statuses(state):
    values = state.get("status", {})
    return (
        {str(entry["id"]): entry for entry in values}
        if isinstance(values, list)
        else values
    )


def _invocation():
    return _command(
        "systemctl",
        "--user",
        "show",
        "bot-hub.service",
        "--property=InvocationID",
        "--value",
    )


def _restart_and_verify(path, record, rollback=False):
    old_invocation = _invocation()
    _save(path, record, "rollback_restart" if rollback else "restarting")
    _command(record["uv"], "sync", "--locked", "--project", record["root"], timeout=180)
    _command("systemctl", "--user", "daemon-reload")
    _command("systemctl", "--user", "restart", "bot-hub.service", timeout=90)
    deadline = time.monotonic() + record["health_timeout"]
    _save(path, record, "rollback_verify" if rollback else "verifying")
    while time.monotonic() < deadline:
        if not rollback and not _current(record):
            raise RuntimeError("Deployment ownership changed")
        try:
            invocation = _invocation()
            state = _dashboard(record)
            statuses = _statuses(state)
            enabled = {
                str(key) for key, value in statuses.items() if value.get("enabled")
            }
            expected = set(record["expected_instances"]) | enabled
            healthy = bool(expected) and all(
                statuses.get(identity, {}).get("enabled")
                and statuses[identity].get("connected")
                and statuses[identity].get("running")
                and not statuses[identity].get("error")
                for identity in expected
            )
            if (
                invocation
                and invocation != old_invocation
                and _active("bot-hub.service")
                and healthy
            ):
                record["verified_invocation"] = invocation
                return
        except (OSError, ValueError, KeyError, RuntimeError, subprocess.TimeoutExpired):
            pass
        time.sleep(2)
    raise RuntimeError("Hub Discord readiness timed out")


def _backup(record, root):
    originals = {}
    for name in [*record["proposal"]["plan"]["files"], "config.json"]:
        path = root / name
        if name != "config.json" and (
            not _editable(root, name, record["private_paths"]) or path.is_symlink()
        ):
            raise RuntimeError("Deployment path validation failed")
        if path.is_symlink():
            raise RuntimeError("Deployment configuration must be a regular file")
        originals[name] = (
            None
            if not path.exists()
            else {
                "data": base64.b64encode(path.read_bytes()).decode(),
                "mode": path.stat().st_mode & 0o777,
            }
        )
    return originals


def _restore(record, root):
    for name, original in record["originals"].items():
        path = root / name
        if not path.resolve().is_relative_to(root) or path.is_symlink():
            raise RuntimeError("Rollback path validation failed")
        if original is None:
            path.unlink(missing_ok=True)
        else:
            _atomic(path, base64.b64decode(original["data"]), original["mode"])


def _run(path):
    os.umask(0o077)
    record = json.loads(path.read_text())
    if record["phase"] in TERMINAL:
        return
    shared = _register(path, record)
    lock_path = shared / "deployment.lock"
    with lock_path.open("a") as lock:
        _save(
            path,
            record,
            note="Independent deployment watchdog waiting for source ownership.",
        )
        fcntl.flock(lock, fcntl.LOCK_EX)
        record = json.loads(path.read_text())
        if record["phase"] in TERMINAL:
            return
        root = Path(record["root"]).resolve()
        barrier = _deployment_barrier(shared, path)
        if barrier:
            _finish(path, record, "blocked", barrier)
            return
        try:
            if record["phase"] == "finalizing":
                _finish(
                    path, record, record["completion_phase"], record["completion_note"]
                )
                return
            if record.get("install_started"):
                raise RuntimeError("Interrupted deployment requires rollback")
            if not _current(record):
                _finish(
                    path,
                    record,
                    "cancelled",
                    "Deployment cancelled before source changes.",
                )
                return
            _save(
                path,
                record,
                "validating",
                "Checking the approved source snapshot and connected Discord identities.",
            )
            if _digest(record["proposal"]) != record["proposal_digest"]:
                raise RuntimeError("The durable proposal changed")
            plan = record["proposal"]["plan"]
            if plan.get("questions") or set(plan["config"]) & PRIVATE_FIELDS:
                raise RuntimeError("The durable proposal failed validation")
            for name, content in plan["files"].items():
                if not _editable(root, name, record["private_paths"]) or not isinstance(
                    content, str
                ):
                    raise RuntimeError("The durable source proposal failed validation")
                if Path(name).suffix == ".py":
                    compile(content, name, "exec")
            if _digest(_snapshot(root, record["private_paths"])) != _digest(
                record["proposal"]["base"]
            ):
                _finish(
                    path,
                    record,
                    "stale",
                    "Source changed. Preparing a fresh approval preview.",
                )
                return
            statuses = _statuses(_dashboard(record))
            record["expected_instances"] = [
                str(key) for key, value in statuses.items() if value.get("enabled")
            ]
            if not record["expected_instances"]:
                raise RuntimeError(
                    "No enabled Discord identities were available for health verification"
                )
            record["originals"] = _backup(record, root)
            if _digest(_snapshot(root, record["private_paths"])) != _digest(
                record["proposal"]["base"]
            ):
                _finish(
                    path,
                    record,
                    "stale",
                    "Source changed. Preparing a fresh approval preview.",
                )
                return
            if not _current(record):
                _finish(
                    path,
                    record,
                    "cancelled",
                    "Deployment cancelled before source changes.",
                )
                return
            record["install_started"] = True
            _save(
                path, record, "installing", "Applying the reviewed source and settings."
            )
            proposal = record["proposal"]
            for name, content in proposal["plan"]["files"].items():
                original = record["originals"][name]
                _atomic(
                    root / name,
                    content.encode(),
                    original["mode"] if original else 0o644,
                )
            config = json.loads((root / "config.json").read_text())
            config.update(proposal["plan"]["config"])
            _atomic(
                root / "config.json", (json.dumps(config, indent=2) + "\n").encode()
            )
            _restart_and_verify(path, record)
            if not _current(record):
                raise RuntimeError("Deployment ownership changed")
            _finish(
                path,
                record,
                "done",
                "Applied. All enabled hub components reconnected to Discord successfully.",
            )
        except (
            OSError,
            ValueError,
            TypeError,
            KeyError,
            RuntimeError,
            SyntaxError,
            sqlite3.Error,
            subprocess.SubprocessError,
        ) as error:
            if isinstance(error, FinalizationPending) or (
                record["phase"] == "finalizing"
                and not isinstance(error, OwnershipChanged)
            ):
                # Restart the watchdog to replay the durable finalization receipt.
                raise
            record["failure_type"] = type(error).__name__
            record["failure_phase"] = record["phase"]
            if not record.get("install_started"):
                _finish(
                    path,
                    record,
                    "failed",
                    "Deployment could not pass source and dashboard preflight. No source files were changed.",
                )
                return
            try:
                _save(
                    path,
                    record,
                    "rolling_back",
                    "Deployment failed. Restoring the original source and settings.",
                )
                _restore(record, root)
                _restart_and_verify(path, record, rollback=True)
            except (
                OSError,
                ValueError,
                TypeError,
                KeyError,
                RuntimeError,
                sqlite3.Error,
                subprocess.SubprocessError,
            ) as rollback_error:
                record["rollback_failure_type"] = type(rollback_error).__name__
                _finish(
                    path,
                    record,
                    "blocked",
                    "Rollback needs operator attention. Deployment queue stopped.",
                )
                return
            record["rollback_verified"] = True
            _finish(
                path,
                record,
                "failed",
                "Deployment failed. Original source and settings were restored and all enabled hub components reconnected to Discord.",
            )


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "--run":
        raise SystemExit("Usage: runner.py --run TRANSACTION")
    _run(Path(sys.argv[2]).resolve())
