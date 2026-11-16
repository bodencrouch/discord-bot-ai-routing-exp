"""SQLite registry and JSON telemetry shared by the hub and Discord clients."""

import json
import re
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path


SECRET_FIELDS = {"token", "api_key", "password", "authorization", "secret"}


def sanitized(value):
    """Remove credential fields and recognizable bearer tokens from stored telemetry."""
    if isinstance(value, dict):
        return {
            str(key): sanitized(item)
            for key, item in value.items()
            if str(key).lower() not in SECRET_FIELDS
        }
    if isinstance(value, (list, tuple)):
        return [sanitized(item) for item in value]
    if isinstance(value, str):
        return re.sub(r"(?i)bearer\s+[a-z0-9._~+/=-]+", "Bearer [redacted]", value)
    return value


class State:
    """Synchronous, thread-safe store. All returned records are decoded dictionaries."""

    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS hub_instances (id TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS hub_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, instance_id TEXT NOT NULL,
                kind TEXT NOT NULL, payload TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
            CREATE INDEX IF NOT EXISTS hub_events_instance ON hub_events(instance_id, id);
            CREATE TABLE IF NOT EXISTS hub_predictions (
                id TEXT PRIMARY KEY, instance_id TEXT, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS hub_feedback (
                prediction_id TEXT, user_id TEXT, kind TEXT, data TEXT NOT NULL,
                PRIMARY KEY(prediction_id, user_id, kind));
        """)
        self.db.commit()

    def instances(self):
        """Return persisted instance overrides in stable id order."""
        with self.lock:
            return [
                json.loads(row[0])
                for row in self.db.execute("SELECT data FROM hub_instances ORDER BY id")
            ]

    def save_instance(self, instance):
        """Merge an instance patch by required id and return the saved record."""
        instance_id = str(instance["id"])
        with self.lock, self.db:
            row = self.db.execute(
                "SELECT data FROM hub_instances WHERE id=?", (instance_id,)
            ).fetchone()
            merged = json.loads(row[0]) if row else {}
            merged.update(instance, id=instance_id)
            self.db.execute(
                "INSERT INTO hub_instances VALUES (?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data",
                (instance_id, json.dumps(sanitized(merged))),
            )
            return sanitized(merged)

    def record(self, instance_id, kind, payload):
        """Append telemetry and return its integer event id."""
        with self.lock, self.db:
            cursor = self.db.execute(
                "INSERT INTO hub_events(instance_id,kind,payload) VALUES (?,?,?)",
                (
                    str(instance_id),
                    str(kind),
                    json.dumps(sanitized(payload), default=str),
                ),
            )
            return cursor.lastrowid

    def events(self, instance_id=None, limit=100):
        """Return newest-first events, optionally filtered to one instance."""
        query = "SELECT * FROM hub_events"
        args = []
        if instance_id is not None:
            query += " WHERE instance_id=?"
            args.append(str(instance_id))
        query += " ORDER BY id DESC"
        if limit is not None:
            query += " LIMIT ?"
            args.append(max(1, int(limit)))
        with self.lock:
            return [
                dict(
                    row,
                    payload=json.loads(row["payload"]),
                    created_at=row["created_at"].replace(" ", "T") + "+00:00",
                )
                for row in self.db.execute(query, args)
            ]

    def save_prediction(self, prediction=None, **fields):
        """Merge a flexible prediction record, assigning a UUID when id is absent."""
        patch = dict(prediction or {}, **fields)
        prediction_id = str(patch.get("id") or uuid.uuid4().hex)
        with self.lock, self.db:
            row = self.db.execute(
                "SELECT data FROM hub_predictions WHERE id=?", (prediction_id,)
            ).fetchone()
            data = (
                json.loads(row[0])
                if row
                else {"created_at": datetime.now(timezone.utc).isoformat()}
            )
            data.update(patch, id=prediction_id)
            data = sanitized(data)
            self.db.execute(
                "INSERT INTO hub_predictions VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET instance_id=excluded.instance_id,data=excluded.data",
                (
                    prediction_id,
                    str(data.get("instance_id", "")),
                    json.dumps(data, default=str),
                ),
            )
            return data

    def predictions(self, instance_id=None, limit=100):
        """Return newest-first predictions with optional instance filter."""
        query = "SELECT data FROM hub_predictions"
        args = []
        if instance_id is not None:
            query += " WHERE instance_id=?"
            args.append(str(instance_id))
        query += " ORDER BY rowid DESC"
        if limit is not None:
            query += " LIMIT ?"
            args.append(max(1, int(limit)))
        with self.lock:
            return [json.loads(row[0]) for row in self.db.execute(query, args)]

    def feedback(self, prediction_id, user_id, kind, value):
        """Upsert one person's feedback for a prediction and feedback kind."""
        data = sanitized(
            {
                "prediction_id": str(prediction_id),
                "user_id": str(user_id),
                "kind": str(kind),
                "value": value,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        with self.lock, self.db:
            self.db.execute(
                "INSERT INTO hub_feedback VALUES (?,?,?,?) ON CONFLICT(prediction_id,user_id,kind) DO UPDATE SET data=excluded.data",
                (
                    data["prediction_id"],
                    data["user_id"],
                    data["kind"],
                    json.dumps(data),
                ),
            )
        return data

    def prediction(self, prediction_id):
        """Fetch a prediction by id without scanning a bounded list."""
        with self.lock:
            row = self.db.execute(
                "SELECT data FROM hub_predictions WHERE id=?", (str(prediction_id),)
            ).fetchone()
            return json.loads(row[0]) if row else None

    def feedbacks(self, prediction_id=None, limit=500):
        """Return feedback, optionally restricted to a prediction. None removes limit."""
        query = "SELECT data FROM hub_feedback"
        args = []
        if prediction_id is not None:
            query += " WHERE prediction_id=?"
            args.append(str(prediction_id))
        query += " ORDER BY rowid DESC"
        if limit is not None:
            query += " LIMIT ?"
            args.append(max(1, int(limit)))
        with self.lock:
            return [json.loads(row[0]) for row in self.db.execute(query, args)]

    def analytics(self):
        """Aggregate all retained events, independent of dashboard paging."""
        with self.lock:
            counts = {
                row[0]: row[1]
                for row in self.db.execute(
                    "SELECT kind,COUNT(*) FROM hub_events GROUP BY kind"
                )
            }
            instances = {}
            for instance_id, kind, count in self.db.execute(
                "SELECT instance_id,kind,COUNT(*) FROM hub_events GROUP BY instance_id,kind"
            ):
                instances.setdefault(instance_id, {})[kind] = count
            return {
                "event_counts": counts,
                "events_total": sum(counts.values()),
                "instances": instances,
                "predictions_total": self.db.execute(
                    "SELECT COUNT(*) FROM hub_predictions"
                ).fetchone()[0],
                "feedback_total": self.db.execute(
                    "SELECT COUNT(*) FROM hub_feedback"
                ).fetchone()[0],
            }

    def close(self):
        with self.lock:
            self.db.close()
