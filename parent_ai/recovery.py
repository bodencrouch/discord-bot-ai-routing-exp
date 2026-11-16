"""Private planning checkpoints and bot-authored activity recovery."""

import json
import sqlite3
import threading
from datetime import datetime, timezone

import discord


class SessionStore:
    """Keep resumable planning state alongside the existing parent job tables."""

    def __init__(self, parent_database_path):
        self.lock = threading.RLock()
        self.db = sqlite3.connect(parent_database_path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute(
            """CREATE TABLE IF NOT EXISTS planning_sessions (
                job_id TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                activity_message_id TEXT,
                updatedat TEXT NOT NULL
            )"""
        )
        self.db.commit()

    def save(self, job_id, state, activity_message_id=None):
        """Atomically replace a checkpoint, retaining an omitted activity marker."""
        encoded = json.dumps(state)
        updated = datetime.now(timezone.utc).isoformat()
        marker = str(activity_message_id) if activity_message_id is not None else None
        with self.lock, self.db:
            self.db.execute(
                """INSERT INTO planning_sessions
                    (job_id, data, activity_message_id, updatedat)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(job_id) DO UPDATE SET
                        data=excluded.data,
                        activity_message_id=COALESCE(
                            excluded.activity_message_id,
                            planning_sessions.activity_message_id
                        ),
                        updatedat=excluded.updatedat""",
                (str(job_id), encoded, marker, updated),
            )

    def load(self, job_id):
        """Return the last saved state and its Discord activity marker."""
        with self.lock:
            row = self.db.execute(
                "SELECT data, activity_message_id FROM planning_sessions WHERE job_id=?",
                (str(job_id),),
            ).fetchone()
        if row is None:
            return None
        return {"state": json.loads(row[0]), "activity_message_id": row[1]}

    def clear(self, job_id):
        with self.lock, self.db:
            self.db.execute(
                "DELETE FROM planning_sessions WHERE job_id=?", (str(job_id),)
            )

    def close(self):
        with self.lock:
            self.db.close()


async def rewind_activity(bot, job_id, activity_message_id):
    """Delete this bot's later activity while retaining humans and the checkpoint.

    Resolve the job's actual thread and use the numeric checkpoint as a stable
    chronological boundary, even if its message was deleted. Discord's history
    iterator paginates through all later items.
    Permission and transport failures propagate so callers can report recovery
    failure without silently claiming a successful rewind.
    """
    if activity_message_id is None or bot.user is None:
        return 0
    job = bot.get(job_id)
    if job is None or not job["thread"]:
        return 0
    try:
        checkpoint_id = int(activity_message_id)
        thread_id = int(job["thread"])
    except (ValueError, TypeError):
        return 0
    if checkpoint_id <= 0 or thread_id <= 0:
        return 0
    try:
        thread = bot.get_channel(thread_id) or await bot.fetch_channel(thread_id)
    except discord.NotFound:
        return 0
    deleted = 0
    async for message in thread.history(
        limit=None, after=discord.Object(id=checkpoint_id), oldest_first=True
    ):
        if message.id <= checkpoint_id or message.author.id != bot.user.id:
            continue
        is_system = getattr(message, "is_system", None)
        message_type = getattr(message, "type", None)
        if (callable(is_system) and is_system()) or message_type in (
            discord.MessageType.recipient_add,
            discord.MessageType.recipient_remove,
            discord.MessageType.thread_starter_message,
        ):
            continue
        try:
            await message.delete()
        except discord.NotFound:
            continue
        except discord.HTTPException as error:
            if error.code == 50021:
                continue
            raise
        deleted += 1
    return deleted
