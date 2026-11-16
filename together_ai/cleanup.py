"""Reviewed, administrator-initiated Discord server cleanup."""

import asyncio
import json
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import discord
from discord import app_commands


SCOPE = "All server messages, including members’ messages, and managed AI threads"


def permissions_url(client_id, guild_id):
    permissions = discord.Permissions(
        view_channel=True,
        send_messages=True,
        send_messages_in_threads=True,
        create_public_threads=True,
        read_message_history=True,
        embed_links=True,
        attach_files=True,
        manage_messages=True,
        manage_threads=True,
    )
    return "https://discord.com/oauth2/authorize?" + urlencode(
        {
            "client_id": str(client_id),
            "guild_id": str(guild_id),
            "disable_guild_select": "true",
            "scope": "bot applications.commands",
            "permissions": str(permissions.value),
        }
    )


class CleanupManager:
    def __init__(self, runtime, state):
        self.runtime, self.state = runtime, state
        self.locks = {}
        self.ui_locks = {}
        self.running = set()
        self.reservations = {}  # guild_id -> snapshot_id claimed but maybe not yet run
        self.tasks = {}
        self.active_runs = set()
        with state.lock, state.db:
            state.db.execute(
                "CREATE TABLE IF NOT EXISTS hub_cleanups (id TEXT PRIMARY KEY,guild_id TEXT,data TEXT)"
            )
            # Interrupted requests can be safely retried against the same message IDs.
            for row in state.db.execute("SELECT id,data FROM hub_cleanups").fetchall():
                data = json.loads(row["data"])
                if data.get("status") == "running":
                    data.update(status="interrupted", running=False)
                    state.db.execute(
                        "UPDATE hub_cleanups SET data=? WHERE id=?",
                        (json.dumps(data), row["id"]),
                    )

    def is_running(self, guild_id):
        return str(guild_id) in self.running

    # Control messages are tracked so consumed or superseded previews retire
    # (disabled) even across restarts. Snapshot IDs never change.
    def _controls(self, guild_id):
        with self.state.lock:
            self.state.db.execute(
                "CREATE TABLE IF NOT EXISTS hub_cleanup_controls (message_id TEXT PRIMARY KEY,guild_id TEXT,channel_id TEXT,snapshot_id TEXT,owner_id TEXT,state TEXT DEFAULT 'active')"
            )
            return [
                dict(row)
                for row in self.state.db.execute(
                    "SELECT * FROM hub_cleanup_controls WHERE guild_id=?",
                    (str(guild_id),),
                ).fetchall()
            ]

    def track_control(self, message, snapshot_id=None, guild_id=None):
        """Persist regular or webhook control cards with their owning bot."""
        channel = getattr(message, "channel", None)
        channel_id = getattr(channel, "id", None) or getattr(
            message, "channel_id", None
        )
        guild = getattr(message, "guild", None) or getattr(channel, "guild", None)
        if guild_id is None and guild is not None:
            guild_id = guild.id
        if guild_id is None and channel_id is not None:
            for client in self.runtime.clients.values():
                cached = client.get_channel(int(channel_id))
                cached_guild = getattr(cached, "guild", None)
                if cached_guild is not None:
                    guild_id = cached_guild.id
                    break
        if guild_id is None:
            raise ValueError("A guild ID is required to track this cleanup control.")
        if channel_id is None:
            raise ValueError("A channel ID is required to track this cleanup control.")
        self._controls(guild_id)
        with self.state.lock, self.state.db:
            self.state.db.execute(
                "INSERT OR REPLACE INTO hub_cleanup_controls VALUES (?,?,?,?,?,'active')",
                (
                    str(message.id),
                    str(guild_id),
                    str(channel_id),
                    snapshot_id,
                    str(message.author.id),
                ),
            )

    def _set_control_state(self, message_id, state):
        with self.state.lock, self.state.db:
            if state == "gone":
                self.state.db.execute(
                    "DELETE FROM hub_cleanup_controls WHERE message_id=?",
                    (str(message_id),),
                )
            else:
                self.state.db.execute(
                    "UPDATE hub_cleanup_controls SET state=? WHERE message_id=?",
                    (state, str(message_id)),
                )

    def _owner_client(self, owner_id):
        for client in self.runtime.clients.values():
            if client.user and str(client.user.id) == str(owner_id):
                return client
        return None

    async def refresh_control(self, row, label=None, retire=True):
        """Edit one card through its own bot identity. Returns True when settled."""
        client = self._owner_client(row.get("owner_id"))
        if client is None or not client.is_ready():
            return False  # Owner offline: keep tracking for a later retry.
        try:
            channel = client.get_channel(int(row["channel_id"]))
            if channel is None:
                channel = await client.fetch_channel(int(row["channel_id"]))
            message = await channel.fetch_message(int(row["message_id"]))
            if retire:
                view = discord.ui.View.from_message(message, timeout=None)
                for item in list(view.children):
                    if getattr(item, "url", None):
                        view.remove_item(item)  # Obsolete permission links go.
                    else:
                        item.disabled = True
                        if label:
                            item.label = label
                await message.edit(view=view)
                self._set_control_state(row["message_id"], "retired")
            else:
                await message.edit(view=status_view(client, self.runtime))
            return True
        except discord.NotFound:
            self._set_control_state(row["message_id"], "gone")
            return True
        except (discord.HTTPException, AttributeError):
            return False  # 403/5xx: retain tracking so a retry can settle it.

    async def retire_controls(self, guild_id, keep=None, label=None):
        """Retire every active control card except `keep`, via each owner bot."""
        for row in self._controls(guild_id):
            if row["message_id"] == str(keep) or row.get("state") == "retired":
                continue
            await self.refresh_control(row, label=label)

    def capable_client(self, guild_id):
        """The connected identity that will execute cleanup: the one with the
        most cleanup permissions. Cleanup is a shared operation, so a grant on
        ANY capable connected bot is enough; no per-identity grant is needed."""
        clients = self.clients(guild_id)
        if not clients:
            return None
        return max(
            clients,
            key=lambda item: (
                item.get_guild(int(guild_id)).me.guild_permissions.manage_threads,
                item.get_guild(int(guild_id)).me.guild_permissions.manage_messages,
            ),
        )

    @staticmethod
    def required_permissions(data):
        """Derive what a snapshot NEEDS from its immutable rooms, independent
        of what was held at review time: any message requires Manage Messages,
        any AI thread requires Manage Threads. A snapshot reviewed while the
        permissions were held therefore still reports a deficit after
        revocation (legacy/empty required_permissions fields are repaired)."""
        rooms = data.get("rooms") or []
        needs = []
        if any(room.get("messages") for room in rooms):
            needs.append("Manage Messages")
        if any(room.get("ai_thread") for room in rooms):
            needs.append("Manage Threads")
        if "rooms" not in data:  # Public copies retain their persisted needs.
            needs = list(data.get("required_permissions") or [])
        for gap in data.get("read_gaps", []):
            if (
                "thread" in gap.get("reason", "").lower()
                and "Manage Threads" not in needs
            ):
                needs.append("Manage Threads")
        return needs

    def permission_deficit(self, guild_id, data):
        """Recompute which cleanup permissions the executing bot lacks, from
        the snapshot's derived needs. Recomputing (not filtering the old
        missing list) catches permissions revoked since the review."""
        needs = self.required_permissions(data)
        client = self.capable_client(guild_id)
        if client is None:
            return list(needs)
        perms = client.get_guild(int(guild_id)).me.guild_permissions
        held = {
            "Manage Messages": perms.manage_messages,
            "Manage Threads": perms.manage_threads,
        }
        return [name for name in needs if not held.get(name, True)]

    async def revalidate(self, data):
        """Recheck real access for a saved preview and persist the result so
        cards and `claim` agree on one validated state.

        * missing_permissions is RECOMPUTED against the capable bot (grants and
          revocations both reflected).
        * read_gaps are NEVER cleared here. The rooms list is immutable; a
          permission flag becoming readable does not populate the messages or
          private threads that were missed. Those gaps clear only when a fresh
          review inventories the scope. The card shows "Review again" instead
          of a permission link once the underlying access is actually held.
        * permission_urls are rebuilt to point only at the capable bot and only
          while a real permission deficit exists."""
        guild_id = data["guild_id"]
        client = self.capable_client(guild_id)
        if client is None:
            return data
        data["required_permissions"] = self.required_permissions(data)
        data["missing_permissions"] = self.permission_deficit(guild_id, data)
        # A read gap that needed Manage Threads is "access granted" once the
        # capable bot holds it; it still requires a fresh review to inventory.
        perms = client.get_guild(int(guild_id)).me.guild_permissions
        for gap in data.get("read_gaps", []):
            gap["access_granted"] = bool(
                perms.manage_threads
                if "Manage Threads" in gap["reason"]
                else self._room_readable(client, guild_id, gap["channel_id"])
            )
        data["permission_urls"] = (
            [
                {
                    "name": client.user.display_name,
                    "url": permissions_url(client.user.id, guild_id),
                }
            ]
            if data["missing_permissions"]
            else []
        )
        self._save(data)
        return data

    def _room_readable(self, client, guild_id, channel_id):
        guild = client.get_guild(int(guild_id))
        channel = guild.get_channel_or_thread(int(channel_id))
        if channel is None:
            return False
        room_perms = channel.permissions_for(guild.me)
        return bool(room_perms.view_channel and room_perms.read_message_history)

    async def refresh_permission_cards(self, guild_id):
        """Rebuild active cards when permissions change or on startup, using
        the same validated snapshot state `start` will check."""
        for row in self._controls(guild_id):
            if row.get("state") == "retired":
                continue
            if row.get("snapshot_id"):
                snapshot = self._get(row["snapshot_id"])
                if snapshot and snapshot.get("status") in {
                    "preview",
                    "partial",
                    "failed",
                    "interrupted",
                }:
                    snapshot = await self.revalidate(snapshot)
                    await self._edit_preview(row, snapshot)
                else:
                    label = (
                        "Cleanup is running"
                        if snapshot and snapshot.get("status") == "running"
                        else "Cleanup complete"
                        if snapshot and snapshot.get("status") == "complete"
                        else "This cleanup review is no longer current"
                    )
                    await self.refresh_control(row, label=label)
                continue
            await self.refresh_control(row, retire=False)

    async def claim(self, guild_id, actor_id, snapshot_id, resume=False):
        """Central confirm/retry claim. Serialized per guild by the UI lock.
        Validates then persists 'running' atomically; on any rejection the
        snapshot keeps its prior status (no phantom running state)."""
        guild_id = str(guild_id)
        ui_lock = self.ui_locks.setdefault(guild_id, asyncio.Lock())
        async with ui_lock:
            data = self._get(snapshot_id)
            if not data or data["guild_id"] != guild_id:
                raise ValueError("Review a cleanup snapshot for this server first.")
            allowed = {"partial", "failed", "interrupted"} if resume else {"preview"}
            if data.get("status") not in allowed:
                raise ValueError(
                    "That cleanup preview is no longer current. Review the server again."
                )
            if self.is_running(guild_id):
                raise ValueError("Server cleanup is already running.")
            data = await self.revalidate(data)
            if data.get("missing_permissions") or data.get("read_gaps"):
                raise ValueError(
                    "Enable cleanup permissions, then review the server again."
                )
            if not self.clients(guild_id):
                raise ValueError("The server bot is disconnected.")
            previous_status = data.get("status")
            # Reserve the guild INSIDE the lock, before scheduling, so a
            # concurrent claim sees is_running() and is rejected above.
            self.running.add(guild_id)
            self.reservations[guild_id] = snapshot_id
            data.update(status="running", running=True, actor_id=str(actor_id))
            try:
                self._save(data)
                task = asyncio.create_task(
                    self.run(guild_id, actor_id, expected_snapshot=snapshot_id)
                )
            except BaseException:
                # Scheduling failed: roll back the reservation and prior status.
                self.running.discard(guild_id)
                self.reservations.pop(guild_id, None)
                data.update(status=previous_status, running=False)
                self._save(data)
                raise
            self.tasks[guild_id] = task
            task.add_done_callback(self._finished)
            return task

    async def _edit_preview(self, row, snapshot):
        client = self._owner_client(row.get("owner_id"))
        try:
            channel = client.get_channel(int(row["channel_id"])) or (
                await client.fetch_channel(int(row["channel_id"]))
            )
            message = await channel.fetch_message(int(row["message_id"]))
            view = (
                preview_view(self.public(snapshot))
                if snapshot.get("status") == "preview"
                else retry_view(
                    client, self.runtime, snapshot["guild_id"], snapshot["snapshot_id"]
                )
            )
            await message.edit(view=view)
        except discord.NotFound:
            self._set_control_state(row["message_id"], "gone")
        except (discord.HTTPException, AttributeError):
            pass

    def supersede_previews(self, guild_id, keep_snapshot):
        """Persist 'superseded' on older unconfirmed previews so the backend
        rejects their confirm buttons even after a restart."""
        with self.state.lock, self.state.db:
            rows = self.state.db.execute(
                "SELECT id,data FROM hub_cleanups WHERE guild_id=?", (str(guild_id),)
            ).fetchall()
            for row in rows:
                data = json.loads(row["data"])
                if row["id"] != keep_snapshot and data.get("status") == "preview":
                    data["status"] = "superseded"
                    self.state.db.execute(
                        "UPDATE hub_cleanups SET data=? WHERE id=?",
                        (json.dumps(data), row["id"]),
                    )

    def resumable(self, guild_id):
        """Snapshots whose run stopped before completion (partial/failed/
        interrupted). Each keeps its original ID and saved deletion progress."""
        with self.state.lock:
            rows = self.state.db.execute(
                "SELECT data FROM hub_cleanups WHERE guild_id=? ORDER BY rowid",
                (str(guild_id),),
            ).fetchall()
        return [
            data
            for data in (json.loads(row["data"]) for row in rows)
            if data.get("status") in {"partial", "failed", "interrupted"}
        ]

    def _save(self, data):
        with self.state.lock, self.state.db:
            self.state.db.execute(
                "INSERT INTO hub_cleanups VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data",
                (data["snapshot_id"], data["guild_id"], json.dumps(data)),
            )

    def _get(self, snapshot_id):
        with self.state.lock:
            row = self.state.db.execute(
                "SELECT data FROM hub_cleanups WHERE id=?", (snapshot_id,)
            ).fetchone()
        return json.loads(row["data"]) if row else None

    def latest(self, guild_id):
        with self.state.lock:
            row = self.state.db.execute(
                "SELECT data FROM hub_cleanups WHERE guild_id=? ORDER BY rowid DESC LIMIT 1",
                (str(guild_id),),
            ).fetchone()
        return self.public(json.loads(row["data"])) if row else None

    @staticmethod
    def public(data):
        return {key: value for key, value in data.items() if key != "rooms"}

    def clients(self, guild_id):
        return [
            client
            for key, client in self.runtime.clients.items()
            if key not in self.runtime.component_hosts
            and client.is_ready()
            and client.get_guild(int(guild_id))
        ]

    async def preview(self, guild_id):
        """Central preview entrypoint for buttons, /cleanup and the dashboard.
        Serialized per guild; supersedes older previews and retires their cards
        inside the same critical section so no stale card can be confirmed."""
        guild_id = str(guild_id)
        ui_lock = self.ui_locks.setdefault(guild_id, asyncio.Lock())
        async with ui_lock:
            if self.is_running(guild_id):
                return self.latest(guild_id)
            data = await self._inventory(guild_id)
            self.supersede_previews(guild_id, data["snapshot_id"])
            await self.retire_controls(guild_id, label="Superseded by a newer review")
            return data

    async def _inventory(self, guild_id):
        clients = self.clients(guild_id)
        if not clients:
            raise ValueError("A bot must be connected to this server first.")
        client = max(
            clients,
            key=lambda item: (
                item.get_guild(int(guild_id)).me.guild_permissions.manage_threads,
                item.get_guild(int(guild_id)).me.guild_permissions.manage_messages,
            ),
        )
        guild = client.get_guild(int(guild_id))
        cutoff = discord.utils.time_snowflake(datetime.now(timezone.utc), high=True)
        bot_ids = {str(item.user.id) for item in clients}
        known_rooms = set()
        for item in clients:
            if hasattr(item, "store"):
                known_rooms.update(
                    str(row[0])
                    for row in item.store.db.execute("SELECT room FROM conversations")
                )
            if hasattr(item, "db"):
                known_rooms.update(
                    str(row[0])
                    for row in item.db.execute(
                        "SELECT thread FROM jobs WHERE thread IS NOT NULL"
                    )
                )
        gaps = []
        channels = await guild.fetch_channels()
        threads = {str(thread.id): thread for thread in await guild.active_threads()}
        for channel in channels:
            if not hasattr(channel, "archived_threads"):
                continue
            permissions = channel.permissions_for(guild.me)
            if not permissions.view_channel:
                gaps.append({"channel_id": str(channel.id), "reason": "View Channel"})
                continue
            try:
                async for thread in channel.archived_threads(limit=None):
                    threads[str(thread.id)] = thread
                async for thread in channel.archived_threads(
                    private=True, joined=not permissions.manage_threads, limit=None
                ):
                    threads[str(thread.id)] = thread
                if not permissions.manage_threads:
                    gaps.append(
                        {
                            "channel_id": str(channel.id),
                            "reason": "Private thread inventory needs Manage Threads",
                        }
                    )
            except discord.Forbidden:
                gaps.append(
                    {"channel_id": str(channel.id), "reason": "Thread history access"}
                )
        rooms = [channel for channel in channels if hasattr(channel, "history")]
        rooms.extend(threads.values())
        inventory = []
        for room in rooms:
            permissions = room.permissions_for(guild.me)
            ai_thread = isinstance(room, discord.Thread) and (
                str(room.owner_id) in bot_ids or str(room.id) in known_rooms
            )
            ids = []
            try:
                async for message in room.history(
                    limit=None, before=discord.Object(cutoff)
                ):
                    if message.type not in {
                        discord.MessageType.thread_starter_message,
                        discord.MessageType.recipient_add,
                        discord.MessageType.recipient_remove,
                    }:
                        ids.append(str(message.id))
            except discord.Forbidden:
                gaps.append(
                    {"channel_id": str(room.id), "reason": "Read Message History"}
                )
            inventory.append(
                {
                    "id": str(room.id),
                    "name": room.name,
                    "messages": ids,
                    "ai_thread": ai_thread,
                    "missing_permissions": [
                        name
                        for name, missing in (
                            (
                                "Manage Messages",
                                bool(ids) and not permissions.manage_messages,
                            ),
                            (
                                "Manage Threads",
                                ai_thread and not permissions.manage_threads,
                            ),
                        )
                        if missing
                    ],
                }
            )
        missing = sorted(
            {name for room in inventory for name in room["missing_permissions"]}
        )
        if (
            gaps
            and "Manage Threads" not in missing
            and any("thread" in gap["reason"].lower() for gap in gaps)
        ):
            missing.append("Manage Threads")
        data = {
            "snapshot_id": uuid.uuid4().hex,
            "guild_id": guild_id,
            "guild_name": guild.name,
            "scope": SCOPE,
            "cutoff_id": str(cutoff),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "message_count": sum(len(room["messages"]) for room in inventory),
            "thread_count": sum(room["ai_thread"] for room in inventory),
            "missing_permissions": missing,
            "required_permissions": self.required_permissions(
                {"rooms": inventory, "read_gaps": gaps}
            ),
            # Cleanup is a shared operation: one link, for the capable bot only.
            "permission_urls": [
                {
                    "name": client.user.display_name,
                    "url": permissions_url(client.user.id, guild_id),
                }
            ]
            if missing
            else [],
            "read_gaps": gaps,
            "rooms": inventory,
            "status": "preview",
            "running": False,
        }
        self._save(data)
        return self.public(data)

    async def start(self, guild_id, actor_id, snapshot_id, resume=False):
        """Central start entrypoint (dashboard and buttons). Delegates to the
        serialized claim so an older preview can never execute."""
        return await self.claim(guild_id, actor_id, snapshot_id, resume=resume)

    def _finished(self, task):
        """Task-done reconciliation. If a run exits before (or without) reaching
        its own finally block (e.g. cancelled while waiting on the run lock),
        clear the reservation and persist a resumable status so no snapshot is
        stranded as 'running'."""
        if not task.cancelled():
            task.exception()
        for guild_id, snapshot_id in list(self.reservations.items()):
            if self.tasks.get(guild_id) is not task:
                continue
            self.reservations.pop(guild_id, None)
            self.running.discard(guild_id)
            data = self._get(snapshot_id)
            if data and data.get("status") == "running":
                data.update(
                    status="interrupted" if task.cancelled() else "failed",
                    running=False,
                )
                self._save(data)

    async def run(self, guild_id, actor_id, expected_snapshot=None, progress=None):
        guild_id = str(guild_id)
        lock = self.locks.setdefault(guild_id, asyncio.Lock())
        async with lock:
            data = self._get(expected_snapshot) if expected_snapshot else None
            if not data or data["guild_id"] != guild_id:
                raise ValueError(
                    "Review a cleanup snapshot before clearing the server."
                )
            if data.get("status") == "complete":
                return data["result"]
            # The UI claims a snapshot by persisting 'running' before starting
            # this task; anything else marked running belongs to another run.
            if data.get("status") == "superseded" or (
                data.get("status") == "running"
                and data.get("actor_id") not in (None, str(actor_id))
            ):
                raise ValueError(
                    "That cleanup preview is no longer current. Review the server again."
                )
            if data.get("missing_permissions") or data.get("read_gaps"):
                raise ValueError(
                    "Grant the missing cleanup permissions, then review a fresh snapshot."
                )
            # Every control card (from either bot, disabled or not) belongs to the
            # operation itself and never counts as newer human activity.
            control_ids = {row["message_id"] for row in self._controls(guild_id)}
            bot_ids = {str(item.user.id) for item in self.clients(guild_id)}
            clients = self.clients(guild_id)
            if not clients:
                raise ValueError("The server bot is disconnected.")
            client = max(
                clients,
                key=lambda item: (
                    item.get_guild(int(guild_id)).me.guild_permissions.manage_threads,
                    item.get_guild(int(guild_id)).me.guild_permissions.manage_messages,
                ),
            )
            if not client.get_guild(int(guild_id)).me.guild_permissions.manage_threads:
                raise ValueError("Manage Threads is required to finish server cleanup.")
            result = data.get("result") or {
                "deleted_messages": 0,
                "deleted_threads": 0,
                "errors": [],
            }
            result["errors"] = []
            result.setdefault("preserved_threads", [])
            deleted_ids = set(data.get("deleted_message_ids", []))
            deleted_threads = set(data.get("deleted_thread_ids", []))
            data.update(
                status="running", running=True, actor_id=str(actor_id), result=result
            )
            self._save(data)
            self.running.add(guild_id)  # idempotent; claim() already reserved
            self.active_runs.add(asyncio.current_task())
            try:
                await self._quiesce(clients)
                await self.runtime.cancel_thread_naming(guild_id)
                clients = self.clients(guild_id)
                if not clients:
                    raise ValueError(
                        "The server bot disconnected during cleanup preparation."
                    )
                client = max(
                    clients,
                    key=lambda item: (
                        item.get_guild(
                            int(guild_id)
                        ).me.guild_permissions.manage_threads,
                        item.get_guild(
                            int(guild_id)
                        ).me.guild_permissions.manage_messages,
                    ),
                )
                old_boundary = discord.utils.time_snowflake(
                    datetime.now(timezone.utc) - timedelta(days=14)
                )
                for room in data["rooms"]:
                    ids = [
                        int(value)
                        for value in room["messages"]
                        if value not in deleted_ids
                    ]
                    recent = [value for value in ids if value > old_boundary]
                    old = [value for value in ids if value <= old_boundary]
                    for offset in range(0, len(recent), 100):
                        batch = recent[offset : offset + 100]
                        try:
                            if len(batch) > 1:
                                await client.http.delete_messages(
                                    int(room["id"]),
                                    batch,
                                    reason="Administrator confirmed server cleanup",
                                )
                            else:
                                await client.http.delete_message(
                                    int(room["id"]),
                                    batch[0],
                                    reason="Administrator confirmed server cleanup",
                                )
                            deleted_ids.update(str(value) for value in batch)
                        except discord.NotFound:
                            deleted_ids.update(str(value) for value in batch)
                        except discord.HTTPException as error:
                            result["errors"].append(
                                {
                                    "channel_id": room["id"],
                                    "operation": "messages",
                                    "status": error.status,
                                }
                            )
                    for message_id in old:
                        try:
                            await client.http.delete_message(
                                int(room["id"]),
                                message_id,
                                reason="Administrator confirmed server cleanup",
                            )
                            deleted_ids.add(str(message_id))
                        except discord.NotFound:
                            deleted_ids.add(str(message_id))
                        except discord.HTTPException as error:
                            result["errors"].append(
                                {
                                    "channel_id": room["id"],
                                    "operation": "message",
                                    "status": error.status,
                                }
                            )
                    if room["ai_thread"] and room["id"] not in deleted_threads:
                        try:
                            removed = await self._delete_thread(
                                client,
                                room,
                                data["cutoff_id"],
                                control_ids=control_ids,
                                bot_ids=bot_ids,
                                errors=result["errors"],
                            )
                            if removed:
                                deleted_threads.add(room["id"])
                                deleted_ids.update(room["messages"])
                            elif room["id"] not in result["preserved_threads"]:
                                result["preserved_threads"].append(room["id"])
                        except discord.NotFound:
                            deleted_threads.add(room["id"])
                        except discord.HTTPException as error:
                            result["errors"].append(
                                {
                                    "channel_id": room["id"],
                                    "operation": "thread",
                                    "status": error.status,
                                }
                            )
                    result.update(
                        deleted_messages=len(deleted_ids),
                        deleted_threads=len(deleted_threads),
                    )
                    data.update(
                        deleted_message_ids=list(deleted_ids),
                        deleted_thread_ids=list(deleted_threads),
                    )
                    self._save(data)
                await self._reconcile(clients, deleted_ids, deleted_threads, data)
                data.update(
                    status="partial" if result["errors"] else "complete", running=False
                )
                self._save(data)
                self.state.record(
                    "system",
                    "cleanup.completed",
                    {"guild_id": guild_id, "actor_id": str(actor_id), **result},
                )
                if progress:
                    await progress(result)
                return result
            except BaseException as error:
                data.update(
                    deleted_message_ids=list(deleted_ids),
                    deleted_thread_ids=list(deleted_threads),
                )
                result.update(
                    deleted_messages=len(deleted_ids),
                    deleted_threads=len(deleted_threads),
                )
                data.update(
                    status="interrupted"
                    if isinstance(error, asyncio.CancelledError)
                    else "failed",
                    running=False,
                )
                result["errors"].append(
                    {"operation": "cleanup", "error": type(error).__name__}
                )
                self._save(data)
                reconcile = asyncio.create_task(
                    self._reconcile(
                        self.clients(guild_id), deleted_ids, deleted_threads, data
                    )
                )
                try:
                    await asyncio.shield(reconcile)
                except asyncio.CancelledError:
                    await reconcile
                raise
            finally:
                self.running.discard(guild_id)
                self.active_runs.discard(asyncio.current_task())
                for item in self.clients(guild_id):
                    if hasattr(item, "wake"):
                        item.wake.set()

    async def _delete_thread(
        self, client, room, cutoff_id, control_ids=(), bot_ids=(), errors=None
    ):
        errors = errors if errors is not None else []
        thread_id = int(room["id"])
        current = await client.http.get_channel(thread_id)
        metadata = current.get("thread_metadata", {})
        original = {
            "archived": bool(metadata.get("archived")),
            "locked": bool(metadata.get("locked")),
        }
        removed = False
        try:
            # Discord rejects other edits on archived threads (50083): unarchive
            # alone first. A locked but unarchived thread still accepts posts, so
            # the real freeze is archived+locked before newer activity is checked.
            if original["archived"]:
                await client.http.edit_channel(
                    thread_id,
                    archived=False,
                    reason="Unarchive an AI thread for cleanup",
                )
            await client.http.edit_channel(
                thread_id,
                archived=True,
                locked=True,
                reason="Freeze an AI thread during confirmed cleanup",
            )
            newer = await client.http.logs_from(
                thread_id, limit=100, after=int(cutoff_id)
            )
            # Cleanup controls from either bot identity belong to the operation
            # itself. Other new posts (humans, Issue 5, verification chat) survive.
            fresh = [
                message
                for message in newer
                if message.get("id") not in control_ids
                and not (
                    message.get("author", {}).get("id") in bot_ids
                    and any(
                        component.get("custom_id", "").startswith("cleanup:")
                        for row in message.get("components", [])
                        for component in row.get("components", [])
                    )
                )
            ]
            if fresh or len(newer) >= 100:
                return False
            await client.http.delete_channel(
                thread_id, reason="Administrator confirmed AI thread cleanup"
            )
            removed = True
            return True
        finally:
            if not removed:
                # Runs on failure and cancellation. Unarchive alone first, then
                # reapply the original lock/archive state in the allowed order.
                restore_task = asyncio.create_task(
                    self._restore_thread(client, thread_id, original, errors)
                )
                try:
                    await asyncio.shield(restore_task)
                except asyncio.CancelledError:
                    # The shield Future is cancelled; join the real task so
                    # restoration finishes before cancellation propagates.
                    await restore_task
                    raise

    async def _restore_thread(self, client, thread_id, original, errors):
        try:
            await client.http.edit_channel(
                thread_id, archived=False, reason="Restore a surviving thread"
            )
            await client.http.edit_channel(
                thread_id,
                locked=original["locked"],
                archived=original["archived"],
                reason="Restore a surviving thread after cleanup",
            )
        except discord.NotFound:
            pass
        except discord.HTTPException as error:
            errors.append(
                {
                    "channel_id": str(thread_id),
                    "operation": "restore_thread",
                    "status": error.status,
                    "original": original,
                }
            )

    async def _quiesce(self, clients):
        host_ids = {item.instance["id"] for item in clients}
        component_tasks = set()
        for component_id, host_id in self.runtime.component_hosts.items():
            if host_id in host_ids:
                component_tasks.update(
                    self.runtime.component_dispatch_tasks.get(component_id, set())
                )
                component = self.runtime.clients.get(component_id)
                component_tasks.update(getattr(component, "message_tasks", set()))
        for task in component_tasks:
            task.cancel()
        await asyncio.gather(*component_tasks, return_exceptions=True)
        for client in clients:
            for job_id, task in list(getattr(client, "active_jobs", {}).items()):
                if not task.done():
                    job = client.get(job_id)
                    if job and job["state"] == "applying":
                        await asyncio.shield(task)
                    else:
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
            tasks = set(getattr(client, "turn_tasks", set()))
            tasks.update(getattr(client, "ambient_tasks", {}).values())
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _reconcile(self, clients, deleted_ids, deleted_threads, data):
        rooms = {
            room["id"] for room in data["rooms"] if set(room["messages"]) <= deleted_ids
        }
        for client in clients:
            if hasattr(client, "store"):
                affected_turns = []
                for turn in client.store.db.execute(
                    "SELECT key,source_id,output_ids,room FROM turns"
                ).fetchall():
                    if (
                        str(turn["source_id"]) in deleted_ids
                        or set(json.loads(turn["output_ids"])) & deleted_ids
                        or str(turn["room"]) in deleted_threads
                    ):
                        affected_turns.append(turn["key"])
                with client.store.db:
                    for key in affected_turns:
                        client.store.db.execute(
                            "DELETE FROM messages WHERE turn_key=?", (key,)
                        )
                        client.store.db.execute("DELETE FROM turns WHERE key=?", (key,))
                        client.store.db.execute(
                            "DELETE FROM raw_results WHERE id=?", (key,)
                        )
                        client.store.db.execute(
                            "DELETE FROM turn_revisions WHERE turn_key=?", (key,)
                        )
                    for room_id in rooms:
                        client.store.db.execute(
                            "DELETE FROM messages WHERE room=? AND source_id IS NULL AND turn_key IS NULL",
                            (room_id,),
                        )
                for source_id in deleted_ids:
                    client.store.remove_message(source_id)
                with client.store.db:
                    for thread_id in deleted_threads:
                        client.store.db.execute(
                            "DELETE FROM conversations WHERE room=?", (thread_id,)
                        )
                        client.store.db.execute(
                            "DELETE FROM messages WHERE room=?", (thread_id,)
                        )
            if hasattr(client, "db"):
                for job in client.db.execute("SELECT * FROM jobs").fetchall():
                    if (
                        str(job["source"]) in deleted_ids
                        or str(job["thread"]) in deleted_threads
                    ):
                        if job["state"] in {"done", "cancelled"} and not any(
                            job[field] for field in ("card", "thread_card", "thread")
                        ):
                            continue
                        client.update(
                            job["id"],
                            state="done" if job["state"] == "done" else "cancelled",
                            version=job["version"] + 1,
                            card=None,
                            thread_card=None,
                            thread=None,
                            note="Server cleanup retired these Discord controls.",
                        )
        for prediction in self.state.predictions(limit=None):
            if (
                prediction.get("message_id") in deleted_ids
                or prediction.get("channel_id") in deleted_threads
            ):
                prediction.setdefault("payload", {}).update(
                    superseded_by="server-cleanup",
                    message_id=None,
                    discord_messages_deleted=True,
                )
                prediction["message_id"] = None
                self.state.save_prediction(prediction)
        if getattr(self.runtime, "projects", None):
            await self.runtime.projects.remove_sources(
                list(deleted_ids), list(deleted_threads)
            )
        data["context_rooms_cleaned"] = len(rooms)

    async def close(self):
        pending = (set(self.tasks.values()) | self.active_runs) - {
            asyncio.current_task()
        }
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


class CleanupButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r"cleanup:(?P<guild>\d+):(?P<snapshot>[a-f0-9]+|new):(?P<action>review|confirm|retry)",
):
    LABELS = {
        "confirm": "Clear server messages and AI threads",
        "retry": "Retry cleanup (same snapshot)",
        "review": "Review server cleanup",
    }

    def __init__(self, guild_id, snapshot_id="new", action="review"):
        super().__init__(
            discord.ui.Button(
                label=self.LABELS[action],
                style=discord.ButtonStyle.danger
                if action in {"confirm", "retry"}
                else discord.ButtonStyle.secondary,
                custom_id=f"cleanup:{guild_id}:{snapshot_id}:{action}",
            )
        )
        self.guild_id, self.snapshot_id, self.action = (
            str(guild_id),
            snapshot_id,
            action,
        )

    @classmethod
    async def from_custom_id(cls, interaction, item, match):
        return cls(match["guild"], match["snapshot"], match["action"])

    async def callback(self, interaction):
        if (
            str(interaction.guild_id) != self.guild_id
            or not interaction.user.guild_permissions.administrator
        ):
            await interaction.response.send_message(
                "A server administrator can use cleanup controls.", ephemeral=True
            )
            return
        runtime = interaction.client.hub_runtime
        manager = runtime.cleanup
        client = interaction.client
        if manager.is_running(self.guild_id):
            await interaction.response.send_message(
                "Server cleanup is already running. Wait for it to finish.",
                ephemeral=True,
            )
            return
        if self.action != "review" and client.instance.get("dry_run"):
            await interaction.response.send_message(
                "This instance is in dry run. Use a live instance for cleanup.",
                ephemeral=True,
            )
            return
        # Retire the CLICKED card immediately (legacy untracked cards included).
        # All lock-protected work happens inside the manager entrypoints below;
        # this callback never holds the guild lock itself (no re-entrancy).
        await disable_message(interaction, remove_links=True)
        manager._set_control_state(interaction.message.id, "retired")
        if self.action == "review":
            try:
                preview = await manager.preview(self.guild_id)
            except (ValueError, discord.HTTPException) as error:
                await interaction.followup.send(
                    str(error)
                    if isinstance(error, ValueError)
                    else "Discord could not inventory the server. Check cleanup permissions.",
                    view=status_view(client, runtime),
                )
                return
            await publish_preview(interaction, manager, preview)
            return
        # confirm / retry: claim is validated + serialized inside the manager.
        try:
            task = await manager.start(
                self.guild_id,
                interaction.user.id,
                self.snapshot_id,
                resume=self.action == "retry",
            )
        except ValueError as error:
            await interaction.followup.send(
                f"{error}", view=status_view(client, runtime), ephemeral=True
            )
            return
        await manager.retire_controls(self.guild_id, label="Cleanup started")
        snapshot_ref = None  # complete result cards carry no snapshot
        try:
            result = await asyncio.shield(task)
            content = f"Cleanup finished. {result['deleted_messages']} messages and {result['deleted_threads']} AI threads removed. {len(result['errors'])} operation errors recorded."
            if result.get("preserved_threads"):
                content += f" {len(result['preserved_threads'])} threads with newer activity were preserved."
            view = status_view(client, runtime)
            if result["errors"]:
                content += " Use Retry cleanup to resume the same snapshot."
                view = retry_view(client, runtime, self.guild_id, self.snapshot_id)
                snapshot_ref = self.snapshot_id
        except (ValueError, discord.HTTPException) as error:
            content = (
                str(error)
                if isinstance(error, ValueError)
                else "Discord refused part of the cleanup. Use Retry cleanup to resume."
            )
            view = retry_view(client, runtime, self.guild_id, self.snapshot_id)
            snapshot_ref = self.snapshot_id
        try:
            sent = await interaction.followup.send(content, view=view, wait=True)
        except discord.NotFound:
            parent = getattr(interaction.channel, "parent", None) or interaction.channel
            sent = await parent.send(content, view=view)
        manager.track_control(sent, snapshot_ref, guild_id=interaction.guild_id)


async def publish_preview(interaction, manager, preview):
    """Post a preview card, track it, and retire it at once if a newer preview
    (from any entrypoint or bot) already superseded it while we were sending."""
    sent = await interaction.followup.send(
        content=preview_text(preview), view=preview_view(preview), wait=True
    )
    manager.track_control(sent, preview["snapshot_id"], guild_id=interaction.guild_id)
    current = manager._get(preview["snapshot_id"])
    if current and not current.get("channel_id"):
        # Remember where this snapshot lives so restart recovery can post a
        # Retry card even after every source card has been retired.
        current["channel_id"] = str(sent.channel.id)
        manager._save(current)
    if not current or current.get("status") != "preview":
        for row in manager._controls(preview["guild_id"]):
            if row["message_id"] == str(sent.id):
                await manager.refresh_control(row, label="Superseded by a newer review")


async def disable_message(interaction, remove_links=False):
    """Retire the clicked card in place: disable buttons, drop permission links.
    Works for legacy cards that were never tracked. Always leaves the
    interaction acknowledged so a followup can be sent."""
    try:
        view = discord.ui.View.from_message(interaction.message, timeout=None)
        for item in list(view.children):
            if getattr(item, "url", None):
                if remove_links:
                    view.remove_item(item)
            else:
                item.disabled = True
        await interaction.response.edit_message(view=view)
    except (discord.HTTPException, AttributeError):
        pass
    if not interaction.response.is_done():
        await interaction.response.defer(thinking=True)


def retry_view(client, runtime, guild_id, snapshot_id):
    """Retry resumes the SAME immutable snapshot with saved deletion progress."""
    view = status_view(client, runtime)
    view.add_item(CleanupButton(guild_id, snapshot_id, "retry"))
    return view


def permissions_granted(client, guild_id):
    guild = client.get_guild(int(guild_id)) if client.user else None
    return bool(
        guild
        and guild.me.guild_permissions.manage_threads
        and guild.me.guild_permissions.manage_messages
    )


def preview_text(preview):
    message = f"**Server cleanup review**\n{preview['scope']}.\n{preview['message_count']} messages and {preview['thread_count']} AI threads are in this snapshot. Deletion is permanent. Messages posted after this snapshot are preserved."
    if preview["missing_permissions"]:
        message += (
            "\nEnable "
            + ", ".join(preview["missing_permissions"])
            + " with the Discord permission link, then review again."
        )
    gaps = preview["read_gaps"]
    if gaps and all(gap.get("access_granted") for gap in gaps):
        message += "\nAccess was granted after this review, but the missed channels or private threads are not in this snapshot. Review again to inventory them."
    elif gaps:
        message += "\nSome channel or private thread history is inaccessible. Complete access is needed for a full server cleanup."
    return message


def preview_view(preview):
    view = discord.ui.View(timeout=None)
    if not preview["missing_permissions"] and not preview["read_gaps"]:
        view.add_item(
            CleanupButton(preview["guild_id"], preview["snapshot_id"], "confirm")
        )
    # Permission links appear only while a real deficit exists on the capable
    # bot (permission_urls is already empty once access is granted). Read gaps
    # whose access was granted show the plain "Review again" action instead.
    for item in (
        preview["permission_urls"][:3] if preview["missing_permissions"] else []
    ):
        view.add_item(
            discord.ui.Button(
                label="Enable cleanup · " + item["name"][:45], url=item["url"]
            )
        )
    review = CleanupButton(preview["guild_id"])
    if preview["read_gaps"] and not preview["missing_permissions"]:
        review.item.label = "Review again"
    view.add_item(review)
    return view


def status_view(client, runtime):
    """Status card: one Review button, plus a permission link only when the
    SHARED cleanup operation lacks a capable connected bot (never a redundant
    per-identity grant when the other identity already holds the permissions)."""
    view = discord.ui.View(timeout=None)
    guild_id = int(client.config["guild_id"])
    view.add_item(CleanupButton(guild_id))
    manager = getattr(runtime, "cleanup", None)
    capable = manager.capable_client(guild_id) if manager else client
    if capable and capable.user and not permissions_granted(capable, guild_id):
        view.add_item(
            discord.ui.Button(
                label="Enable cleanup permissions · " + capable.user.display_name[:30],
                url=permissions_url(capable.user.id, guild_id),
            )
        )
    return view


def install_commands(client, runtime):
    client.add_dynamic_items(CleanupButton)

    async def recover():
        """Restart recovery: refresh/retire this bot's cards from persisted IDs
        and surface a Retry card for runs that were interrupted mid-flight."""
        guild_id = str(client.config["guild_id"])
        manager = runtime.cleanup
        await manager.refresh_permission_cards(guild_id)
        # Drive recovery from persisted SNAPSHOT rows, not surviving cards: all
        # source cards may have been retired right before the interruption.
        for snapshot in manager.resumable(guild_id):
            snapshot_id = snapshot["snapshot_id"]
            rows = [
                row
                for row in manager._controls(guild_id)
                if row.get("snapshot_id") == snapshot_id
            ]
            # An active Retry card for this snapshot (either bot) already exists.
            if any(
                row.get("state") == "active"
                and (owner := manager._owner_client(row.get("owner_id"))) is not None
                and owner.is_ready()
                for row in rows
            ):
                continue
            # Only one bot posts the Retry card: the one that owned the last
            # card for this snapshot, else the first ready bot.
            owners = [manager._owner_client(row.get("owner_id")) for row in rows]
            ready_owners = [
                owner for owner in owners if owner is not None and owner.is_ready()
            ]
            poster = (
                ready_owners[-1] if ready_owners else manager.capable_client(guild_id)
            )
            if poster is None or str(poster.user.id) != str(client.user.id):
                continue
            channel_id = rows[-1]["channel_id"] if rows else snapshot.get("channel_id")
            channel = client.get_channel(int(channel_id)) if channel_id else None
            if channel is None and channel_id:
                try:
                    channel = await client.fetch_channel(int(channel_id))
                except discord.HTTPException:
                    continue
            if channel is None:
                continue
            sent = await channel.send(
                "Cleanup was interrupted by a restart. Retry resumes the same snapshot with saved progress.",
                view=retry_view(client, runtime, guild_id, snapshot_id),
            )
            manager.track_control(sent, snapshot_id, guild_id=guild_id)

    async def safe_recover():
        try:
            await recover()
        except Exception:  # Recovery must never block the READY path.
            pass

    # HubTogether.on_ready / on_guild_role_update call these hooks here; the
    # operator foundation dispatches the same callbacks for the Department bot.
    # Both identities run recovery; each edits only cards it owns.
    client.cleanup_recover = safe_recover
    client.cleanup_permissions_changed = runtime.cleanup.refresh_permission_cards

    @app_commands.command(
        name="cleanup", description="Review clearing server messages and AI threads"
    )
    @app_commands.default_permissions(administrator=True)
    async def cleanup(interaction: discord.Interaction):
        if (
            not interaction.guild
            or not interaction.user.guild_permissions.administrator
        ):
            await interaction.response.send_message(
                "A server administrator can use cleanup controls.", ephemeral=True
            )
            return
        await interaction.response.defer(thinking=True)
        try:
            # Supersession/retirement happen inside manager.preview (serialized).
            preview = await runtime.cleanup.preview(interaction.guild_id)
            await publish_preview(interaction, runtime.cleanup, preview)
        except (ValueError, discord.HTTPException) as error:
            await interaction.followup.send(
                str(error)
                if isinstance(error, ValueError)
                else "Discord could not inventory the server. Check cleanup permissions.",
                view=status_view(client, runtime),
            )

    client.tree.add_command(cleanup, override=True)
