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
        self.running = set()
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
        guild_id = str(guild_id)
        if self.is_running(guild_id):
            return self.latest(guild_id)
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
            "permission_urls": [
                {
                    "name": item.user.display_name,
                    "url": permissions_url(item.user.id, guild_id),
                }
                for item in clients
            ],
            "read_gaps": gaps,
            "rooms": inventory,
            "status": "preview",
            "running": False,
        }
        self._save(data)
        return self.public(data)

    def start(self, guild_id, actor_id, snapshot_id):
        data = self._get(snapshot_id)
        if not data or data["guild_id"] != str(guild_id):
            raise ValueError("Review a cleanup snapshot for this server first.")
        if data.get("missing_permissions") or data.get("read_gaps"):
            raise ValueError(
                "Enable cleanup permissions, then review the server again."
            )
        task = self.tasks.get(str(guild_id))
        if task and not task.done():
            raise ValueError("Server cleanup is already running.")
        task = asyncio.create_task(
            self.run(guild_id, actor_id, expected_snapshot=snapshot_id)
        )
        self.tasks[str(guild_id)] = task
        task.add_done_callback(self._finished)
        return task

    def _finished(self, task):
        if not task.cancelled():
            task.exception()

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
            if data.get("missing_permissions") or data.get("read_gaps"):
                raise ValueError(
                    "Grant the missing cleanup permissions, then review a fresh snapshot."
                )
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
            self.running.add(guild_id)
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
                                client, room, data["cutoff_id"]
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

    async def _delete_thread(self, client, room, cutoff_id):
        thread_id = int(room["id"])
        current = await client.http.get_channel(thread_id)
        metadata = current.get("thread_metadata", {})
        await client.http.edit_channel(
            thread_id,
            archived=True,
            locked=True,
            reason="Freeze an AI thread during confirmed cleanup",
        )
        removed = False
        try:
            newer = await client.http.logs_from(
                thread_id, limit=100, after=int(cutoff_id)
            )
            # Cleanup controls belong to the operation itself. Other new posts survive.
            fresh = [
                message
                for message in newer
                if not (
                    message.get("author", {}).get("id") == str(client.user.id)
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
                try:
                    await client.http.edit_channel(
                        thread_id,
                        archived=metadata.get("archived", False),
                        locked=metadata.get("locked", False),
                        reason="Restore a surviving thread after cleanup",
                    )
                except discord.NotFound:
                    pass

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
    template=r"cleanup:(?P<guild>\d+):(?P<snapshot>[a-f0-9]+|new):(?P<action>review|confirm)",
):
    def __init__(self, guild_id, snapshot_id="new", action="review"):
        super().__init__(
            discord.ui.Button(
                label="Clear server messages and AI threads"
                if action == "confirm"
                else "Review server cleanup",
                style=discord.ButtonStyle.danger
                if action == "confirm"
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
        await interaction.response.defer(thinking=True)
        try:
            if self.action == "review":
                preview = await runtime.cleanup.preview(self.guild_id)
                await interaction.followup.send(
                    content=preview_text(preview), view=preview_view(preview)
                )
            else:
                if interaction.client.instance.get("dry_run"):
                    raise ValueError(
                        "This instance is in dry run. Use a live instance for cleanup."
                    )
                result = await asyncio.shield(
                    runtime.cleanup.start(
                        self.guild_id, interaction.user.id, self.snapshot_id
                    )
                )
                content = f"Cleanup finished. {result['deleted_messages']} messages and {result['deleted_threads']} AI threads removed. {len(result['errors'])} operation errors recorded."
                if result.get("preserved_threads"):
                    content += f" {len(result['preserved_threads'])} threads with newer activity were preserved."
                try:
                    await interaction.followup.send(
                        content, view=status_view(interaction.client, runtime)
                    )
                except discord.NotFound:
                    parent = getattr(interaction.channel, "parent", None)
                    if parent:
                        await parent.send(
                            content, view=status_view(interaction.client, runtime)
                        )
        except (ValueError, discord.HTTPException) as error:
            await interaction.followup.send(
                str(error)
                if isinstance(error, ValueError)
                else "Discord refused part of the cleanup. Review the dashboard result.",
                view=status_view(interaction.client, runtime),
            )


def preview_text(preview):
    message = f"**Server cleanup review**\n{preview['scope']}.\n{preview['message_count']} messages and {preview['thread_count']} AI threads are in this snapshot. Deletion is permanent. Messages posted after this snapshot are preserved."
    if preview["missing_permissions"]:
        message += (
            "\nEnable "
            + ", ".join(preview["missing_permissions"])
            + " with the Discord permission link, then review again."
        )
    if preview["read_gaps"]:
        message += "\nSome channel or private thread history is inaccessible. Complete access is needed for a full server cleanup."
    return message


def preview_view(preview):
    view = discord.ui.View(timeout=None)
    if not preview["missing_permissions"] and not preview["read_gaps"]:
        view.add_item(
            CleanupButton(preview["guild_id"], preview["snapshot_id"], "confirm")
        )
    for item in preview["permission_urls"][:3]:
        view.add_item(
            discord.ui.Button(
                label="Enable cleanup · " + item["name"][:45], url=item["url"]
            )
        )
    view.add_item(CleanupButton(preview["guild_id"]))
    return view


def status_view(client, runtime):
    view = discord.ui.View(timeout=None)
    guild_id = int(client.config["guild_id"])
    view.add_item(CleanupButton(guild_id))
    if client.user:
        guild = client.get_guild(guild_id)
        if guild and not guild.me.guild_permissions.manage_threads:
            view.add_item(
                discord.ui.Button(
                    label="Enable cleanup permissions",
                    url=permissions_url(client.user.id, guild_id),
                )
            )
    return view


def install_commands(client, runtime):
    client.add_dynamic_items(CleanupButton)

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
            preview = await runtime.cleanup.preview(interaction.guild_id)
            await interaction.followup.send(
                content=preview_text(preview), view=preview_view(preview)
            )
        except (ValueError, discord.HTTPException) as error:
            await interaction.followup.send(
                str(error)
                if isinstance(error, ValueError)
                else "Discord could not inventory the server. Check cleanup permissions.",
                view=status_view(client, runtime),
            )

    client.tree.add_command(cleanup, override=True)
