import asyncio
import base64
import gzip
import io
import json
import logging
import os
import re
import time
import uuid
from collections import OrderedDict
from pathlib import Path

import discord
from discord import app_commands

from .conversations import TurnButton
from .core import Engine, Store
from .routing import routing_report

HELP = """**Together AI**
In public channels, mention me or use `/chat`. In managed AI conversation threads, chat normally without mentions. Replies share context within the current channel or thread.
`/thread with_user` starts a shared conversation and invites your friends. Everyone can chat there without mentioning me.
Use **Retry** or **Edit prompt** beneath an answer. Editing your original Discord message also regenerates it.
Editing an earlier thread prompt rewinds the thread, removes later messages, and regenerates from there.
`/history` shows this conversation's AI context. `/retry` retries the latest answer.
`/reconcile result` records the connector result you checked when acceptance is uncertain, then resumes the saved request.
`/participation` adjusts how often I join in. Mentions and direct replies still get an answer.
`/models` finds available routes. `/model` selects this channel's route.
`/routes` lists favorites and manages route aliases.
`/modify request` opens a reviewed change to this bot’s own code and settings. Review the diff, refine it, then approve to apply.
`/skills` lists skills. `/skill` runs one. You can also write `@Together AI @explain your question`.
`/connectors` lists connected tools. `/tool` calls a tool with JSON arguments.
`/summarize` summarizes recent channel messages. `/reset` clears AI conversation context.
`/video` uses generation routes you connect in OmniRoute.
`/status` shows the current model and gateway. `/help` shows this guide.
Messages sent to me and requested summaries go to the configured AI gateway. Answers are shared here."""


def load_config():
    path = Path(os.environ.get("TOGETHER_CONFIG", "config.json"))
    return path, json.loads(path.read_text())


def has_image_attachment(message):
    extensions = {".png", ".jpg", ".jpeg", ".gif", ".webp"}
    return any(
        (attachment.content_type or "").startswith("image/")
        or Path(attachment.filename).suffix.lower() in extensions
        for attachment in message.attachments
    )


async def attachment_image_parts(message, budget):
    """Read image bytes from Discord attachments within a shared request budget."""
    parts = []
    for attachment in message.attachments:
        mime_hint = attachment.content_type or ""
        if not mime_hint.startswith("image/") and Path(
            attachment.filename
        ).suffix.lower() not in {".png", ".jpg", ".jpeg", ".gif", ".webp"}:
            continue
        if attachment.size > 8 * 1024 * 1024 or attachment.size > budget["bytes"]:
            raise ValueError(
                "Each image must fit 8 MiB, with at most 16 MiB of images per request."
            )
        try:
            data = await attachment.read()
        except discord.HTTPException as error:
            if error.status == 404:
                raise ValueError(
                    "The attached image is no longer available. Reattach it and retry."
                ) from None
            raise
        if len(data) > 8 * 1024 * 1024 or len(data) > budget["bytes"]:
            raise ValueError(
                "Each image must fit 8 MiB, with at most 16 MiB of images per request."
            )
        if data.startswith(b"\x89PNG\r\n\x1a\n"):
            mime = "image/png"
        elif data.startswith(b"\xff\xd8\xff"):
            mime = "image/jpeg"
        elif data.startswith((b"GIF87a", b"GIF89a")):
            mime = "image/gif"
        elif data.startswith(b"RIFF") and data[8:12] == b"WEBP":
            mime = "image/webp"
        else:
            raise ValueError(
                "Image attachments must contain PNG, JPEG, GIF or WebP bytes."
            )
        budget["bytes"] -= len(data)
        parts.append(
            {
                "type": "image_url",
                "image_url": {
                    "url": "data:" + mime + ";base64," + base64.b64encode(data).decode()
                },
            }
        )
    return parts


class RawJSONButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r"rawjson:(?P<key>[a-f0-9]{32}):(?P<expanded>[01])",
):
    def __init__(self, key, expanded=False):
        self.key, self.expanded = key, expanded
        super().__init__(
            discord.ui.Button(
                label="Hide raw JSON" if expanded else "Show raw JSON",
                style=discord.ButtonStyle.secondary,
                custom_id=f"rawjson:{key}:{int(expanded)}",
            )
        )

    @classmethod
    async def from_custom_id(cls, interaction, item, match):
        return cls(match["key"], match["expanded"] == "1")

    async def callback(self, interaction):
        record = interaction.client.store.raw(self.key)
        if record is None:
            await interaction.response.send_message(
                "This raw response is no longer available."
            )
            return
        summary, raw = record
        expanded = not self.expanded
        view = interaction.client.response_view(self.key, expanded)
        attachments = [
            a
            for a in interaction.message.attachments
            if not a.filename.startswith("raw-response.")
        ]
        content = summary
        if expanded:
            if len(summary) + len(raw) < 1850 and "```" not in raw:
                content += "\n```json\n" + raw + "\n```"
            else:
                content += "\nThe complete raw JSON is attached."
                data, filename = raw.encode(), "raw-response.json"
                if len(data) > interaction.guild.filesize_limit:
                    data, filename = gzip.compress(data), "raw-response.json.gz"
                if len(data) > interaction.guild.filesize_limit:
                    await interaction.response.send_message(
                        "The full raw response exceeds this server’s upload limit even after compression."
                    )
                    return
                attachments.append(discord.File(io.BytesIO(data), filename=filename))
        await interaction.response.edit_message(
            content=content, attachments=attachments, view=view
        )


class Together(discord.Client):
    def __init__(self, config_path, config):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(
            intents=intents, allowed_mentions=discord.AllowedMentions.none()
        )
        self.config_path, self.config = config_path, config
        self.tree = app_commands.CommandTree(self)
        self.store = Store(config["database"])
        self.engine = Engine(
            config, Path(config["api_key_file"]).read_text().strip(), self.store
        )
        self.slots = asyncio.Semaphore(4)
        self.room_locks = {}
        self.room_epochs = {}
        self.active_turns = {}
        self.recovery_tasks = set()
        self.recovery_scan_running = False
        self.recovery_loop_task = None
        self.internal_deletes = set()
        self.ambient_tasks = {}
        self.ambient_sequences = {}
        self.last_ambient_reply = {}
        self.image_cache = OrderedDict()
        self.image_cache_bytes = 0
        self.add_dynamic_items(RawJSONButton, TurnButton)
        self.register_commands()

    async def setup_hook(self):
        await self.engine.refresh_models()
        try:
            await self.engine.refresh_tools()
        except Exception:
            logging.warning("Connector discovery unavailable. Chat remains available.")
        guild = discord.Object(id=int(self.config["guild_id"]))
        self.tree.copy_global_to(guild=guild)
        commands = await self.tree.sync(guild=guild)
        logging.info("Registered %d server commands", len(commands))

    async def on_ready(self):
        logging.info(
            "READY bot=%s guilds=%s models=%d tools=%d",
            self.user.id,
            [g.id for g in self.guilds],
            len(self.engine.models),
            len(self.engine.tools),
        )
        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.listening, name="/help · mention me to chat"
            )
        )
        await self.recover_pending_turns()
        if self.recovery_loop_task is None or self.recovery_loop_task.done():
            self.recovery_loop_task = asyncio.create_task(self._recover_waiting_turns())

    async def close(self):
        pending = set(self.active_turns.values()) | self.recovery_tasks
        if self.recovery_loop_task:
            pending.add(self.recovery_loop_task)
        pending.discard(asyncio.current_task())
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in self.ambient_tasks.values():
            task.cancel()
        await self.engine.http.aclose()
        self.store.db.close()
        await super().close()

    @staticmethod
    def error_text(error):
        return safe_error(error)

    def response_view(self, key, expanded=False, version=None):
        view = discord.ui.View(timeout=None)
        turn = self.store.turn(key)
        if turn:
            version = turn["version"] if version is None else version
            if version > turn["version"] or turn["status"] not in {
                "pending",
                "recovering",
                "waiting",
                "delivering",
            }:
                view.add_item(TurnButton(key, "retry", version))
            view.add_item(TurnButton(key, "edit", version))
        view.add_item(RawJSONButton(key, expanded))
        return view

    async def render_turn(
        self,
        key,
        model,
        answer,
        raw,
        send=None,
        existing=None,
        version=None,
        prompt_annotation=None,
    ):
        actual_model = (
            next(
                (
                    entry.get("model")
                    for entry in reversed(raw)
                    if isinstance(entry, dict) and entry.get("model")
                ),
                None,
            )
            if isinstance(raw, list)
            else None
        )
        heading = f"**{actual_model or model}**"
        if actual_model and actual_model != model:
            heading += f" · route `{model}`"
        content = heading + "\n" + answer
        turn = self.store.turn(key)
        if turn and isinstance(
            self.get_channel(int(turn["room"])), discord.TextChannel
        ):
            content = answer
        if prompt_annotation:
            content = f"**Revised prompt**\n> {prompt_annotation}\n\n" + content
        files = []
        if len(content) > 1900:
            content = heading + "\nThe answer is attached."
            files.append(
                discord.File(io.BytesIO(answer.encode()), filename="answer.md")
            )
        self.store.save_raw(key, content, json.dumps(raw, ensure_ascii=False, indent=2))
        view = self.response_view(key, version=version)
        if existing:
            return await existing.edit(content=content, attachments=files, view=view)
        return await send(content=content, files=files, view=view)

    def invalidate_image_cache(self, message_id):
        cached = self.image_cache.pop(str(message_id), None)
        if cached is not None:
            self.image_cache_bytes -= cached["bytes"]

    def dispatch(self, event, *args, **kwargs):
        # Invalidate before subclass raw handlers can return for scope/dry-run checks.
        if args and event in {
            "raw_message_edit",
            "raw_message_delete",
            "raw_bulk_message_delete",
        }:
            payload = args[0]
            message_ids = (
                payload.message_ids
                if event == "raw_bulk_message_delete"
                else [payload.message_id]
            )
            for message_id in message_ids:
                self.invalidate_image_cache(message_id)
                draft_task = getattr(self, "draft_sources", {}).get(str(message_id))
                if draft_task and (
                    event != "raw_message_edit"
                    or "content" in payload.data
                    or "attachments" in payload.data
                ):
                    draft_task.cancel()
                turn = self.store.turn_by_source(message_id)
                changed = event != "raw_message_edit"
                if turn and event == "raw_message_edit":
                    content = payload.data.get("content")
                    changed = "attachments" in payload.data or (
                        content is not None
                        and self.normalize_prompt(
                            content,
                            getattr(
                                self.get_channel(payload.channel_id), "guild", None
                            ),
                        )
                        != turn["prompt"]
                    )
                if turn and changed:
                    if event == "raw_message_edit":
                        self.queue_source_refresh(turn["key"], data=payload.data)
                    else:
                        self.queue_turn_operation(turn["key"], "delete")
        super().dispatch(event, *args, **kwargs)

    async def source_image_parts(
        self, channel, source_id, budget, source=None, refresh=False
    ):
        """Cache source images/negative lookups, while keeping inference bytes bounded."""
        key = str(source_id)
        if refresh:
            self.invalidate_image_cache(key)
        cached = self.image_cache.get(key)
        if cached is None:
            source = source or await channel.fetch_message(int(source_id))
            source_budget = {"bytes": 16 * 1024 * 1024}
            parts = await attachment_image_parts(source, source_budget)
            cached = {
                "parts": parts,
                "bytes": 16 * 1024 * 1024 - source_budget["bytes"],
            }
            self.image_cache[key] = cached
            self.image_cache_bytes += cached["bytes"]
            while (
                len(self.image_cache) > 256 or self.image_cache_bytes > 64 * 1024 * 1024
            ):
                _, expired = self.image_cache.popitem(last=False)
                self.image_cache_bytes -= expired["bytes"]
        else:
            self.image_cache.move_to_end(key)
        if cached["bytes"] > budget["bytes"]:
            raise ValueError(
                "At most 16 MiB of image bytes fit this request. Earlier image input was omitted."
            )
        budget["bytes"] -= cached["bytes"]
        return cached["parts"]

    async def image_history(
        self,
        channel,
        history,
        budget,
        *,
        prompt="",
        include_previous=True,
        snapshots=None,
    ):
        """Reload recent source images without persisting image bytes in SQLite."""
        context = []
        candidates = []
        for message in history:
            entry = dict(message)
            source_id = entry.pop("source_id", None)
            context.append(entry)
            if source_id and entry.get("role") == "user":
                candidates.append((entry, source_id))
        refers_to_image = bool(
            re.search(
                r"\b(image|screenshot|picture|photo|diagram|attachment|shown|above|look at|see this)\b",
                prompt,
                re.I,
            )
        )
        if not include_previous and not refers_to_image:
            return context
        for entry, source_id in reversed(candidates):
            try:
                if snapshots is not None:
                    if (
                        snapshots.get(str(source_id), {}).get("source_attachments")
                        == []
                    ):
                        # Frozen text needs no Discord lookup once we know this
                        # history entry supplied no attachment bytes.
                        continue
                    source = await channel.fetch_message(int(source_id))
                    snapshot = self.source_snapshot(source)
                    if (
                        str(source_id) in snapshots
                        and snapshots[str(source_id)] != snapshot
                    ):
                        raise ValueError(
                            "Earlier image source changed; review the saved input before continuing."
                        )
                    snapshots[str(source_id)] = snapshot
                    parts = await self.source_image_parts(
                        channel, source_id, budget, source=source, refresh=True
                    )
                else:
                    parts = await self.source_image_parts(channel, source_id, budget)
            except (
                discord.HTTPException,
                ValueError,
                OSError,
                asyncio.TimeoutError,
            ) as error:
                if snapshots is not None:
                    # Never combine completed connector results with substituted input.
                    if isinstance(error, discord.NotFound):
                        raise ValueError(
                            "An earlier image source is no longer available. Review the saved input before continuing."
                        ) from error
                    raise
                content = entry.get("content", "")
                detail = (
                    ("Discord HTTP " + str(error.status))
                    if isinstance(error, discord.HTTPException)
                    else str(error)
                )
                note = {
                    "type": "text",
                    "text": "Earlier image input unavailable: "
                    + (detail or type(error).__name__),
                }
                entry["content"] = (
                    [{"type": "text", "text": content}]
                    if isinstance(content, str)
                    else list(content)
                ) + [note]
                continue
            if parts:
                content = entry.get("content", "")
                entry["content"] = (
                    [{"type": "text", "text": content}]
                    if isinstance(content, str)
                    else list(content)
                ) + parts
        return context

    def bot_role_ids(self, guild):
        return {
            role.id
            for role in getattr(getattr(guild, "me", None), "roles", ())
            if getattr(role, "managed", False)
            and getattr(getattr(role, "tags", None), "bot_id", None) == self.user.id
        }

    def mentioned_directly(self, message):
        return self.user in message.mentions or bool(
            self.bot_role_ids(message.guild)
            & {int(value) for value in re.findall(r"<@&(\d+)>", message.content)}
        )

    def normalize_prompt(self, content, guild=None):
        prompt = re.sub(rf"<@!?{self.user.id}>", "", content)
        roles = self.bot_role_ids(guild)
        return re.sub(
            r"<@&(\d+)>",
            lambda match: "" if int(match[1]) in roles else match[0],
            prompt,
        ).strip()

    def normalized_source_prompt(self, source):
        prompt = self.normalize_prompt(
            source.content,
            getattr(source, "guild", getattr(source.channel, "guild", None)),
        )
        return prompt or (
            "Describe the attached image." if has_image_attachment(source) else ""
        )

    def source_snapshot(self, source):
        return {
            "source_prompt": self.normalized_source_prompt(source),
            "source_edited_at": source.edited_at.isoformat()
            if source.edited_at
            else None,
            "source_attachments": [
                [
                    str(getattr(item, "id", "")),
                    item.filename,
                    getattr(item, "size", None),
                ]
                for item in source.attachments
            ],
        }

    def source_changed(self, turn, request, source):
        snapshot = self.source_snapshot(source)
        prior_prompt = self.normalize_prompt(
            request.get("source_prompt", turn["prompt"]), source.channel.guild
        )
        if (
            not prior_prompt
            and snapshot["source_prompt"] == "Describe the attached image."
        ):
            prior_prompt = snapshot["source_prompt"]
        return prior_prompt != snapshot["source_prompt"] or (
            "source_attachments" in request
            and request["source_attachments"] != snapshot["source_attachments"]
        )

    def queue_turn_operation(self, key, kind, **changes):
        turn = self.store.turn(key)
        if not turn:
            return
        previous = json.loads(turn.get("operation_json") or "{}")
        if (
            kind == "refresh"
            and previous.get("kind") == "refresh"
            and previous.get("native_snapshot") == changes.get("native_snapshot")
        ):
            return
        # A source deletion cannot be undone by a delayed edit callback.
        if previous.get("kind") == "delete":
            kind, changes = "delete", {}
        operation = {
            "id": uuid.uuid4().hex,
            "kind": kind,
            "resume_status": previous.get("resume_status", turn["status"]),
            "resume_detail": previous.get("resume_detail", turn.get("recovery_detail")),
            "origin_snapshot": previous.get("native_snapshot")
            or {
                name: value
                for name, value in json.loads(turn.get("request_json") or "{}").items()
                if name in {"source_prompt", "source_attachments", "source_edited_at"}
            },
            **changes,
        }
        self.store.checkpoint_turn(
            key,
            status="deleting" if kind == "delete" else "revising",
            operation=operation,
            detail="Reconciling deleted source."
            if kind == "delete"
            else "Reconciling revised source before replacing its answer.",
        )
        self.cancel_turn(key)
        # An older edit must be able to acquire the room lock even when a later
        # request is waiting on a provider. Its checkpoint remains intact.
        for later_key in list(self.active_turns):
            later = self.store.turn(later_key)
            if later and later["room"] == turn["room"] and later["seq"] > turn["seq"]:
                self.cancel_turn(later_key)

    def queue_source_refresh(self, key, *, data=None, source=None):
        """Persist distinct source input; duplicate raw/cached events keep its ID."""
        turn = self.store.turn(key)
        if not turn:
            return False
        operation = json.loads(turn.get("operation_json") or "{}")
        if operation.get("kind") == "delete":
            return False
        request = json.loads(turn.get("request_json") or "{}")
        baseline = dict(
            operation.get("native_snapshot")
            or operation.get("origin_snapshot")
            or request
        )
        baseline.setdefault("source_prompt", turn["prompt"])
        channel = self.get_channel(int(turn["room"]))
        guild = getattr(channel, "guild", None)
        baseline["source_prompt"] = self.normalize_prompt(
            baseline["source_prompt"], guild
        )
        if source is not None:
            snapshot = self.source_snapshot(source)
        else:
            snapshot = {
                name: value
                for name, value in baseline.items()
                if name.startswith("source_")
            }
            data = data or {}
            if "content" in data:
                if data["content"] == operation.get("bot_edit_content"):
                    return False
                snapshot["source_prompt"] = self.normalize_prompt(
                    data["content"], guild
                )
            if "attachments" in data:
                known = {
                    item[0]: item for item in baseline.get("source_attachments", [])
                }
                snapshot["source_attachments"] = [
                    [
                        str(item.get("id", "")),
                        item.get(
                            "filename",
                            known.get(str(item.get("id", "")), ["", "", None])[1],
                        ),
                        item.get(
                            "size",
                            known.get(str(item.get("id", "")), ["", "", None])[2],
                        ),
                    ]
                    for item in data["attachments"]
                ]
            if not snapshot["source_prompt"] and any(
                Path(item[1]).suffix.lower()
                in {".png", ".jpg", ".jpeg", ".gif", ".webp"}
                for item in snapshot.get("source_attachments", [])
            ):
                snapshot["source_prompt"] = "Describe the attached image."
        if (
            not baseline["source_prompt"]
            and snapshot["source_prompt"] == "Describe the attached image."
        ):
            baseline["source_prompt"] = snapshot["source_prompt"]
        if baseline["source_prompt"] == snapshot["source_prompt"] and (
            "source_attachments" not in baseline
            or "source_attachments" not in snapshot
            or baseline["source_attachments"] == snapshot["source_attachments"]
        ):
            return False
        self.queue_turn_operation(key, "refresh", native_snapshot=snapshot)
        return True

    def cancel_turn(self, key, status=None):
        task = self.active_turns.get(key)
        if status:
            self.store.checkpoint_turn(key, status=status)
        if task and task is not asyncio.current_task():
            task.cancel()

    def cancel_room_turns(self, room):
        for turn in self.store.pending_turns():
            if turn["room"] == str(room):
                self.cancel_turn(turn["key"], "reset")

    def turn_event(self, kind, payload):
        emit = getattr(self, "emit", None)
        if emit:
            emit(kind, payload)

    async def _recover_waiting_turns(self):
        while not self.is_closed():
            await asyncio.sleep(5)
            await self.recover_pending_turns()

    async def recover_pending_turns(self):
        if self.recovery_scan_running or getattr(self, "instance", {}).get("dry_run"):
            return
        self.recovery_scan_running = True
        try:
            for turn in self.store.pending_turns():
                if turn["key"] in self.active_turns:
                    continue
                task = asyncio.create_task(self._resume_turn(turn["key"]))
                self.recovery_tasks.add(task)
                self.active_turns[turn["key"]] = task
                task.add_done_callback(self.recovery_tasks.discard)
        finally:
            self.recovery_scan_running = False

    async def _apply_turn_operation(self, channel, turn, source):
        key = turn["key"]
        operation = json.loads(turn.get("operation_json") or "{}")
        if not operation:
            return True
        request = json.loads(turn.get("request_json") or "{}")
        checkpoint = json.loads(turn.get("checkpoint_json") or "{}")
        source_snapshot = self.source_snapshot(source) if source else None
        deleting = operation["kind"] == "delete"
        if (
            operation["kind"] == "refresh"
            and source
            and not self.source_changed(turn, request, source)
        ):
            # Discord also emits unchanged content/attachment updates. Compare the
            # fetched source before any output reconciliation, deletion or rewind.
            request.update(self.source_snapshot(source), source_initialized=True)
            status = operation.get("resume_status")
            if status not in {
                "complete",
                "pending",
                "recovering",
                "waiting",
                "waiting_user",
                "delivering",
                "response_deleted",
            }:
                status = (
                    "waiting_user"
                    if checkpoint.get("tool_in_flight")
                    else (
                        "delivering"
                        if checkpoint.get("answer")
                        else "complete"
                        if turn["version"]
                        else "recovering"
                    )
                )
            with self.store.db:
                self.store.db.execute(
                    "UPDATE turns SET operation_json=NULL,status=?,recovery_detail=?,request_json=? WHERE key=?",
                    (status, operation.get("resume_detail"), json.dumps(request), key),
                )
            return True
        if checkpoint.get("tool_in_flight"):
            raise ValueError(
                "Connector acceptance is uncertain. Use /reconcile with its checked result before applying the saved source change."
            )
        outputs = await self._delivery_outputs(channel, turn)
        if not deleting and isinstance(channel, discord.Thread):
            anchor = turn["source_id"] or turn.get("anchor_id")
            if not anchor:
                raise ValueError("A thread rewind requires a reliable message anchor.")
            outputs = [
                message
                async for message in channel.history(
                    limit=None, after=discord.Object(id=int(anchor))
                )
            ]
        await self.delete_messages(channel, outputs, preserve_turn_key=key)
        # A newer source event can arrive while Discord cleanup is in progress.
        current = self.store.turn(key)
        if (
            not current
            or json.loads(current.get("operation_json") or "{}").get("id")
            != operation["id"]
        ):
            return False
        if deleting:
            self.store.remove_message(turn["source_id"])
            return False
        if source and self.source_changed(turn, source_snapshot, source):
            self.queue_source_refresh(key, source=source)
            return False
        revised = operation.get("prompt")
        if revised is None:
            revised = (
                source_snapshot["source_prompt"]
                if source and not source.author.bot
                else turn["prompt"]
            )
        if source:
            request.update(source_snapshot, source_initialized=True)
            if source.author.id == self.user.id and operation.get("prompt") is not None:
                operation["bot_edit_content"] = (
                    f"**{turn['author_name']}**\n{revised[:1800]}"
                )
                self.store.checkpoint_turn(key, operation=operation)
                edited = await source.edit(content=operation["bot_edit_content"])
                request.update(self.source_snapshot(edited or source))
                current = self.store.turn(key)
                if (
                    not current
                    or json.loads(current.get("operation_json") or "{}").get("id")
                    != operation["id"]
                ):
                    return False
        request.pop("execution_history", None)
        request.pop("history_image_snapshots", None)
        request["prompt_annotation"] = (
            revised if operation.get("prompt") is not None else None
        )
        if isinstance(channel, discord.Thread):
            for later in self.store.db.execute(
                "SELECT key FROM turns WHERE room=? AND seq>?",
                (turn["room"], turn["seq"]),
            ).fetchall():
                self.cancel_turn(later["key"], "superseded")
        self.store.rewind(
            key,
            revised,
            truncate=isinstance(channel, discord.Thread),
            request=request,
            model=operation.get("model"),
        )
        return True

    async def _resume_turn(self, key, channel=None, send=None):
        turn = self.store.turn(key)
        if not turn:
            return
        room = turn["room"]
        self.active_turns[key] = asyncio.current_task()
        tracked = getattr(self, "turn_tasks", None)
        if tracked is not None:
            tracked.add(asyncio.current_task())
        try:
            channel = (
                channel
                or self.get_channel(int(room))
                or await self.fetch_channel(int(room))
            )
            if hasattr(self, "allowed") and not self.allowed(channel):
                return
            if getattr(self, "cleanup_running", lambda: False)():
                return
            async with self.room_locks.setdefault(room, asyncio.Lock()):
                turn = self.store.turn(key)
                if not turn or turn["status"] not in {
                    "pending",
                    "recovering",
                    "waiting",
                    "delivering",
                    "revising",
                    "deleting",
                }:
                    return
                earlier_operation = self.store.db.execute(
                    "SELECT key FROM turns WHERE room=? AND seq<? AND operation_json IS NOT NULL AND status!='reset' ORDER BY seq LIMIT 1",
                    (room, turn["seq"]),
                ).fetchone()
                if earlier_operation:
                    self.store.checkpoint_turn(
                        key,
                        detail="Waiting for an earlier source change to be reconciled.",
                    )
                    return
                source = None
                request = json.loads(turn.get("request_json") or "{}")
                operation = json.loads(turn.get("operation_json") or "{}")
                if turn["source_id"] and operation.get("kind") != "delete":
                    try:
                        source = await channel.fetch_message(int(turn["source_id"]))
                    except discord.NotFound:
                        self.queue_turn_operation(key, "delete")
                    else:
                        snapshot = self.source_snapshot(source)
                        initialized = request.get(
                            "source_initialized", "source_prompt" in request
                        )
                        changed = initialized and self.source_changed(
                            turn, request, source
                        )
                        if initialized and (changed or operation):
                            self.queue_source_refresh(key, source=source)
                        elif not initialized:
                            if not source.author.bot:
                                with self.store.db:
                                    self.store.db.execute(
                                        "UPDATE turns SET prompt=? WHERE key=?",
                                        (snapshot["source_prompt"], key),
                                    )
                            request.update(snapshot, source_initialized=True)
                            self.store.checkpoint_turn(key, request=request)
                turn = self.store.turn(key)
                if not await self._apply_turn_operation(channel, turn, source):
                    return
                current = self.store.turn(key)
                if not current or current["status"] not in {
                    "pending",
                    "recovering",
                    "waiting",
                    "delivering",
                }:
                    return current
                return await self._complete_pending_turn(
                    key, channel, send or channel.send, source=source
                )
        except asyncio.CancelledError:
            raise
        except ValueError as error:
            self.store.checkpoint_turn(
                key, status="waiting_user", detail=safe_error(error)
            )
            self.turn_event(
                "turn.waiting_user",
                {"turn_key": key, "channel_id": room, "detail": safe_error(error)},
            )
        except Exception as error:
            current = self.store.turn(key)
            if current:
                # Delivery and source operations are phases, not transient errors.
                phase = (
                    current["status"]
                    if current["status"] in {"delivering", "revising", "deleting"}
                    else "waiting"
                )
                self.store.checkpoint_turn(
                    key,
                    status=phase,
                    detail="Discord recovery unavailable: " + type(error).__name__,
                )
            self.turn_event(
                "turn.waiting",
                {"turn_key": key, "channel_id": room, "reason": type(error).__name__},
            )
        finally:
            if self.active_turns.get(key) is asyncio.current_task():
                self.active_turns.pop(key, None)
            if tracked is not None:
                tracked.discard(asyncio.current_task())
        return self.store.turn(key)

    async def _delivery_outputs(self, channel, turn):
        outputs = []
        for output_id in json.loads(turn["output_ids"]):
            try:
                outputs.append(await channel.fetch_message(int(output_id)))
            except discord.NotFound:
                pass
        if outputs or not json.loads(turn.get("checkpoint_json") or "{}").get("answer"):
            return outputs
        anchor = turn["source_id"] or turn.get("anchor_id")
        if not anchor:
            raise ValueError(
                "Discord delivery is uncertain; review the conversation before retrying."
            )
        count = 0
        async for message in channel.history(
            limit=101, after=discord.Object(id=int(anchor))
        ):
            count += 1
            if message.author.id == self.user.id and any(
                (getattr(component, "custom_id", "") or "").startswith(
                    f"turn:{turn['key']}:"
                )
                for row in getattr(message, "components", [])
                for component in row.children
            ):
                return [message]
        if count > 100:
            raise ValueError(
                "Discord delivery is uncertain; review the conversation before retrying."
            )
        return []

    async def _complete_pending_turn(
        self, key, channel, send, *, source=None, prompt_annotation=None
    ):
        turn = self.store.turn(key)
        if not turn:
            return None
        room = turn["room"]
        request = json.loads(turn.get("request_json") or "{}")
        prompt_annotation = prompt_annotation or request.get("prompt_annotation")
        ambient = request.get("ambient", False)
        epoch = self.room_epochs.get(room, 0)
        ambient_sequence = self.ambient_sequences.get(room, 0)
        checkpoint = json.loads(turn.get("checkpoint_json") or "{}")
        self.active_turns[key] = asyncio.current_task()

        async def progress(detail):
            current = self.store.turn(key)
            if (
                not current
                or current["status"]
                in {
                    "superseded",
                    "reset",
                    "response_deleted",
                }
                or current.get("operation_json")
            ):
                raise asyncio.CancelledError()
            self.store.checkpoint_turn(key, status="recovering", detail=detail)
            self.turn_event(
                "turn.recovering",
                {"turn_key": key, "channel_id": room, "detail": detail},
            )

        async def save_checkpoint(data):
            current = self.store.turn(key)
            if (
                not current
                or current.get("operation_json")
                or current["status"] == "reset"
            ):
                raise asyncio.CancelledError()
            self.store.checkpoint_turn(key, checkpoint=data)

        try:
            # A response is checkpointed before delivery. On restart, reconcile a possibly
            # accepted Discord send by its stable controls before considering another send.
            if checkpoint.get("answer"):
                outputs = await self._delivery_outputs(channel, turn)
                if outputs:
                    self.store.finish_turn(
                        key,
                        turn["prompt"],
                        checkpoint["selected"],
                        checkpoint["answer"],
                        [message.id for message in outputs],
                        revision=turn["version"] > 0,
                    )
                    return self.store.turn(key)
            if checkpoint.get("answer"):
                selected, answer, raw = (
                    checkpoint["selected"],
                    checkpoint["answer"],
                    checkpoint["raw"],
                )
            else:
                context = request.get("execution_history", request.get("history"))
                if context is None:
                    context = self.store.history(
                        room,
                        exclude_source=turn["source_id"],
                        exclude_turn=key,
                        with_sources=True,
                        before_turn=key,
                    )
                if "execution_history" not in request:
                    request["execution_history"] = context
                    self.store.checkpoint_turn(key, request=request)
                budget = {"bytes": 16 * 1024 * 1024}
                if checkpoint.get("tool_in_flight"):
                    raise ValueError(
                        "A connector's acceptance is uncertain. Review it before requesting another call."
                    )
                images = (
                    await self.source_image_parts(
                        channel, turn["source_id"], budget, source=source, refresh=True
                    )
                    if turn["source_id"]
                    else []
                )
                context = await self.image_history(
                    channel,
                    context,
                    budget,
                    prompt=turn["prompt"],
                    include_previous=bool(images)
                    or isinstance(channel, discord.Thread),
                    snapshots=request.setdefault("history_image_snapshots", {}),
                )
                self.store.checkpoint_turn(key, request=request)
                async with channel.typing():
                    selected, answer, raw = await self.engine.ask(
                        room,
                        turn["prompt"],
                        turn["author_name"],
                        turn["model"],
                        turn["skill"],
                        history=context,
                        images=images,
                        persist=False,
                        casual=not isinstance(channel, discord.Thread),
                        ambient=ambient,
                        participation=self.participation(room)["level"],
                        progress=progress,
                        checkpoint=save_checkpoint,
                        resume=checkpoint,
                    )
            current = self.store.turn(key)
            if (
                not current
                or current["status"] in {"superseded", "reset", "response_deleted"}
                or current.get("operation_json")
                or epoch != self.room_epochs.get(room, 0)
            ):
                return None
            if ambient and (
                ambient_sequence != self.ambient_sequences.get(room, 0)
                or answer.strip() == "[SILENT]"
            ):
                self.store.discard_pending(key)
                return None
            checkpoint = {"selected": selected, "answer": answer, "raw": raw}
            self.store.checkpoint_turn(
                key,
                status="delivering",
                checkpoint=checkpoint,
                detail="Delivering the completed answer.",
            )
            # Deferred interaction tokens expire during an outage; channel delivery remains valid.
            if request.get("interaction"):
                send = channel.send
            output = await self.render_turn(
                key,
                selected,
                answer,
                raw,
                send=send,
                version=turn["version"] + 1,
                prompt_annotation=prompt_annotation,
            )
            current = self.store.turn(key)
            if (
                not current
                or current.get("operation_json")
                or current["status"] == "reset"
            ):
                return None
            self.store.finish_turn(
                key,
                turn["prompt"],
                selected,
                answer,
                [output.id],
                revision=turn["version"] > 0,
            )
            if ambient:
                self.last_ambient_reply[room] = time.monotonic()
            self.turn_event(
                "turn.resumed", {"turn_key": key, "channel_id": room, "model": selected}
            )
        except asyncio.CancelledError:
            current = self.store.turn(key)
            if (
                current
                and not current.get("operation_json")
                and current["status"] in {"pending", "recovering", "waiting"}
            ):
                self.store.checkpoint_turn(
                    key,
                    status="recovering",
                    detail="Interrupted; will resume after reconnect.",
                )
            raise
        except ValueError as error:
            self.store.checkpoint_turn(
                key, status="waiting_user", detail=safe_error(error)
            )
            self.turn_event(
                "turn.waiting_user",
                {"turn_key": key, "channel_id": room, "detail": safe_error(error)},
            )
        except Exception as error:
            current = self.store.turn(key)
            if current and current["status"] not in {
                "delivering",
                "revising",
                "deleting",
            }:
                self.store.checkpoint_turn(
                    key,
                    status="waiting",
                    detail="Waiting for Discord or request resources: "
                    + type(error).__name__,
                )
            self.turn_event(
                "turn.waiting",
                {"turn_key": key, "channel_id": room, "reason": type(error).__name__},
            )
        finally:
            if self.active_turns.get(key) is asyncio.current_task():
                self.active_turns.pop(key, None)
        return self.store.turn(key)

    async def run_turn(
        self,
        channel,
        prompt,
        author,
        send,
        model=None,
        skill=None,
        source_id=None,
        history=None,
        ambient=False,
        anchor_id=None,
    ):
        room = str(channel.id)
        existing = self.store.turn_by_source(source_id) if source_id else None
        if existing and existing["room"] == room:
            return existing
        selected = self.engine.resolve_model(
            model or self.store.model(room, self.config["default_model"])
        )
        key = uuid.uuid4().hex
        # No awaited lookup precedes durable enqueue. Native observe() may already
        # contain this source, so losing it here would prevent any later retry.
        boundary = self.store.db.execute(
            "SELECT COALESCE(MAX(id),0) FROM messages WHERE room=?", (room,)
        ).fetchone()[0]
        self.store.begin_turn(
            key,
            room,
            author.id,
            author.display_name,
            prompt,
            selected,
            skill,
            source_id,
            anchor_id,
            request={
                "ambient": ambient,
                "history": history,
                "history_boundary": boundary,
                "interaction": bool(anchor_id and not source_id),
                "source_initialized": False,
            },
        )
        return await self._resume_turn(key, channel, send)

    async def create_conversation(
        self, channel, title, author, participants=(), model=None
    ):
        selected = self.engine.resolve_model(
            model or self.store.model(str(channel.id), self.config["default_model"])
        )
        if isinstance(channel, discord.Thread):
            thread = channel
        elif isinstance(channel, discord.TextChannel):
            thread = await channel.create_thread(
                name=title[:100] or "AI conversation",
                type=discord.ChannelType.public_thread,
                auto_archive_duration=1440,
            )
        else:
            raise ValueError("Start a conversation in a text channel or thread.")
        people = {
            member.id: member for member in [author, *participants] if not member.bot
        }
        for member in people.values():
            await thread.add_user(member)
        self.store.conversation(str(thread.id), title, str(author.id))
        self.store.set_model(str(thread.id), selected)
        mentions = " ".join(member.mention for member in people.values())
        await thread.send(
            f"{mentions}\n**{selected}** · Chat naturally here. Every participant’s message gets a direct answer.\n"
            "This thread has its own context. Use `/history` to inspect it, "
            "`/model` to switch routes, and `/reset` to start fresh. Answers have Retry and Edit prompt buttons.",
            allowed_mentions=discord.AllowedMentions(
                users=list(people.values()), roles=False, everyone=False
            ),
        )
        return thread

    async def revise_turn(
        self, key, actor, prompt=None, model=None, expected_version=None
    ):
        turn = self.store.turn(key)
        if not turn:
            raise ValueError("That conversation turn is no longer available.")
        if expected_version is not None and turn["version"] != expected_version:
            raise ValueError(
                "That answer has already changed. Use its current controls."
            )
        if prompt is not None and str(actor.id) != turn["author_id"]:
            raise ValueError("Only the prompt author can edit their prompt.")
        selected = self.engine.resolve_model(model) if model else None
        if (
            prompt is None
            and turn["status"]
            in {"pending", "recovering", "waiting", "waiting_user", "delivering"}
            and not turn.get("operation_json")
        ):
            checkpoint = json.loads(turn.get("checkpoint_json") or "{}")
            if checkpoint.get("tool_in_flight"):
                raise ValueError(
                    "Connector acceptance is uncertain. After reviewing the connector, use /reconcile with its observed result."
                )
            if selected:
                with self.store.db:
                    self.store.db.execute(
                        "UPDATE turns SET model=? WHERE key=?", (selected, key)
                    )
            self.store.checkpoint_turn(
                key, status="delivering" if checkpoint.get("answer") else "recovering"
            )
            self.cancel_turn(key)
            await self._resume_turn(key)
            return self.get_channel(int(turn["room"]))
        self.queue_turn_operation(key, "revise", prompt=prompt, model=selected)
        await self._resume_turn(key)
        return self.get_channel(int(turn["room"]))

    async def reconcile_turn(self, key, actor, result):
        turn = self.store.turn(key)
        if not turn or str(actor.id) != turn["author_id"]:
            raise ValueError("Only the prompt author can reconcile this request.")
        if not result.strip() or len(result) > 20000:
            raise ValueError(
                "Include the connector result you checked, under 20,000 characters."
            )
        self.cancel_turn(key)
        async with self.room_locks.setdefault(turn["room"], asyncio.Lock()):
            turn = self.store.turn(key)
            checkpoint = json.loads(turn.get("checkpoint_json") or "{}")
            marker = checkpoint.get("tool_in_flight")
            if not marker:
                raise ValueError(
                    "This request has no uncertain connector call to reconcile."
                )
            messages = checkpoint.get("messages", [])
            completed = {
                item.get("tool_call_id")
                for item in messages
                if item.get("role") == "tool"
            }
            calls = [
                call
                for item in messages
                for call in item.get("tool_calls", [])
                if call.get("id") not in completed
            ]
            call = marker if isinstance(marker, dict) else (calls[0] if calls else None)
            if not call or not call.get("id"):
                raise ValueError(
                    "The saved connector call lacks an identity; review its raw checkpoint before continuing."
                )
            reported = json.dumps(
                {
                    "reconciliation": "user_reported",
                    "reported_by": str(actor.id),
                    "result": result,
                }
            )
            messages.append(
                {"role": "tool", "tool_call_id": call["id"], "content": reported}
            )
            checkpoint.setdefault("trace", []).append(
                {"tool": call["function"]["name"], "result": reported}
            )
            checkpoint["pending_tools"] = [
                item
                for item in checkpoint.get("pending_tools", calls)
                if item.get("id") != call["id"]
            ]
            checkpoint["tool_in_flight"] = False
            operation = json.loads(turn.get("operation_json") or "{}")
            status = (
                ("deleting" if operation.get("kind") == "delete" else "revising")
                if operation
                else "recovering"
            )
            self.store.checkpoint_turn(
                key,
                status=status,
                checkpoint=checkpoint,
                detail="Connector result recorded from the prompt author; continuing saved work.",
            )
        await self._resume_turn(key)

    async def delete_messages(self, channel, targets, preserve_turn_key=None):
        targets = [
            message
            for message in targets
            if not (
                callable(getattr(message, "is_system", None)) and message.is_system()
            )
            and getattr(message, "type", None)
            not in {
                discord.MessageType.recipient_add,
                discord.MessageType.recipient_remove,
                discord.MessageType.thread_starter_message,
            }
        ]
        permissions = channel.permissions_for(channel.guild.me)
        if not permissions.manage_messages and any(
            m.author.id != self.user.id for m in targets
        ):
            raise ValueError(
                "Manage Messages permission is required to rewind this thread."
            )
        for message in targets:
            self.internal_deletes.add(message.id)
            try:
                await message.delete()
            except discord.NotFound:
                pass
            except discord.HTTPException as error:
                self.internal_deletes.discard(message.id)
                if error.code == 50021:
                    continue
                raise
            except Exception:
                self.internal_deletes.discard(message.id)
                raise
            row = self.store.db.execute(
                "SELECT turn_key FROM messages WHERE source_id=?", (str(message.id),)
            ).fetchone()
            if not preserve_turn_key or not row or row["turn_key"] != preserve_turn_key:
                self.store.remove_message(message.id)

    async def on_raw_message_delete(self, event):
        self.invalidate_image_cache(event.message_id)
        await self.remove_deleted_message(event.channel_id, event.message_id)

    async def on_raw_bulk_message_delete(self, event):
        for message_id in event.message_ids:
            self.invalidate_image_cache(message_id)
            await self.remove_deleted_message(event.channel_id, message_id)

    async def remove_deleted_message(self, channel_id, message_id):
        if message_id in self.internal_deletes:
            self.internal_deletes.discard(message_id)
            return
        turn = self.store.turn_by_source(message_id)
        if turn:
            self.queue_turn_operation(turn["key"], "delete")
            await self._resume_turn(turn["key"])
        else:
            self.store.remove_message(message_id)

    async def on_raw_message_edit(self, event):
        self.invalidate_image_cache(event.message_id)
        if "content" not in event.data and "attachments" not in event.data:
            return
        turn = self.store.turn_by_source(event.message_id)
        if turn:
            # Persist before fetching the revised source. Failures/restarts resume
            # cleanup from the same operation and retain any uncertain acceptance.
            self.queue_source_refresh(turn["key"], data=event.data)
            active = self.active_turns.get(turn["key"])
            if active and not active.done() and not active.cancelling():
                return
            await self._resume_turn(turn["key"])
            return
        if not self.store.has_message(event.message_id):
            return
        try:
            channel = self.get_channel(event.channel_id) or await self.fetch_channel(
                event.channel_id
            )
            source = await channel.fetch_message(event.message_id)
            if source.author.bot:
                return
            snapshot = self.source_snapshot(source)
            self.store.observe(
                str(event.channel_id),
                source.id,
                source.author.display_name,
                snapshot["source_prompt"],
                snapshot["source_edited_at"],
            )
            self.schedule_ambient(source)
        except Exception as error:
            logging.error(
                "Observed message edit unavailable (%s)", type(error).__name__
            )

    async def generate_media(self, interaction, kind, prompt, model):
        await interaction.response.defer(thinking=True)
        async with self.slots, interaction.channel.typing():
            selected, payload = await self.engine.generate(kind, prompt, model)
        raw_json = json.dumps(payload, ensure_ascii=False, indent=2)
        raw_data = raw_json.encode()
        raw_filename = "raw-response.json"
        if len(raw_data) > interaction.guild.filesize_limit:
            raw_data = gzip.compress(raw_data)
            raw_filename = "raw-response.json.gz"
        raw_file = None
        if len(raw_data) <= interaction.guild.filesize_limit:
            raw_file = discord.File(io.BytesIO(raw_data), filename=raw_filename)
        items = payload.get("data", [])
        if isinstance(items, dict):
            items = items.get("data", [])
        if not isinstance(items, list):
            items = []
        delivered = 0
        for item in items[:10]:
            if item.get("b64_json"):
                data = base64.b64decode(item["b64_json"], validate=True)
                if len(data) > interaction.guild.filesize_limit:
                    await interaction.followup.send(
                        f"The generated {kind} exceeds this server’s upload limit. Its full result is available in raw JSON."
                    )
                    continue
                extension = item.get("format", "png" if kind == "image" else "mp4")
                if data.startswith(b"\xff\xd8\xff"):
                    extension = "jpg"
                elif data.startswith(b"\x89PNG\r\n\x1a\n"):
                    extension = "png"
                elif data.startswith((b"GIF87a", b"GIF89a")):
                    extension = "gif"
                elif data[:4] == b"RIFF" and data[8:12] == b"WEBP":
                    extension = "webp"
                if extension not in {
                    "png",
                    "jpg",
                    "jpeg",
                    "webp",
                    "gif",
                    "mp4",
                    "webm",
                    "mov",
                }:
                    extension = "png" if kind == "image" else "mp4"
                await interaction.followup.send(
                    file=discord.File(
                        io.BytesIO(data), filename=f"{kind}-{delivered + 1}.{extension}"
                    )
                )
                delivered += 1
            elif isinstance(item.get("url"), str) and item["url"].startswith(
                "https://"
            ):
                if kind == "image":
                    await interaction.followup.send(
                        embed=discord.Embed().set_image(url=item["url"])
                    )
                else:
                    await interaction.followup.send(item["url"])
                delivered += 1
        key = uuid.uuid4().hex
        summary = (
            f"**{selected}** · {delivered} {kind} result(s)"
            if delivered
            else f"**{selected}** · Gateway result received. Full JSON is attached."
        )
        self.store.save_raw(key, summary, raw_json)
        kwargs = {"view": self.response_view(key)}
        if raw_file:
            kwargs["file"] = raw_file
        else:
            summary += (
                "\nThe complete JSON exceeds Discord's upload limit after compression."
            )
        await interaction.followup.send(summary, **kwargs)

    async def on_message(self, message):
        excluded = {int(value) for value in self.config.get("excluded_channels", [])}
        if (
            message.channel.id in excluded
            or getattr(message.channel, "parent_id", None) in excluded
        ):
            return
        if (
            message.author.bot
            or not message.guild
            or message.guild.id != int(self.config["guild_id"])
        ):
            return
        if not message.content.strip() and not has_image_attachment(message):
            return
        if self.store.has_message(message.id):
            return
        room = str(message.channel.id)
        self.ambient_sequences[room] = self.ambient_sequences.get(room, 0) + 1
        in_thread = isinstance(message.channel, discord.Thread) and (
            message.channel.owner_id == self.user.id
            or self.store.conversation(str(message.channel.id))
        )
        replied = (
            message.reference
            and isinstance(message.reference.resolved, discord.Message)
            and message.reference.resolved.author.id == self.user.id
        )
        public_channel = (
            isinstance(message.channel, discord.TextChannel)
            and message.channel.permissions_for(message.guild.default_role).view_channel
        )
        directed = in_thread or self.mentioned_directly(message) or replied
        if not directed and not public_channel and not in_thread:
            return
        if self.store.turn_by_source(message.id):
            return
        prompt = self.normalized_source_prompt(message)
        self.store.observe(room, message.id, message.author.display_name, prompt)
        if not directed:
            if public_channel or in_thread:
                self.schedule_ambient(message)
            return
        pending = self.ambient_tasks.pop(room, None)
        if pending:
            pending.cancel()
        if not prompt:
            await message.reply(HELP)
            return
        try:
            await self.run_turn(
                message.channel,
                prompt,
                message.author,
                message.reply,
                source_id=message.id,
            )
        except Exception as error:
            logging.error("Mention request failed (%s)", type(error).__name__)
            await message.reply(f"I couldn't finish that request. {safe_error(error)}")

    def participation(self, room):
        defaults = {"level": "quiet", "cooldown": 60, "delay": 6}
        defaults.update(self.config.get("participation", {}))
        return self.store.participation(room, defaults)

    def schedule_ambient(self, message):
        room = str(message.channel.id)
        settings = self.participation(room)
        pending = self.ambient_tasks.pop(room, None)
        if pending:
            pending.cancel()
        mentioned_roles = {
            int(value) for value in re.findall(r"<@&(\d+)>", message.content)
        }
        mentioned_roles.update(
            role.id for role in getattr(message, "role_mentions", ())
        )
        addressed_elsewhere = (
            any(user.id != self.user.id for user in message.mentions)
            or bool(mentioned_roles - self.bot_role_ids(message.guild))
            or getattr(message, "mention_everyone", False)
            or bool(re.search(r"(?<!\w)@(everyone|here)\b", message.content))
        )
        eligible = "?" in message.content or re.search(
            r"\b(why|how|what|who|help|explain|suggest|write|make|build|generate|ideas|thoughts|reply|tell|show|find|check|create)\b",
            message.content,
            re.I,
        )
        if settings["level"] == "balanced":
            eligible = eligible or (
                len(message.content.split()) >= 12
                and re.search(
                    r"\b(code|model|engine|bug|design|implementation|performance|game|API|build)\b",
                    message.content,
                    re.I,
                )
            )
        if settings["level"] == "lively":
            eligible = eligible or len(message.content.split()) >= 8
        if (
            settings["level"] == "off"
            or addressed_elsewhere
            or (not pending and not eligible)
        ):
            return

        async def participate():
            delay = max(
                settings["delay"],
                settings["cooldown"]
                - (time.monotonic() - self.last_ambient_reply.get(room, 0)),
            )
            await asyncio.sleep(delay)
            self.ambient_tasks.pop(room, None)
            try:
                await self.run_turn(
                    message.channel,
                    message.content,
                    message.author,
                    message.channel.send,
                    source_id=message.id,
                    ambient=True,
                )
            except Exception as error:
                logging.warning(
                    "Ambient participation skipped (%s)", type(error).__name__
                )

        self.ambient_tasks[room] = asyncio.create_task(participate())

    def register_commands(self):
        tree, engine, store = self.tree, self.engine, self.store

        async def autocomplete(interaction, current):
            values = engine.models
            return [
                app_commands.Choice(name=v[:100], value=v)
                for v in values
                if current.lower() in v.lower() and len(v) <= 100
            ][:25]

        async def media_autocomplete(interaction, current):
            kind = interaction.command.name
            models = engine.media_models(kind)
            return [
                app_commands.Choice(name=value[:100], value=value)
                for value in models
                if current.lower() in value.lower() and len(value) <= 100
            ][:25]

        async def skill_autocomplete(interaction, current):
            return [
                app_commands.Choice(name=v, value=v)
                for v in engine.skills
                if current.lower() in v.lower()
            ][:25]

        async def tool_autocomplete(interaction, current):
            return [
                app_commands.Choice(name=v, value=v)
                for v in engine.tools
                if current.lower() in v.lower()
            ][:25]

        @tree.command(
            name="help",
            description="Quick guide to chatting, models, skills and connectors",
        )
        async def help_command(i: discord.Interaction):
            await i.response.send_message(HELP)

        @tree.command(
            name="chat", description="Ask AI together in this channel or thread"
        )
        @app_commands.autocomplete(model=autocomplete, skill=skill_autocomplete)
        async def chat(
            i: discord.Interaction, message: str, model: str = None, skill: str = None
        ):
            await i.response.defer(thinking=True)
            await self.run_turn(
                i.channel,
                message,
                i.user,
                i.followup.send,
                model,
                skill,
                anchor_id=i.id,
            )

        @tree.command(
            name="new", description="Start a shared AI thread with its own conversation"
        )
        @app_commands.autocomplete(model=autocomplete)
        async def new(
            i: discord.Interaction, title: str = "AI conversation", model: str = None
        ):
            await i.response.defer(thinking=True)
            thread = await self.create_conversation(
                i.channel, title, i.user, model=model
            )
            await i.followup.send(f"Ready in {thread.mention}")

        @tree.command(
            name="thread",
            description="Start a shared AI conversation and invite your friends",
        )
        @app_commands.autocomplete(model=autocomplete)
        @app_commands.describe(
            with_user="Invite a friend",
            participants="Additional @mentions to invite",
            message="Optional opening prompt",
        )
        async def thread_command(
            i: discord.Interaction,
            with_user: discord.Member = None,
            title: str = "AI conversation",
            message: str = None,
            participants: str = "",
            model: str = None,
        ):
            await i.response.defer(thinking=True)
            people = [with_user] if with_user else []
            for member_id in dict.fromkeys(re.findall(r"<@!?(\d+)>", participants)):
                member = i.guild.get_member(
                    int(member_id)
                ) or await i.guild.fetch_member(int(member_id))
                people.append(member)
            thread = await self.create_conversation(
                i.channel, title, i.user, people, model
            )
            await i.followup.send(
                f"Your conversation is ready in {thread.mention}. Chat normally there without mentioning me."
            )
            if message:
                opening = await thread.send(
                    f"**{i.user.display_name}**\n{message[:1800]}",
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                await self.run_turn(
                    thread,
                    message,
                    i.user,
                    thread.send,
                    model=model,
                    source_id=opening.id,
                )

        tree.add_command(
            app_commands.Command(
                name="convo",
                description="Start a shared AI conversation and invite your friends",
                callback=thread_command.callback,
            )
        )

        @tree.command(
            name="retry", description="Regenerate this conversation's latest answer"
        )
        @app_commands.autocomplete(model=autocomplete)
        async def retry(i: discord.Interaction, model: str = None):
            turn = store.latest_turn(str(i.channel_id))
            if not turn:
                raise ValueError(
                    "Send a new prompt first. New answers have persistent Retry and Edit prompt controls."
                )
            await i.response.defer(thinking=True)
            await self.revise_turn(
                turn["key"], i.user, model=model, expected_version=turn["version"]
            )

        @tree.command(
            name="reconcile",
            description="Resume an uncertain connector call using the result you checked",
        )
        async def reconcile(i: discord.Interaction, result: str):
            turn = store.latest_turn(str(i.channel_id))
            if not turn:
                raise ValueError("There is no saved request in this conversation.")
            await i.response.defer(thinking=True)
            await self.reconcile_turn(turn["key"], i.user, result)
            await i.followup.send(
                "Recorded your checked connector result and resumed the saved request.",
                ephemeral=True,
            )

        @tree.command(
            name="history",
            description="See exactly which recent exchanges this conversation remembers",
        )
        async def history(i: discord.Interaction):
            context = store.history(str(i.channel_id))
            text = (
                "\n\n".join(f"## {m['role']}\n\n{m['content']}" for m in context)
                or "This conversation has no AI exchanges yet."
            )
            data, filename = text.encode(), "conversation-history.md"
            if len(data) > i.guild.filesize_limit:
                data, filename = gzip.compress(data), "conversation-history.md.gz"
            await i.response.send_message(
                f"{len(context)} messages in this conversation’s current AI context. Each channel and thread keeps its own history.",
                file=discord.File(io.BytesIO(data), filename=filename),
            )

        @tree.command(
            name="participation",
            description="Tune how often I join this conversation without a mention",
        )
        @app_commands.choices(
            level=[
                app_commands.Choice(name=name, value=name.lower())
                for name in ["Off", "Quiet", "Balanced", "Lively"]
            ]
        )
        async def participation(
            i: discord.Interaction,
            level: str = None,
            cooldown: app_commands.Range[int, 0, 3600] = None,
            delay: app_commands.Range[int, 2, 120] = None,
        ):
            room = str(i.channel_id)
            changes = {
                key: value
                for key, value in {
                    "level": level,
                    "cooldown": cooldown,
                    "delay": delay,
                }.items()
                if value is not None
            }
            settings = store.participation(room, self.participation(room), changes)
            pending = self.ambient_tasks.pop(room, None)
            if pending:
                pending.cancel()
            self.ambient_sequences[room] = self.ambient_sequences.get(room, 0) + 1
            await i.response.send_message(
                f"Participation **{settings['level']}** · waits {settings['delay']}s for a pause · at least {settings['cooldown']}s between unprompted replies.\nMention me or reply directly to my message for an answer at any level."
            )

        @tree.command(
            name="video",
            description="Generate a video through a connected OmniRoute video route",
        )
        @app_commands.autocomplete(model=media_autocomplete)
        async def video(i: discord.Interaction, prompt: str, model: str = None):
            await self.generate_media(i, "video", prompt, model)

        @tree.command(
            name="model", description="Show or change the AI route for this channel"
        )
        @app_commands.autocomplete(route=autocomplete)
        async def model(i: discord.Interaction, route: str = None):
            room = str(i.channel_id)
            if route:
                selected = engine.resolve_model(route)
                store.set_model(room, selected)
            await i.response.send_message(
                f"This channel uses **{store.model(room, self.config['default_model'])}**."
            )

        @tree.command(
            name="models",
            description="Search the gateway's available providers and routes",
        )
        async def models(i: discord.Interaction, search: str = ""):
            await i.response.defer(thinking=True)
            await engine.refresh_models()
            matches = [v for v in engine.models if search.lower() in v.lower()]
            text = "\n".join(matches)
            report = routing_report(self.config, engine.catalog, matches)
            pipeline = json.dumps(report, ensure_ascii=False, indent=2)
            await i.followup.send(
                f"{len(matches)} matching routes. Use `/model` with autocomplete to choose.\n"
                "The pipeline attachment includes timestamped combo definitions and live catalog metadata. Dynamic routes are marked.",
                files=[
                    discord.File(io.BytesIO(text.encode()), filename="routes.txt"),
                    discord.File(
                        io.BytesIO(pipeline.encode()), filename="routing-pipelines.json"
                    ),
                ],
            )

        @tree.command(
            name="routes", description="List favorites or configure a route alias"
        )
        @app_commands.autocomplete(model=autocomplete)
        async def routes(i: discord.Interaction, alias: str = None, model: str = None):
            if alias or model:
                if not i.permissions.manage_guild:
                    raise ValueError(
                        "Managing route aliases requires Manage Server permission."
                    )
                if (
                    not alias
                    or not model
                    or not re.fullmatch(r"[a-zA-Z0-9_-]{1,40}", alias)
                ):
                    raise ValueError(
                        "Provide an alias using letters, numbers, - or _, and a model."
                    )
                self.config.setdefault("aliases", {})[alias] = engine.resolve_model(
                    model
                )
                save_config(self.config_path, self.config)
            text = (
                "\n".join(
                    f"`{a}` → `{m}`" for a, m in self.config.get("aliases", {}).items()
                )
                or "No aliases configured."
            )
            await i.response.send_message(text[:1900])

        @tree.command(
            name="skills", description="List available built-in and installed skills"
        )
        async def skills(i: discord.Interaction):
            await i.response.send_message(
                "Available skills\n"
                + "\n".join(f"• `{s}`" for s in engine.skills)[:1800]
            )

        @tree.command(name="skill", description="Run a skill with the current AI route")
        @app_commands.autocomplete(name=skill_autocomplete)
        async def skill(i: discord.Interaction, name: str, message: str):
            await i.response.defer(thinking=True)
            await self.run_turn(
                i.channel, message, i.user, i.followup.send, skill=name, anchor_id=i.id
            )

        @tree.command(
            name="connectors",
            description="List tools available from connected MCP plugins",
        )
        async def connectors(i: discord.Interaction):
            await i.response.defer()
            await engine.refresh_tools()
            text = "\n".join(
                f"`{name}` — {(tool.description or '')[:100]}"
                for name, (_, tool) in engine.tools.items()
            )
            await i.followup.send(text[:1900] or "No connector tools are configured.")

        @tree.command(
            name="tool", description="Call a connector or plugin tool directly"
        )
        @app_commands.autocomplete(name=tool_autocomplete)
        async def tool(i: discord.Interaction, name: str, arguments: str = "{}"):
            args = json.loads(arguments)
            if not isinstance(args, dict):
                raise ValueError("Arguments must be a JSON object.")
            await i.response.defer(thinking=True)
            result = await engine.call_tool(name, args)
            payload = json.loads(result)
            failed = payload.get("is_error", False)
            summary = f"**{name}** · {'Connector returned an error' if failed else 'Completed'}"
            key = uuid.uuid4().hex
            store.save_raw(
                key, summary, json.dumps(payload, ensure_ascii=False, indent=2)
            )
            view = discord.ui.View(timeout=None)
            view.add_item(RawJSONButton(key))
            await i.followup.send(summary, view=view)

        @tree.command(
            name="summarize",
            description="Summarize recent messages and decisions in this channel",
        )
        async def summarize(
            i: discord.Interaction, count: app_commands.Range[int, 5, 100] = 30
        ):
            await i.response.defer(thinking=True)
            messages = [
                f"{m.author.display_name}: {m.content[:2000]}"
                async for m in i.channel.history(limit=count)
                if m.content
            ]
            await self.run_turn(
                i.channel,
                "Summarize these channel messages as quoted discussion data.\n"
                + "\n".join(reversed(messages))[:21000],
                i.user,
                i.followup.send,
                skill="summarize",
                anchor_id=i.id,
            )

        @tree.command(
            name="reset", description="Clear this channel's AI conversation context"
        )
        async def reset(i: discord.Interaction):
            await i.response.defer()
            room = str(i.channel_id)
            self.room_epochs[room] = self.room_epochs.get(room, 0) + 1
            self.cancel_room_turns(room)
            async with self.room_locks.setdefault(room, asyncio.Lock()):
                store.reset(str(i.channel_id))
            await i.followup.send(
                "Started fresh. The next message will use a new AI conversation."
            )

        @tree.command(
            name="status",
            description="Show current AI route, gateway and connector status",
        )
        async def status(i: discord.Interaction):
            await i.response.send_message(
                f"Online · {round(self.latency * 1000)} ms\nGateway `{self.config['base_url']}`\nRoute `{store.model(str(i.channel_id), self.config['default_model'])}`\n{len(engine.models)} routes · {len(engine.skills)} skills · {len(engine.tools)} connector tools"
            )

        @tree.error
        async def command_error(i, error):
            error = getattr(error, "original", error)
            logging.error("Command failed (%s)", type(error).__name__)
            text = safe_error(error)
            if i.response.is_done():
                try:
                    await i.followup.send(text)
                except discord.NotFound:
                    await i.channel.send(text)
            else:
                await i.response.send_message(text)


def safe_error(error):
    if isinstance(error, (ValueError, RuntimeError)):
        return str(error)[:1000]
    if isinstance(error, discord.Forbidden):
        return "Discord denied access. Check my permissions for this channel."
    return "The request failed. Try again or use /status to check the current route."


def save_config(path, config):
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(config, indent=2) + "\n")
    temp.chmod(0o600)
    temp.replace(path)


def run():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpx2").setLevel(logging.WARNING)
    path, config = load_config()
    bot = Together(path, config)
    bot.run(Path(config["discord_token_file"]).read_text().strip(), log_handler=None)
