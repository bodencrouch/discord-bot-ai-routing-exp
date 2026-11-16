"""Editable Together AI chat behavior used by the supervised bot hub."""

import asyncio
import io
import json
import re
import time
import uuid

import discord

from .bot import Together


def telemetry_value(value):
    """Keep readable provider/tool traces while excluding binary response bodies."""
    if isinstance(value, dict):
        return {
            key: telemetry_value(item)
            for key, item in value.items()
            if key not in {"b64_json", "base64", "audio", "image_url"}
        }
    if isinstance(value, list):
        return [telemetry_value(item) for item in value]
    if isinstance(value, str):
        if value.startswith("data:"):
            return "[binary data omitted]"
        return value[:48000]
    return value


class DraftReview(discord.ui.View):
    """Record review decisions without executing the proposed output or change."""

    def __init__(self, state, instance_id, draft_id, author_id):
        super().__init__(timeout=86400)
        self.state, self.instance_id = state, instance_id
        self.draft_id, self.author_id = draft_id, author_id

    async def interaction_check(self, interaction):
        return interaction.user.id == self.author_id or bool(
            getattr(
                getattr(interaction.user, "guild_permissions", None),
                "administrator",
                False,
            )
        )

    async def finish(self, interaction, decision):
        self.state.feedback(
            self.draft_id, interaction.user.id, "draft_review", decision
        )
        self.state.record(
            self.instance_id,
            "draft.review",
            {"id": self.draft_id, "decision": decision},
        )
        for item in self.children:
            if item.label == "Approve draft":
                item.label = (
                    "Approved draft" if decision == "approved" else "Approve draft"
                )
            if item.label == "Dismiss":
                item.label = "Dismissed" if decision == "dismissed" else "Dismiss"
            item.disabled = True
        await interaction.response.edit_message(view=self)
        self.stop()

    @discord.ui.button(label="Approve draft", style=discord.ButtonStyle.success)
    async def approve(self, interaction, button):
        await self.finish(interaction, "approved")

    @discord.ui.button(label="Dismiss", style=discord.ButtonStyle.secondary)
    async def dismiss(self, interaction, button):
        await self.finish(interaction, "dismissed")


class HubTogether(Together):
    def __init__(self, path, config, instance, state):
        self.instance, self.hub_state = instance, state
        self.turn_tasks = set()
        super().__init__(path, config)
        self.tree.interaction_check = self.command_check
        original_tool = self.engine.call_tool

        async def call_tool(name, arguments):
            self.emit(
                "tool.started", {"name": name, "arguments": telemetry_value(arguments)}
            )
            if self.instance.get("dry_run"):
                self.emit("tool.proposed", {"name": name})
                return json.dumps(
                    {"dry_run": True, "proposed_tool": name, "executed": False}
                )
            try:
                result = await original_tool(name, arguments)
                try:
                    readable_result = json.loads(result)
                except (TypeError, ValueError):
                    readable_result = result
                self.emit(
                    "tool.completed",
                    {"name": name, "result": telemetry_value(readable_result)},
                )
                return result
            except Exception as error:
                self.emit("tool.failed", {"name": name, "error": type(error).__name__})
                raise

        self.engine.call_tool = call_tool
        original_ask = self.engine.ask

        async def ask(room, prompt, user, *args, **kwargs):
            started = time.monotonic()
            self.emit(
                "turn.started",
                {
                    "channel_id": str(room),
                    "prompt": prompt,
                    "author": user,
                    "dry_run": bool(self.instance.get("dry_run")),
                },
            )
            try:
                selected, answer, trace = await original_ask(
                    room, prompt, user, *args, **kwargs
                )
                responses = [
                    entry
                    for entry in trace
                    if isinstance(entry, dict) and entry.get("choices")
                ]
                actual_model = next(
                    (
                        entry.get("model")
                        for entry in reversed(responses)
                        if entry.get("model")
                    ),
                    selected,
                )
                usage = {}
                for response in responses:
                    response_usage = response.get("usage") or {}
                    if not isinstance(response_usage, dict):
                        continue
                    for key, value in response_usage.items():
                        if isinstance(value, (int, float)):
                            usage[key] = usage.get(key, 0) + value
                self.emit(
                    "turn.completed",
                    {
                        "channel_id": str(room),
                        "prompt": prompt,
                        "author": user,
                        "answer": answer,
                        "model": selected,
                        "actual_model": actual_model,
                        "usage": usage,
                        "trace": telemetry_value(trace),
                        "latency_ms": round((time.monotonic() - started) * 1000),
                        "dry_run": bool(self.instance.get("dry_run")),
                    },
                )
                return selected, answer, trace
            except Exception as error:
                self.emit(
                    "generation.failed",
                    {"channel_id": str(room), "error": type(error).__name__},
                )
                raise

        self.engine.ask = ask
        if instance.get("dry_run"):
            # Persisted answer controls can mutate conversations or produce a live reply.
            from together_ai.bot import RawJSONButton
            from together_ai.conversations import TurnButton

            self.remove_dynamic_items(RawJSONButton, TurnButton)

    def emit(self, kind, payload):
        return self.hub_state.record(self.instance["id"], kind, payload)

    def allowed(self, channel):
        channels = {str(value) for value in self.instance.get("channels", [])}
        return bool(
            channel
            and getattr(channel, "guild", None)
            and str(channel.guild.id) == str(self.config["guild_id"])
            and (
                not channels
                or str(channel.id) in channels
                or str(getattr(channel, "parent_id", "")) in channels
            )
        )

    async def command_check(self, interaction):
        name = getattr(interaction.command, "qualified_name", "unknown")
        self.emit(
            "command.received",
            {
                "name": name,
                "channel_id": str(interaction.channel_id),
                "user_id": str(interaction.user.id),
            },
        )
        if not self.allowed(interaction.channel):
            await interaction.response.send_message(
                "This instance is scoped to its configured channels.", ephemeral=True
            )
            return False
        if self.instance.get("dry_run") and name not in {"chat", "skill"}:
            self.emit("command.proposed", {"name": name})
            await interaction.response.send_message(
                "Dry run recorded this command for review in the dashboard.",
                ephemeral=True,
            )
            return False
        return True

    async def on_ready(self):
        self.emit("connection.ready", {"user_id": str(self.user.id)})
        await super().on_ready()

    async def on_disconnect(self):
        self.emit("connection.disconnected", {})

    async def on_message(self, message):
        if getattr(self, "hub_runtime", None):
            self.hub_runtime.dispatch_components(self.instance["id"], message)
        if message.author.bot or not self.allowed(message.channel):
            return
        self.emit(
            "message.received",
            {
                "message_id": str(message.id),
                "channel_id": str(message.channel.id),
                "user_id": str(message.author.id),
                "characters": len(message.content),
            },
        )
        if self.instance.get("dry_run"):
            directed = self.user in message.mentions or (
                message.reference
                and isinstance(message.reference.resolved, discord.Message)
                and message.reference.resolved.author.id == self.user.id
            )
            if directed:
                prompt = re.sub(rf"<@!?{self.user.id}>", "", message.content).strip()
                if prompt:
                    await self.run_turn(
                        message.channel,
                        prompt,
                        message.author,
                        message.reply,
                        source_id=message.id,
                    )
            else:
                self.schedule_ambient(message)
            return
        await super().on_message(message)

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
        if not self.allowed(channel):
            return None
        self.turn_tasks.add(asyncio.current_task())
        self.emit(
            "turn.requested",
            {
                "channel_id": str(channel.id),
                "user_id": str(author.id),
                "model": model or self.config["default_model"],
                "ambient": ambient,
            },
        )
        try:
            if self.instance.get("dry_run"):
                async with self.room_locks.setdefault(str(channel.id), asyncio.Lock()):
                    selected, answer, _ = await self.engine.ask(
                        str(channel.id),
                        prompt,
                        author.display_name,
                        model=model,
                        skill=skill,
                        history=history,
                        persist=False,
                        ambient=ambient,
                        participation=self.participation(str(channel.id))["level"],
                    )
                    if answer.strip() == "[SILENT]":
                        return None
                    draft_id = uuid.uuid4().hex
                    self.emit(
                        "draft.proposed",
                        {
                            "id": draft_id,
                            "channel_id": str(channel.id),
                            "model": selected,
                            "answer": answer,
                        },
                    )
                    view = DraftReview(
                        self.hub_state, self.instance["id"], draft_id, author.id
                    )
                    files = []
                    content = "**Dry-run draft · review only**\n" + answer[:1800]
                    if len(answer) > 1800:
                        content += "\nThe complete draft is attached."
                        files.append(
                            discord.File(
                                io.BytesIO(answer.encode()), filename="draft.md"
                            )
                        )
                    await send(content=content, view=view, files=files)
                    return {"id": draft_id, "dry_run": True, "answer": answer}
            result = await super().run_turn(
                channel,
                prompt,
                author,
                send,
                model,
                skill,
                source_id,
                history,
                ambient,
                anchor_id,
            )
            self.emit("turn.rendered", {"channel_id": str(channel.id)})
            return result
        except Exception as error:
            self.emit(
                "turn.failed",
                {"channel_id": str(channel.id), "error": type(error).__name__},
            )
            raise
        finally:
            self.turn_tasks.discard(asyncio.current_task())

    async def revise_turn(
        self, key, actor, prompt=None, model=None, expected_version=None
    ):
        if self.instance.get("dry_run"):
            raise ValueError(
                "Turn revisions are unavailable while this instance is in dry-run mode."
            )
        task = asyncio.current_task()
        self.turn_tasks.add(task)
        try:
            return await super().revise_turn(
                key, actor, prompt, model, expected_version
            )
        except asyncio.CancelledError:
            if self.store.turn(key):
                self.store.fail_turn(key)
            raise
        finally:
            self.turn_tasks.discard(task)

    async def close(self):
        pending = [
            task for task in self.turn_tasks if task is not asyncio.current_task()
        ]
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        await super().close()

    async def on_raw_message_edit(self, event):
        if self.instance.get("dry_run"):
            return
        channel = self.get_channel(event.channel_id)
        if self.allowed(channel):
            await super().on_raw_message_edit(event)

    async def on_raw_message_delete(self, event):
        if not self.instance.get("dry_run"):
            await super().on_raw_message_delete(event)

    async def on_raw_bulk_message_delete(self, event):
        if not self.instance.get("dry_run"):
            await super().on_raw_bulk_message_delete(event)
