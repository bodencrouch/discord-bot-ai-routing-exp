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
from pathlib import Path

import discord
from discord import app_commands

from .conversations import TurnButton
from .core import Engine, Store
from .routing import routing_report

HELP = """**Together AI**
Mention me to chat, or use `/chat`. Replies share context within the current channel or thread.
`/thread with_user` starts a shared conversation and invites your friends. Everyone can chat there without mentioning me.
Use **Retry** or **Edit prompt** beneath an answer. Editing your original Discord message also regenerates it.
Editing an earlier thread prompt rewinds the thread, removes later messages, and regenerates from there.
`/history` shows this conversation's AI context. `/retry` retries the latest answer.
`/participation` adjusts how often I join in. Mentions and direct replies still get an answer.
`/models` finds available routes. `/model` selects this channel's route.
`/routes` lists favorites and manages route aliases.
`/skills` lists skills. `/skill` runs one. You can also write `@Together AI @explain your question`.
`/connectors` lists connected tools. `/tool` calls a tool with JSON arguments.
`/summarize` summarizes recent channel messages. `/reset` clears AI conversation context.
`/image` and `/video` use generation routes you connect in OmniRoute.
`/status` shows the current model and gateway. `/help` shows this guide.
Messages sent to me and requested summaries go to the configured AI gateway. Answers are shared here."""


def load_config():
    path = Path(os.environ.get("TOGETHER_CONFIG", "config.json"))
    return path, json.loads(path.read_text())


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
        self.internal_deletes = set()
        self.ambient_tasks = {}
        self.ambient_sequences = {}
        self.last_ambient_reply = {}
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

    async def close(self):
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
        heading = f"**{model}**"
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
        async with self.room_locks.setdefault(room, asyncio.Lock()):
            existing_turn = self.store.turn_by_source(source_id) if source_id else None
            if existing_turn and existing_turn["room"] == room:
                return existing_turn
            if source_id:
                try:
                    await channel.fetch_message(int(source_id))
                except discord.NotFound:
                    return None
            context = (
                self.store.history(room, exclude_source=source_id)
                if history is None
                else history
            )
            epoch = self.room_epochs.get(room, 0)
            ambient_sequence = self.ambient_sequences.get(room, 0)
            selected = self.engine.resolve_model(
                model or self.store.model(room, self.config["default_model"])
            )
            key = uuid.uuid4().hex
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
            )
            try:
                async with self.slots, channel.typing():
                    selected, answer, raw = await self.engine.ask(
                        room,
                        prompt,
                        author.display_name,
                        selected,
                        skill,
                        history=context,
                        persist=False,
                        casual=not isinstance(channel, discord.Thread),
                        ambient=ambient,
                        participation=self.participation(room)["level"],
                    )
                changed = epoch != self.room_epochs.get(room, 0)
                superseded = ambient and ambient_sequence != self.ambient_sequences.get(
                    room, 0
                )
                if changed:
                    self.store.fail_turn(key)
                    return None
                if superseded or (ambient and answer.strip() == "[SILENT]"):
                    self.store.discard_pending(key)
                    return None
                output = await self.render_turn(
                    key, selected, answer, raw, send=send, version=1
                )
                self.store.finish_turn(key, prompt, selected, answer, [output.id])
                if ambient:
                    self.last_ambient_reply[room] = time.monotonic()
            except Exception as error:
                if ambient:
                    self.store.discard_pending(key)
                    logging.warning(
                        "Ambient participation unavailable (%s)", type(error).__name__
                    )
                    return None
                self.store.fail_turn(key)
                logging.error("Conversation request failed (%s)", type(error).__name__)
                text = "I couldn't finish that request. " + safe_error(error)
                output = await self.render_turn(
                    key,
                    selected,
                    text,
                    {"error": safe_error(error)},
                    send=send,
                    version=0,
                )
                with self.store.db:
                    self.store.db.execute(
                        "UPDATE turns SET output_ids=? WHERE key=?",
                        (json.dumps([output.id]), key),
                    )
            return self.store.turn(key)

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
            f"{mentions}\n**{selected}** · Chat naturally here. Mention me when you want a direct answer.\n"
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
        room = turn["room"]
        channel = self.get_channel(int(room)) or await self.fetch_channel(int(room))
        async with self.room_locks.setdefault(room, asyncio.Lock()):
            turn = self.store.turn(key)
            if expected_version is not None and turn["version"] != expected_version:
                raise ValueError(
                    "That answer has already changed. Use its current controls."
                )
            if prompt is not None and str(actor.id) != turn["author_id"]:
                raise ValueError("Only the prompt author can edit their prompt.")
            revised = turn["prompt"] if prompt is None else prompt
            if prompt is not None and revised == turn["prompt"]:
                return channel
            output_ids = json.loads(turn["output_ids"])
            if isinstance(channel, discord.Thread):
                anchor = (
                    turn["source_id"]
                    or turn.get("anchor_id")
                    or (output_ids[0] if output_ids else None)
                )
                if anchor:
                    targets = [
                        message
                        async for message in channel.history(
                            limit=None, after=discord.Object(id=int(anchor))
                        )
                    ]
                    if not turn["source_id"] and not turn.get("anchor_id"):
                        try:
                            targets.append(await channel.fetch_message(int(anchor)))
                        except discord.NotFound:
                            pass
                    await self.delete_messages(channel, targets)
            else:
                targets = []
                for output_id in output_ids:
                    try:
                        targets.append(await channel.fetch_message(int(output_id)))
                    except discord.NotFound:
                        pass
                await self.delete_messages(channel, targets)
            self.store.rewind(
                key, revised, truncate=isinstance(channel, discord.Thread)
            )
            prompt_annotation = revised if prompt is not None else None
            if prompt is not None and turn["source_id"]:
                source = await channel.fetch_message(int(turn["source_id"]))
                if source.author.id == self.user.id:
                    await source.edit(
                        content=f"**{turn['author_name']}**\n{revised[:1800]}"
                    )
                    prompt_annotation = None
                elif (
                    re.sub(rf"<@!?{self.user.id}>", "", source.content).strip()
                    == revised
                ):
                    prompt_annotation = None
            context = self.store.history(
                room, exclude_source=turn["source_id"], exclude_turn=key
            )
            epoch = self.room_epochs.get(room, 0)
            try:
                async with self.slots, channel.typing():
                    selected, answer, raw = await self.engine.ask(
                        room,
                        revised,
                        turn["author_name"],
                        model or turn["model"],
                        turn["skill"],
                        history=context,
                        persist=False,
                        casual=not isinstance(channel, discord.Thread),
                    )
            except Exception:
                self.store.fail_turn(key)
                raise
            if epoch != self.room_epochs.get(room, 0):
                return channel
            output = await self.render_turn(
                key,
                selected,
                answer,
                raw,
                send=channel.send,
                version=turn["version"] + 1,
                prompt_annotation=prompt_annotation,
            )
            self.store.finish_turn(
                key, revised, selected, answer, [output.id], revision=True
            )
            return channel

    async def delete_messages(self, channel, targets):
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
            except Exception:
                self.internal_deletes.discard(message.id)
                raise
            self.store.remove_message(message.id)

    async def on_raw_message_delete(self, event):
        await self.remove_deleted_message(event.channel_id, event.message_id)

    async def on_raw_bulk_message_delete(self, event):
        for message_id in event.message_ids:
            await self.remove_deleted_message(event.channel_id, message_id)

    async def remove_deleted_message(self, channel_id, message_id):
        if message_id in self.internal_deletes:
            self.internal_deletes.discard(message_id)
            return
        if not self.store.has_message(message_id):
            return
        room = str(channel_id)
        self.room_epochs[room] = self.room_epochs.get(room, 0) + 1
        async with self.room_locks.setdefault(room, asyncio.Lock()):
            turn = self.store.remove_message(message_id)
            if turn:
                channel = self.get_channel(channel_id) or await self.fetch_channel(
                    channel_id
                )
                outputs = []
                for output_id in json.loads(turn["output_ids"]):
                    try:
                        outputs.append(await channel.fetch_message(int(output_id)))
                    except discord.NotFound:
                        pass
                await self.delete_messages(channel, outputs)

    async def on_raw_message_edit(self, event):
        if "content" not in event.data:
            return
        if not self.store.has_message(
            event.message_id
        ) and not self.store.turn_by_source(event.message_id):
            return
        room = str(event.channel_id)
        turn = self.store.turn_by_source(event.message_id)
        try:
            source_channel = self.get_channel(
                event.channel_id
            ) or await self.fetch_channel(event.channel_id)
            source = await source_channel.fetch_message(event.message_id)
            if source.author.bot:
                return
            prompt = re.sub(rf"<@!?{self.user.id}>", "", source.content).strip()
            edited_at = source.edited_at.isoformat() if source.edited_at else None
            stored = self.store.db.execute(
                "SELECT content,edited_at FROM messages WHERE source_id=?",
                (str(source.id),),
            ).fetchone()
            if not edited_at or (stored and stored["edited_at"] == edited_at):
                return
            if (
                stored
                and stored["content"] == f"{source.author.display_name}: {prompt}"
            ):
                self.store.observe(
                    room, source.id, source.author.display_name, prompt, edited_at
                )
                return
            self.room_epochs[room] = self.room_epochs.get(room, 0) + 1
            self.store.observe(
                room, source.id, source.author.display_name, prompt, edited_at
            )
            if turn:
                await self.revise_turn(turn["key"], source.author, prompt)
            else:
                if isinstance(source_channel, discord.Thread):
                    async with self.room_locks.setdefault(room, asyncio.Lock()):
                        targets = [
                            m
                            async for m in source_channel.history(
                                limit=None, after=discord.Object(id=source.id)
                            )
                        ]
                        await self.delete_messages(source_channel, targets)
                self.schedule_ambient(source)
        except Exception as error:
            logging.error("Prompt edit failed (%s)", type(error).__name__)
            target = self.get_channel(event.channel_id) or await self.fetch_channel(
                event.channel_id
            )
            await target.send(
                "Could not regenerate the edited prompt. " + safe_error(error)
            )

    async def generate_media(self, interaction, kind, prompt, model):
        await interaction.response.defer(thinking=True)
        async with self.slots, interaction.channel.typing():
            selected, payload = await self.engine.generate(kind, prompt, model)
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
            else f"**{selected}** · Gateway result received. Expand raw JSON for details."
        )
        self.store.save_raw(
            key, summary, json.dumps(payload, ensure_ascii=False, indent=2)
        )
        await interaction.followup.send(summary, view=self.response_view(key))

    async def on_message(self, message):
        if (
            message.author.bot
            or not message.guild
            or message.guild.id != int(self.config["guild_id"])
        ):
            return
        if not message.content.strip():
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
        directed = self.user in message.mentions or replied
        if not directed and not public_channel and not in_thread:
            return
        if self.store.turn_by_source(message.id):
            return
        prompt = re.sub(rf"<@!?{self.user.id}>", "", message.content).strip()
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
        addressed_elsewhere = any(user.id != self.user.id for user in message.mentions)
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
            values = list(self.config.get("aliases", {})) + engine.models
            return [
                app_commands.Choice(name=v[:100], value=v)
                for v in values
                if current.lower() in v.lower() and len(v) <= 100
            ][:25]

        async def media_autocomplete(interaction, current):
            kind = interaction.command.name
            models = engine.media_models(kind)
            aliases = [
                alias
                for alias, route in self.config.get("aliases", {}).items()
                if route in models
            ]
            return [
                app_commands.Choice(name=value[:100], value=value)
                for value in aliases + models
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
                f"Your conversation is ready in {thread.mention}. Chat normally there, and mention me when you want a direct answer."
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
            name="image",
            description="Generate an image through a connected OmniRoute image route",
        )
        @app_commands.autocomplete(model=media_autocomplete)
        async def image(i: discord.Interaction, prompt: str, model: str = None):
            await self.generate_media(i, "image", prompt, model)

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
