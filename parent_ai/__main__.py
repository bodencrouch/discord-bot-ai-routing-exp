import asyncio
import base64
import fcntl
import io
import json
import logging
import mimetypes
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import discord

from .changes import Changes, StaleProposal
from .recovery import SessionStore, rewind_activity


def progress_icon(message):
    """Identify the step from its header, independently of quoted model output."""
    text = message.lower().strip()
    if text.startswith("tool call"):
        tools = {
            "read_file": "📄",
            "search_source": "🔎",
            "list_files": "🗂️",
            "read_settings": "⚙️",
            "stage_edit": "📝",
            "validate_change": "✅",
            "finish_change": "📋",
        }
        for name, icon in tools.items():
            if f"**{name}**" in text[:100]:
                return icon
        return "🛠️"
    categories = (
        ("📥", ("tool result",)),
        ("❌", ("request failed:", "deployment failed")),
        (
            "🔄",
            ("retry", "resuming", "transient planning failure", "the prior checkpoint"),
        ),
        ("⚠️", ("planning stream stopped", "the route did not return")),
        (
            "🚀",
            (
                "source matches",
                "deployment",
                "files applied",
                "applying",
                "restarting",
                "reconnected",
                "change is complete",
            ),
        ),
        ("✅", ("final proposal validated", "final validation passed")),
        ("📋", ("proposal ready", "approval received", "approved")),
        ("📎", ("retrieved",)),
        ("📚", ("reading", "read ")),
        ("🧭", ("hook completed",)),
        ("🧠", ("model turn", "planning stream", "stream active", "thinking")),
        ("🔀", ("the configured route",)),
        ("🛑", ("request cancelled", "cancelled")),
    )
    for icon, prefixes in categories:
        if text.startswith(prefixes):
            return icon
    return "💬"


class EditRequest(discord.ui.Modal, title="Refine the skill issue"):
    request = discord.ui.TextInput(
        label="What should change?", style=discord.TextStyle.paragraph, max_length=4000
    )

    def __init__(self, job):
        super().__init__(timeout=900)
        self.job_id, self.version = job["id"], job["version"]
        self.request.default = job["request"][-4000:]

    async def on_submit(self, interaction):
        await interaction.response.defer()
        bot = (
            getattr(interaction.client, "maintenance_controller", None)
            or interaction.client
        )
        await bot.revise(interaction, self.job_id, self.version, self.request.value)


class RequestButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r"parent:(?P<job>\d+):(?P<version>\d+):(?P<action>approve|edit|cancel|retry)",
):
    def __init__(self, job, version, action):
        labels = {
            "approve": "Yes, apply this change",
            "edit": "Edit request",
            "cancel": "Cancel",
            "retry": "Retry planning",
        }
        super().__init__(
            discord.ui.Button(
                label=labels[action],
                style=discord.ButtonStyle.success
                if action == "approve"
                else discord.ButtonStyle.secondary,
                custom_id=f"parent:{job}:{version}:{action}",
            )
        )
        self.job, self.version, self.action = job, version, action

    @classmethod
    async def from_custom_id(cls, interaction, item, match):
        return cls(int(match["job"]), int(match["version"]), match["action"])

    async def callback(self, interaction):
        bot = (
            getattr(interaction.client, "maintenance_controller", None)
            or interaction.client
        )
        job = bot.get(self.job)
        if (
            not bot.allowed(interaction.channel)
            or not job
            or not bot.can_manage(interaction.user, job)
        ):
            await interaction.response.send_message(
                "The requester or a server administrator can manage this change."
            )
            return
        if not job["thread"] and self.action in {"approve", "edit", "retry"}:
            await interaction.response.send_message(
                "This request has no usable thread. Submit it again using /modify.",
                ephemeral=True,
            )
            return
        busy = getattr(bot, "cleanup_running", None)
        if busy and busy():
            await interaction.response.send_message(
                "Server cleanup is in progress. Retry when it finishes.", ephemeral=True
            )
            return
        if job["version"] != self.version or job["state"] == "applying":
            await interaction.response.send_message(
                f"This is an older revision or deployment is in progress. Current status: **{job['state']}**, revision {job['version']}. Open <#{job['thread']}> for the live controls and activity."
            )
            return
        if self.action == "edit":
            await interaction.response.send_modal(EditRequest(job))
            return
        await interaction.response.defer()
        job = bot.get(self.job)
        if (
            not job
            or job["version"] != self.version
            or job["state"] == "applying"
            or (busy and busy())
        ):
            await interaction.followup.send(
                "This preview changed. Use the latest card."
            )
            return
        expected_version = self.version
        if self.action == "approve":
            if job["state"] != "awaiting":
                await interaction.followup.send(
                    f"Current status is **{job['state']}**. Approval becomes available when the proposal is ready. Follow progress in <#{job['thread']}>."
                )
                return
            proposal = json.loads(job["proposal"])
            if bot.changes.digest(bot.changes.snapshot()) != bot.changes.digest(
                proposal["base"]
            ):
                expected_version = self.version + 1
                bot.update(
                    self.job,
                    state="queued",
                    version=expected_version,
                    note="The application changed. Preparing a fresh preview for approval.",
                )
            else:
                bot.update(
                    self.job,
                    state="approved",
                    note="Confirmed. Waiting for the deployment queue.",
                )
        elif self.action == "cancel":
            expected_version = self.version + 1
            bot.update(
                self.job,
                state="cancelled",
                version=self.version + 1,
                note="Cancelled. No further changes will be applied.",
            )
        elif self.action == "retry":
            if job["state"] != "failed":
                await interaction.followup.send(
                    "Planning retry is available after a failed attempt. Use the latest controls."
                )
                return
            retry_version = self.version + 1
            expected_version = retry_version
            bot.update(
                self.job,
                state="rewinding",
                version=retry_version,
                note="Queued for another planning attempt.",
            )
            try:
                if interaction.message:
                    try:
                        await interaction.message.edit(view=None)
                    except discord.NotFound:
                        pass
                    if not bot.retry_is_current(self.job, retry_version):
                        return
                checkpoint = bot.sessions.load(self.job)
                marker = (checkpoint or {}).get("activity_message_id") or str(
                    job["source"]
                )
                await rewind_activity(bot, self.job, marker)
            except discord.HTTPException as error:
                if not bot.retry_is_current(self.job, retry_version):
                    return
                bot.update(
                    self.job,
                    state="failed",
                    note=f"Retry could not rewind the later bot output (Discord HTTP {error.status}).",
                )
                await bot.card(self.job)
                current = bot.get(self.job)
                if (
                    current
                    and current["version"] == retry_version
                    and current["state"] == "failed"
                    and not (busy and busy())
                ):
                    await bot.progress(self.job, "Request failed: " + current["note"])
                return
            if not bot.retry_is_current(self.job, retry_version):
                return
            bot.update(self.job, state="queued")
        current = bot.get(self.job)
        if not current or current["version"] != expected_version or (busy and busy()):
            return
        await bot.card(self.job)
        current = bot.get(self.job)
        if not current or current["version"] != expected_version or (busy and busy()):
            return
        await bot.progress(
            self.job,
            {
                "approve": "Approval received. Checking the reviewed source before deployment.",
                "cancel": "Request cancelled.",
                "retry": "Retrying planning.",
            }.get(self.action, "Request updated."),
        )
        bot.wake.set()


class Parent(discord.Client):
    def __init__(self, config):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(
            intents=intents, allowed_mentions=discord.AllowedMentions.none()
        )
        self.config = config
        self.changes = Changes(config)
        self.db = sqlite3.connect(config["database"])
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT UNIQUE, author INTEGER,
            request TEXT, state TEXT, version INTEGER DEFAULT 1, thread INTEGER,
            card INTEGER, proposal TEXT, note TEXT DEFAULT '')""")
        if "thread_card" not in {
            row[1] for row in self.db.execute("PRAGMA table_info(jobs)")
        }:
            self.db.execute("ALTER TABLE jobs ADD COLUMN thread_card INTEGER")
        if "failure_message" not in {
            row[1] for row in self.db.execute("PRAGMA table_info(jobs)")
        }:
            self.db.execute("ALTER TABLE jobs ADD COLUMN failure_message TEXT")
        if "source_channel" not in {
            row[1] for row in self.db.execute("PRAGMA table_info(jobs)")
        }:
            self.db.execute("ALTER TABLE jobs ADD COLUMN source_channel INTEGER")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS audit (id INTEGER PRIMARY KEY AUTOINCREMENT, job INTEGER, snapshot TEXT, recorded_at TEXT DEFAULT CURRENT_TIMESTAMP)"
        )
        self.db.commit()
        self.sessions = SessionStore(config["database"])
        self.activity_messages = {}
        self.wake = asyncio.Event()
        self.worker = None
        self.deployment_monitor = None
        self.active_jobs = {}
        self.deploy_lock = asyncio.Lock()
        self.deploy_blocked = False
        self.add_dynamic_items(RequestButton)

    @staticmethod
    def can_manage(user, job):
        return user.id == job["author"] or bool(
            getattr(getattr(user, "guild_permissions", None), "administrator", False)
        )

    def retry_is_current(self, job_id, version):
        job = self.get(job_id)
        busy = getattr(self, "cleanup_running", None)
        return bool(
            job
            and job["version"] == version
            and job["state"] == "rewinding"
            and not (busy and busy())
        )

    def get(self, job_id):
        return self.db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()

    def update(self, job_id, **fields):
        with self.db:
            previous = self.get(job_id)
            if previous:
                self.db.execute(
                    "INSERT INTO audit(job,snapshot) VALUES(?,?)",
                    (job_id, json.dumps(dict(previous))),
                )
            self.db.execute(
                "UPDATE jobs SET "
                + ",".join(f"{key}=?" for key in fields)
                + " WHERE id=?",
                (*fields.values(), job_id),
            )
        if (
            previous
            and previous["state"] in {"planning", "queued", "rewinding"}
            and fields.get("version", previous["version"]) != previous["version"]
            and fields.get("state", previous["state"]) != "applying"
        ):
            active = self.active_jobs.get(job_id)
            if active and not active.done():
                try:
                    current = asyncio.current_task()
                except RuntimeError:
                    current = None
                if active is not current:
                    active.cancel()

    def allowed(self, channel):
        return (
            channel
            and getattr(channel, "guild", None)
            and channel.guild.id == self.config["guild_id"]
            and (
                channel.id == self.config["channel_id"]
                or isinstance(channel, discord.Thread)
                and channel.parent_id == self.config["channel_id"]
            )
        )

    async def setup_hook(self):
        self.worker = asyncio.create_task(self.work())
        if self.self_deployment_enabled():
            self.deployment_monitor = asyncio.create_task(self.monitor_deployments())

    async def on_ready(self):
        logging.info(
            "PARENT_READY bot=%s channel=%s", self.user.id, self.config["channel_id"]
        )
        await self.change_presence(
            activity=discord.Game(name="parental controls · @mention me")
        )
        for job in self.db.execute(
            "SELECT id FROM jobs WHERE thread IS NOT NULL AND thread_card IS NULL AND state IN ('awaiting','clarify','approved','failed','done')"
        ).fetchall():
            try:
                await self.card(job["id"], attach=True)
            except discord.HTTPException:
                logging.exception("Could not restore thread review card")
        for job in self.db.execute(
            "SELECT * FROM jobs WHERE state='rewinding'"
        ).fetchall():
            saved = self.sessions.load(job["id"])
            marker = (saved or {}).get("activity_message_id") or str(job["source"])
            try:
                await rewind_activity(self, job["id"], marker)
                if not self.retry_is_current(job["id"], job["version"]):
                    continue
                self.update(job["id"], state="queued")
            except discord.HTTPException:
                if not self.retry_is_current(job["id"], job["version"]):
                    continue
                self.update(
                    job["id"],
                    state="failed",
                    note="Interrupted retry could not finish rewinding Discord activity.",
                )
        self.wake.set()

        for job in self.db.execute(
            "SELECT * FROM jobs WHERE state='failed' AND thread IS NOT NULL"
        ).fetchall():
            try:
                thread = self.get_channel(job["thread"]) or await self.fetch_channel(
                    job["thread"]
                )
                if thread.archived:
                    await thread.edit(archived=False)
            except discord.HTTPException:
                logging.exception("Could not retrieve failed request thread")
                continue
            view = discord.ui.View(timeout=None)
            view.add_item(RequestButton(job["id"], job["version"], "retry"))
            try:
                if job["failure_message"]:
                    message = await thread.fetch_message(int(job["failure_message"]))
                    await message.edit(view=view)
                else:
                    await self.progress(job["id"], "Request failed: " + job["note"])
            except discord.NotFound:
                await self.progress(job["id"], "Request failed: " + job["note"])
        await self.run_cleanup_hook("cleanup_recover")

    async def run_cleanup_hook(self, name, *args):
        """Keep optional child cleanup integration isolated from Parent readiness."""
        hook = getattr(self, name, None)
        if hook is None:
            return
        try:
            await hook(*args)
        except Exception as error:
            logging.warning("Cleanup hook %s failed (%s)", name, type(error).__name__)

    async def on_guild_role_update(self, before, after):
        if not self.user or str(after.guild.id) != str(self.config["guild_id"]):
            return
        member = after.guild.me
        if (
            member
            and any(role.id == after.id for role in member.roles)
            and before.permissions != after.permissions
        ):
            await self.run_cleanup_hook("cleanup_permissions_changed", after.guild.id)

    async def on_member_update(self, before, after):
        if (
            self.user
            and after.id == self.user.id
            and str(after.guild.id) == str(self.config["guild_id"])
            and (
                before.roles != after.roles
                or before.guild_permissions != after.guild_permissions
            )
        ):
            await self.run_cleanup_hook("cleanup_permissions_changed", after.guild.id)

    async def close(self):
        if self.deployment_monitor:
            self.deployment_monitor.cancel()
            await asyncio.gather(self.deployment_monitor, return_exceptions=True)
        if self.worker:
            self.worker.cancel()
            try:
                await self.worker
            except asyncio.CancelledError:
                pass
        for task in self.active_jobs.values():
            task.cancel()
        await asyncio.gather(*self.active_jobs.values(), return_exceptions=True)
        await self.changes.http.aclose()
        self.sessions.close()
        self.db.close()
        await super().close()

    async def progress(self, job_id, message):
        job = self.get(job_id)
        if not job or not job["thread"]:
            return
        try:
            thread = self.get_channel(job["thread"]) or await self.fetch_channel(
                job["thread"]
            )
            if thread.archived:
                await thread.edit(archived=False)
            icon = progress_icon(message)
            content = f"{icon} {message}"
            view = None
            if job["state"] == "failed" and message.startswith("Request failed:"):
                view = discord.ui.View(timeout=None)
                view.add_item(RequestButton(job_id, job["version"], "retry"))
            if len(content) <= 1900:
                sent = await thread.send(content, view=view)
            else:
                sent = await thread.send(
                    f"{icon} Full model or tool output attached.",
                    file=discord.File(
                        io.BytesIO(message.encode()), filename="activity.txt"
                    ),
                    view=view,
                )
            self.sessions.record_output(job_id, sent.id, thread.id, "progress")
            self.activity_messages[job_id] = str(sent.id)
            if view is not None:
                self.update(job_id, failure_message=str(sent.id))
            return sent
        except discord.HTTPException:
            logging.exception("Could not post request activity")

    async def card(self, job_id, attach=False):
        job = self.get(job_id)
        channel_id = job["source_channel"] or self.config["channel_id"]
        channel = self.get_channel(channel_id) or await self.fetch_channel(channel_id)
        view = discord.ui.View(timeout=None)
        if job["state"] == "awaiting":
            view.add_item(RequestButton(job_id, job["version"], "approve"))
        if job["state"] != "applying" and job["thread"]:
            view.add_item(RequestButton(job_id, job["version"], "edit"))
        if job["state"] in {
            "queued",
            "planning",
            "clarify",
            "awaiting",
            "approved",
            "failed",
        }:
            view.add_item(RequestButton(job_id, job["version"], "cancel"))
        if job["state"] == "failed" and job["thread"]:
            view.add_item(RequestButton(job_id, job["version"], "retry"))
        embed = discord.Embed(
            title=f"🛠️ Skill Issue #{job_id} · {job['state']}",
            description=job["request"][:2500],
            color=0xF1C40F,
        )
        embed.add_field(
            name="Department update",
            value=(job["note"] or "Queued. The department is on it.")[:1024],
            inline=False,
        )
        if job["thread"]:
            embed.add_field(
                name="Refine this request",
                value=f"<#{job['thread']}> · @mention me there with follow-ups.",
                inline=False,
            )
        footer = {
            "awaiting": "requester or administrator approval required",
            "approved": "approved",
            "applying": "deploying approved changes",
            "done": "deployed",
            "failed": "retry available",
            "clarify": "clarification needed",
            "cancelled": "cancelled",
        }.get(job["state"], "preparing proposal")
        embed.set_footer(text=f"Revision {job['version']} · {footer}")
        same_destination = bool(job["thread"] and int(channel_id) == int(job["thread"]))
        if same_destination and not job["thread_card"] and job["card"]:
            self.update(job_id, thread_card=job["card"])
            job = self.get(job_id)
        destinations = (
            (("thread_card", channel),)
            if same_destination
            else (
                ("card", channel),
                (
                    "thread_card",
                    self.get_channel(job["thread"]) if job["thread"] else None,
                ),
            )
        )
        for field, destination in destinations:
            if field == "thread_card" and destination is None and job["thread"]:
                destination = await self.fetch_channel(job["thread"])
            if not destination:
                continue
            if getattr(destination, "archived", False):
                await destination.edit(archived=False)
            # Each Discord send needs fresh file objects.
            files = []
            if attach and job["proposal"]:
                proposal = json.loads(job["proposal"])
                files = [
                    discord.File(
                        io.BytesIO(self.changes.diff(proposal).encode()),
                        filename=f"change-{job_id}-v{job['version']}.diff",
                    ),
                    discord.File(
                        io.BytesIO(proposal["plan"]["summary"].encode()),
                        filename=f"proposal-{job_id}-v{job['version']}.md",
                    ),
                ]
            params = {"embed": embed, "view": view}
            if attach:
                params["attachments"] = files
            elif not job["proposal"]:
                params["attachments"] = []
            if job[field]:
                try:
                    msg = await destination.fetch_message(job[field])
                    if field == "thread_card" and attach and job["proposal"]:
                        await msg.edit(
                            content="A newer proposal is available below.", view=None
                        )
                    else:
                        await msg.edit(**params)
                        self.sessions.record_output(
                            job_id, msg.id, destination.id, field
                        )
                        continue
                except discord.NotFound:
                    pass
            params.pop("attachments", None)
            msg = await destination.send(**params, files=files)
            self.sessions.record_output(job_id, msg.id, destination.id, field)
            fields = (
                {"card": msg.id, "thread_card": msg.id}
                if same_destination
                else {field: msg.id}
            )
            self.update(job_id, **fields)

    async def revise(self, interaction, job_id, version, text):
        job = self.get(job_id)
        if (
            not self.allowed(interaction.channel)
            or not job
            or not self.can_manage(interaction.user, job)
        ):
            return
        if not job["thread"]:
            await interaction.followup.send(
                "This request has no usable thread. Submit it again using /modify.",
                ephemeral=True,
            )
            return
        if job["version"] != version or job["state"] == "applying":
            await interaction.followup.send(
                "That request changed while you were editing. Open the latest card."
            )
            return
        self.update(
            job_id,
            request=text,
            state="queued",
            version=version + 1,
            proposal=None,
            note="Updated request. Previous approval cleared.",
        )
        await self.card(job_id)
        self.wake.set()

    async def on_message(self, message):
        if (
            message.author.bot
            or not self.allowed(message.channel)
            or self.user.id not in message.raw_mentions
        ):
            return
        prompt = re.sub(rf"<@!?{self.user.id}>", "", message.content).strip()
        if not prompt:
            await message.reply(
                "🛠️ @mention me with a change for this bot or any configured bot. I’ll draft it, show the diff, and ask you to confirm. The department runs on consent and questionable coffee."
            )
            return
        await self.submit_request(message, prompt)

    async def submit_request(self, message, prompt, author=None):
        """File a real Discord source on behalf of its requesting member."""
        author = author or message.author
        if not self.allowed(message.channel):
            raise ValueError("Choose a configured maintenance channel.")
        job = self.db.execute(
            "SELECT * FROM jobs WHERE thread=? ORDER BY id DESC LIMIT 1",
            (message.channel.id,),
        ).fetchone()
        if job and self.can_manage(author, job) and job["state"] != "applying":
            self.update(
                job["id"],
                request=job["request"] + "\n\nFollow-up\n" + prompt,
                state="queued",
                version=job["version"] + 1,
                proposal=None,
                note="Follow-up received. Preparing a revised preview.",
            )
            await self.card(job["id"])
        else:
            with self.db:
                cur = self.db.execute(
                    "INSERT OR IGNORE INTO jobs(source,author,request,state,source_channel) VALUES(?,?,?,?,?)",
                    (str(message.id), author.id, prompt, "opening", message.channel.id),
                )
            if not cur.rowcount:
                return self.db.execute(
                    "SELECT * FROM jobs WHERE source=?", (str(message.id),)
                ).fetchone()
            job_id = cur.lastrowid
            thread = None
            try:
                if isinstance(message.channel, discord.Thread):
                    thread = message.channel
                else:
                    thread = await message.create_thread(
                        name=f"Skill Issue #{job_id} · {prompt[:65]}",
                        auto_archive_duration=1440,
                    )
                    await thread.add_user(author)
            except Exception:
                current = self.get(job_id)
                if current and current["state"] == "opening":
                    self.update(
                        job_id,
                        state="failed",
                        thread=thread.id if thread else None,
                        note="Discord could not finish opening the request thread. Submit the request again using /modify.",
                    )
                raise
            with self.db:
                opened = self.db.execute(
                    "UPDATE jobs SET thread=?,state='queued' WHERE id=? AND state='opening'",
                    (thread.id, job_id),
                ).rowcount
            if not opened:
                return self.get(job_id)
            await self.card(job_id)
            await message.reply(
                f"Filed as **Skill Issue #{job_id}**. Follow-ups go in <#{thread.id}>. I’ll ask before applying anything."
            )
        self.wake.set()
        return self.db.execute(
            "SELECT * FROM jobs WHERE thread=? ORDER BY id DESC LIMIT 1",
            (
                message.channel.id
                if isinstance(message.channel, discord.Thread)
                else thread.id,
            ),
        ).fetchone()

    async def on_raw_message_edit(self, payload):
        job = self.db.execute(
            "SELECT * FROM jobs WHERE source=?", (str(payload.message_id),)
        ).fetchone()
        if (
            not job
            or job["state"] in {"applying", "opening"}
            or "content" not in payload.data
        ):
            return
        channel = self.get_channel(payload.channel_id) or await self.fetch_channel(
            payload.channel_id
        )
        message = await channel.fetch_message(payload.message_id)
        if not self.allowed(channel):
            return
        if self.user.id not in message.raw_mentions:
            self.update(
                job["id"],
                state="cancelled",
                version=job["version"] + 1,
                note="The parent mention was removed. Previous approval cleared.",
            )
            await self.card(job["id"])
            return
        prompt = re.sub(rf"<@!?{self.user.id}>", "", message.content).strip()
        if prompt and prompt != job["request"]:
            self.update(
                job["id"],
                request=prompt,
                version=job["version"] + 1,
                state="queued",
                proposal=None,
                note="Original request edited. Previous approval cleared.",
            )
            await self.card(job["id"])
            self.wake.set()

    async def on_raw_message_delete(self, payload):
        job = self.db.execute(
            "SELECT * FROM jobs WHERE source=?", (str(payload.message_id),)
        ).fetchone()
        if job and job["state"] not in {"applying", "done"}:
            self.update(
                job["id"],
                state="cancelled",
                version=job["version"] + 1,
                note="Source request deleted.",
            )
            await self.card(job["id"])

    def self_deployment_enabled(self):
        return bool(self.config.get("hub_config") or os.environ.get("BOT_HUB_CONFIG"))

    async def resume_deployment_handoffs(self):
        """Recover approved ownership without restoring files in the hub process."""
        from . import self_deploy

        for job in self.db.execute(
            "SELECT * FROM jobs WHERE state='applying'"
        ).fetchall():
            if self_deploy.status(self.config, job["id"], job["version"]):
                continue
            try:
                await self_deploy.launch(
                    self.config, job["id"], job["version"], json.loads(job["proposal"])
                )
            except (OSError, ValueError, RuntimeError) as error:
                logging.warning(
                    "Deployment handoff for job %s will retry (%s)",
                    job["id"],
                    type(error).__name__,
                )
        return await self_deploy.recover(self.config)

    async def notify_deployment(self, job, transaction):
        """Reconcile a stable Discord receipt before retrying uncertain delivery."""
        from . import self_deploy

        version = transaction["version"]
        with self_deploy.notification_lock(self.config, job["id"], version) as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return False
            if self_deploy.status(self.config, job["id"], version).get("notified"):
                return True
            delivery = self_deploy.notification(self.config, job["id"], version)
            thread = self.get_channel(job["thread"]) or await self.fetch_channel(
                job["thread"]
            )
            if thread.archived:
                await thread.edit(archived=False)
            transport = getattr(self, "host", self)
            owned_id = transport.user.id
            sent = None
            if delivery.get("message_id"):
                try:
                    sent = await thread.fetch_message(int(delivery["message_id"]))
                except discord.NotFound:
                    pass
            if sent is None:
                since = datetime.fromtimestamp(
                    max(0, transaction.get("created_at", 0) - 5), timezone.utc
                )
                async for candidate in thread.history(limit=None, after=since):
                    if (
                        candidate.author.id == owned_id
                        and delivery["marker"] in candidate.content
                    ):
                        sent = candidate
                        break
            if sent is None:
                message = transaction.get("note") or "Deployment finished."
                content = f"{progress_icon(message)} {message}\n{delivery['marker']}"
                # Discord's nonce enforcement covers an uncertain send response,
                # while history reconciliation covers recovery after its nonce window.
                data = await transport.http.request(
                    discord.http.Route(
                        "POST", "/channels/{channel_id}/messages", channel_id=thread.id
                    ),
                    json={
                        "content": content,
                        "nonce": delivery["nonce"],
                        "enforce_nonce": True,
                        "allowed_mentions": {"parse": []},
                    },
                )
                sent = discord.Message(state=thread._state, channel=thread, data=data)
            delivery.update(message_id=str(sent.id), channel_id=str(thread.id))
            self_deploy.save_notification(self.config, job["id"], version, delivery)
            self.sessions.record_output(job["id"], sent.id, thread.id, "progress")
            self.activity_messages[job["id"]] = str(sent.id)
            self_deploy.mark_notified(self.config, job["id"], version)
            return True

    async def monitor_deployments(self):
        """Retry durable handoffs and reconcile every transaction revision."""
        from . import self_deploy

        await self.wait_until_ready()
        while not self.is_closed():
            try:
                await self.resume_deployment_handoffs()
                for transaction in self_deploy.list_transactions(self.config):
                    if not transaction or not transaction.get("terminal"):
                        continue
                    if transaction["phase"] == "blocked":
                        self.deploy_blocked = True
                    if transaction["phase"] == "stale":
                        self.wake.set()
                    if transaction.get("notified"):
                        continue
                    job = self.get(transaction["job_id"])
                    if not job or not job["thread"]:
                        continue
                    # Existing cards are edited in place. Completion has its own
                    # durable identity and can recover a send whose reply was lost.
                    try:
                        await self.card(job["id"])
                        if await self.notify_deployment(job, transaction):
                            self.wake.set()
                    except Exception:
                        logging.exception(
                            "Could not deliver deployment outcome for job %s", job["id"]
                        )
                        continue
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("Could not reconcile self-deployment outcome")
            await asyncio.sleep(3)

    async def work(self):
        interrupted_openings = self.db.execute(
            "SELECT id,version FROM jobs WHERE state='opening'"
        ).fetchall()
        await self.wait_until_ready()
        for opening in interrupted_openings:
            current = self.get(opening["id"])
            if (
                current
                and current["state"] == "opening"
                and current["version"] == opening["version"]
            ):
                self.update(
                    opening["id"],
                    state="failed",
                    note="Request thread setup was interrupted. Submit the request again using /modify.",
                )
                try:
                    await self.card(opening["id"])
                except discord.HTTPException:
                    logging.exception(
                        "Could not report interrupted request thread setup"
                    )
        if self.self_deployment_enabled():
            recovered = await self.resume_deployment_handoffs()
            self.deploy_blocked = any(
                transaction and transaction.get("phase") == "blocked"
                for transaction in recovered
            )
        # Interrupted writes have a durable pre-change snapshot. Restore before accepting more work.
        for job in self.db.execute(
            "SELECT * FROM jobs WHERE state='applying'"
        ).fetchall():
            if self.self_deployment_enabled():
                # The monitor re-hands off missing manifests. All self-deployment
                # source restoration belongs to the independent locked watchdog.
                continue
            try:
                self.changes.restore(json.loads(job["proposal"]))
                await self.changes.restart()
                self.update(
                    job["id"],
                    state="failed",
                    note="Interrupted deployment restored. Review and retry when ready.",
                )
            except Exception:
                logging.exception("Recovery failed")
                self.update(
                    job["id"],
                    note="Recovery needs operator attention. Deployment queue stopped.",
                )
                await self.card(job["id"])
                return
        with self.db:
            self.db.execute("UPDATE jobs SET state='queued' WHERE state='planning'")
        while not self.is_closed():
            self.wake.clear()
            for job in self.db.execute(
                "SELECT * FROM jobs WHERE state IN ('approved','queued') AND thread IS NOT NULL ORDER BY id"
            ).fetchall():
                if job["id"] not in self.active_jobs and not (
                    self.deploy_blocked and job["state"] == "approved"
                ):
                    task = asyncio.create_task(self.process_job(job["id"]))
                    self.active_jobs[job["id"]] = task
                    task.add_done_callback(
                        lambda _, job_id=job["id"]: self.job_finished(job_id)
                    )
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=3)
            except asyncio.TimeoutError:
                pass

    def job_finished(self, job_id):
        self.active_jobs.pop(job_id, None)
        self.wake.set()

    async def request_context(self, job):
        """Resolve the request's live Discord context and actual image bytes."""
        thread = self.get_channel(job["thread"]) or await self.fetch_channel(
            job["thread"]
        )
        parent = getattr(thread, "parent", None) or thread
        source_channel = job["source_channel"]
        if source_channel:
            parent = self.get_channel(source_channel) or await self.fetch_channel(
                source_channel
            )
        messages = {}
        errors = []
        try:
            source = await parent.fetch_message(int(job["source"]))
            messages[source.id] = source
            async for message in parent.history(limit=30, before=source):
                if not message.author.bot:
                    messages[message.id] = message
        except discord.HTTPException as error:
            errors.append({"source_id": str(job["source"]), "status": error.status})
        scanned = 0
        retained = 0
        async for message in thread.history(limit=500):
            scanned += 1
            if not message.author.bot:
                messages[message.id] = message
                retained += 1
                if retained >= 40:
                    break
        if scanned == 500 and retained < 40:
            errors.append(
                {
                    "scope": "thread_history",
                    "reason": "Scanned 500 recent posts before reaching 40 human messages",
                }
            )
        links = re.findall(
            r"https?://(?:www\.)?discord\.com/channels/(\d+)/(\d+)/(\d+)",
            job["request"],
        )
        for guild_id, channel_id, message_id in links:
            if int(guild_id) != int(self.config["guild_id"]):
                continue
            try:
                channel = self.get_channel(int(channel_id)) or await self.fetch_channel(
                    int(channel_id)
                )
                message = await channel.fetch_message(int(message_id))
                messages[message.id] = message
            except discord.HTTPException as error:
                errors.append({"source_id": message_id, "status": error.status})
        context = {"messages": [], "images": [], "retrieval_errors": errors}
        image_sources = []
        for message in sorted(messages.values(), key=lambda item: item.id):
            context["messages"].append(
                {
                    "source_id": str(message.id),
                    "author": message.author.display_name,
                    "role": "human",
                    "content": message.content[:4000],
                }
            )
            for attachment in message.attachments:
                mime = (
                    attachment.content_type
                    or mimetypes.guess_type(attachment.filename)[0]
                    or ""
                )
                if mime.split(";")[0] not in {
                    "image/png",
                    "image/jpeg",
                    "image/webp",
                    "image/gif",
                }:
                    continue
                if attachment.size > 8_000_000:
                    errors.append(
                        {
                            "source_id": str(message.id),
                            "file": attachment.filename,
                            "reason": "Image exceeds the inference upload budget",
                        }
                    )
                    continue
                try:
                    raw = await attachment.read()
                    if raw.startswith(b"\x89PNG\r\n\x1a\n"):
                        mime = "image/png"
                    elif raw.startswith(b"\xff\xd8\xff"):
                        mime = "image/jpeg"
                    elif raw.startswith((b"GIF87a", b"GIF89a")):
                        mime = "image/gif"
                    elif raw.startswith(b"RIFF") and raw[8:12] == b"WEBP":
                        mime = "image/webp"
                    else:
                        errors.append(
                            {
                                "source_id": str(message.id),
                                "file": attachment.filename,
                                "reason": "Unsupported image bytes",
                            }
                        )
                        continue
                    context["images"].append(
                        {
                            "url": "data:"
                            + mime.split(";")[0]
                            + ";base64,"
                            + base64.b64encode(raw).decode()
                        }
                    )
                    image_sources.append(str(message.id))
                except discord.HTTPException as error:
                    errors.append(
                        {
                            "source_id": str(message.id),
                            "file": attachment.filename,
                            "status": error.status,
                        }
                    )
        await self.progress(
            job["id"],
            f"Retrieved {len(context['messages'])} live Discord context messages and {len(context['images'])} image attachments. Image source messages: {', '.join(image_sources) or 'none'}. {len(errors)} retrieval errors.",
        )
        return context

    async def process_job(self, job_id):
        job = self.get(job_id)
        if not job or job["state"] not in {"queued", "approved"}:
            return
        version = job["version"]
        try:
            if job["state"] == "approved":
                async with self.deploy_lock:
                    job = self.get(job_id)
                    if (
                        self.deploy_blocked
                        or job["state"] != "approved"
                        or job["version"] != version
                    ):
                        return
                    await self.deploy(job_id, version, job)
                return
            self.update(
                job_id,
                state="planning",
                note="Reading the current implementation and running protecting-your-energy.",
            )
            await self.card(job_id)
            context = await self.request_context(job)
            resume = self.sessions.load(job_id)

            async def checkpoint(state):
                await asyncio.to_thread(
                    self.sessions.save,
                    job_id,
                    state,
                    self.activity_messages.get(job_id),
                )

            thread = self.get_channel(job["thread"]) or await self.fetch_channel(
                job["thread"]
            )
            async with thread.typing():
                proposal = await self.changes.propose(
                    job["request"],
                    job_id,
                    lambda message: self.progress(job_id, message),
                    context=context,
                    resume=resume["state"] if resume else None,
                    checkpoint=checkpoint,
                )
            if (
                self.get(job_id)["version"] != version
                or self.get(job_id)["state"] != "planning"
            ):
                return
            plan = proposal["plan"]
            saved = self.sessions.load(job_id)
            if saved:
                saved["state"]["staged"] = dict(plan["files"])
                saved["state"]["messages"].append(
                    {
                        "role": "assistant",
                        "content": "Current validated proposal for review\n"
                        + json.dumps(
                            {
                                "summary": plan["summary"],
                                "questions": plan["questions"],
                                "config": plan["config"],
                                "staged_files": list(plan["files"]),
                            }
                        ),
                    }
                )
                await asyncio.to_thread(
                    self.sessions.save,
                    job_id,
                    saved["state"],
                    self.activity_messages.get(job_id),
                )
            if plan["questions"]:
                note = "\n".join(str(q) for q in plan["questions"])
                state = "clarify"
            elif not self.changes.diff(proposal).strip():
                note, state = (
                    plan["summary"] + "\nNo implementation change is needed.",
                    "done",
                )
            else:
                note, state = (
                    plan["summary"]
                    + "\n\nAre you sure? Review the attached diff, then approve or edit.",
                    "awaiting",
                )
            self.update(job_id, state=state, proposal=json.dumps(proposal), note=note)
            thread = self.get_channel(job["thread"]) or await self.fetch_channel(
                job["thread"]
            )
            if thread.archived:
                await thread.edit(archived=False)
            await self.progress(
                job_id,
                "Proposal ready for review." if state == "awaiting" else note[:1600],
            )
            await self.card(job_id, attach=True)
        except Exception as exc:
            logging.exception("Request failed")
            current = self.get(job_id)
            if (
                self.self_deployment_enabled()
                and current["version"] == version
                and current["state"] == "applying"
            ):
                # Once approved ownership commits, transport and handoff failures
                # are recovered by the deployment monitor independently of planning.
                self.wake.set()
                return
            if current["version"] == version:
                self.update(
                    job_id,
                    state="failed",
                    note=str(exc)[:950]
                    if isinstance(exc, (ValueError, RuntimeError))
                    else "The department hit an error. Retry planning or refine the request.",
                )
                await self.card(job_id)
                await self.progress(
                    job_id, f"Request failed: {self.get(job_id)['note']}"
                )

    async def deploy(self, job_id, version, job):
        proposal = json.loads(job["proposal"])
        if self.changes.digest(self.changes.snapshot()) != self.changes.digest(
            proposal["base"]
        ):
            self.update(
                job_id,
                state="queued",
                version=version + 1,
                note="Application source changed. Preparing a fresh approval preview.",
            )
            await self.card(job_id)
            return
        self.update(
            job_id,
            state="applying",
            note="Approved change is being applied. Checking restart health.",
        )
        await self.card(job_id)
        await self.progress(
            job_id,
            "Source matches the approved preview. Applying the reviewed files and settings.",
        )
        if self.self_deployment_enabled():
            from . import self_deploy

            await self.progress(
                job_id,
                "Deployment watchdog is taking over. It will restart the unified bot application, verify every enabled identity, and restore the approved baseline if reconnecting fails.",
            )
            try:
                await self_deploy.launch(self.config, job_id, version, proposal)
            except (OSError, ValueError, RuntimeError) as error:
                # Keep approved ownership so the monitor can retry this handoff.
                logging.warning(
                    "Deployment watchdog launch will retry (%s)", type(error).__name__
                )
                self.update(
                    job_id,
                    note="Approved deployment is waiting for its independent watchdog. Launch will retry automatically.",
                )
            return
        try:
            self.changes.install(proposal)
            await self.progress(
                job_id,
                "Files applied. Restarting Together AI and waiting for its Discord connection.",
            )
            await self.changes.restart()
        except StaleProposal:
            self.update(
                job_id,
                state="queued",
                version=version + 1,
                note="Source changed. Preparing a new preview.",
            )
            await self.card(job_id)
            return
        except Exception:
            try:
                self.changes.restore(proposal)
                await self.changes.restart()
            except Exception:
                self.update(
                    job_id,
                    note="Rollback needs operator attention. Deployment queue stopped.",
                )
                await self.card(job_id)
                self.deploy_blocked = True
                logging.exception("Rollback failed")
                return
            raise RuntimeError(
                "Deployment failed. Previous source and settings were restored."
            )
        self.update(
            job_id,
            state="done",
            note="Applied. Together AI reconnected successfully. Use Edit request or @mention me in the thread to refine it.",
        )
        await self.card(job_id, attach=True)
        await self.progress(
            job_id, "Together AI reconnected. The approved change is complete."
        )


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    config = json.loads(Path(os.environ["PARENT_CONFIG"]).read_text())
    Parent(config).run(Path(config["token_file"]).read_text().strip(), log_handler=None)


if __name__ == "__main__":
    main()
