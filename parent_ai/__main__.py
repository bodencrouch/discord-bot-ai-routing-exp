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
import time
from datetime import datetime, timezone
from pathlib import Path

import discord

from .changes import Changes, StaleProposal
from .recovery import (
    SessionStore,
    ContextClarification,
    SourceDeleted,
    recovery_delay,
    recovery_kind,
)


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
            "retry": "Resume interrupted planning",
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
        if job and job["card_retired"]:
            await interaction.response.send_message(
                "Server cleanup retired these controls. Submit a new request using /modify.",
                ephemeral=True,
            )
            return
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
            or job["card_retired"]
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
            if job["state"] != "interrupted":
                await interaction.followup.send(
                    "Recovery runs automatically. Use Edit request or Cancel on the latest card."
                )
                return
            bot.update(
                self.job,
                state="queued",
                next_retry=0,
                note="Resuming the interrupted planning checkpoint.",
            )
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
                "retry": "Resuming interrupted planning.",
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
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(jobs)")}
        for name, definition in {
            "next_retry": "REAL DEFAULT 0",
            "recovery_attempts": "INTEGER DEFAULT 0",
            "recovery_target": "TEXT DEFAULT 'queued'",
            "recovery_controls_pending": "INTEGER DEFAULT 0",
            "card_delivery_pending": "INTEGER DEFAULT 0",
            "card_delivery_retry": "REAL DEFAULT 0",
            "card_delivery_attempts": "INTEGER DEFAULT 0",
            "card_generation": "INTEGER DEFAULT 0",
            "card_retired": "INTEGER DEFAULT 0",
            "card_retirement_pending": "INTEGER DEFAULT 0",
        }.items():
            if name not in columns:
                self.db.execute(f"ALTER TABLE jobs ADD COLUMN {name} {definition}")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS audit (id INTEGER PRIMARY KEY AUTOINCREMENT, job INTEGER, snapshot TEXT, recorded_at TEXT DEFAULT CURRENT_TIMESTAMP)"
        )
        self.db.commit()
        self.sessions = SessionStore(config["database"])
        # Recognize the existing cleanup caller's durable pre-upgrade receipt.
        for job in self.db.execute(
            "SELECT * FROM jobs WHERE card_retired=0 AND state IN ('done','cancelled') "
            "AND card IS NULL AND thread_card IS NULL AND thread IS NULL "
            "AND note='Server cleanup retired these Discord controls.'"
        ).fetchall():
            self.remember_card_destinations(job)
            self.db.execute("UPDATE jobs SET card_retired=1 WHERE id=?", (job["id"],))
        with self.db:
            self.db.execute(
                "UPDATE jobs SET card_delivery_pending=0,card_retirement_pending=1,"
                "card_delivery_retry=0 WHERE card_retired=1"
            )
        self.activity_messages = {}
        self.wake = asyncio.Event()
        self.worker = None
        self.deployment_monitor = None
        self.active_jobs = {}
        self.card_deliveries = {}
        self.card_locks = {}
        self.deploy_lock = asyncio.Lock()
        self.deploy_blocked = (
            self.db.execute(
                "SELECT 1 FROM jobs WHERE state='attention' LIMIT 1"
            ).fetchone()
            is not None
        )
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

    def remember_card_destinations(self, job):
        """Retain deletion targets in the same transaction as a retirement."""
        destinations = [(job["source_channel"] or self.config["channel_id"], "card")]
        if job["thread"]:
            destinations.append((job["thread"], "thread_card"))
        for channel_id, kind in destinations:
            self.db.execute(
                "INSERT OR IGNORE INTO planning_card_destinations VALUES (?,?,?)",
                (str(job["id"]), str(channel_id), kind),
            )
        self.db.execute(
            "INSERT OR IGNORE INTO planning_card_destinations "
            "SELECT job_id,thread_id,kind FROM planning_job_outputs "
            "WHERE job_id=? AND thread_id IS NOT NULL AND kind IN ('card','thread_card')",
            (str(job["id"]),),
        )

    async def quiesce_card_deliveries(self):
        """Cleanup barrier: call while cleanup_running() blocks new publication."""
        for lock in list(self.card_locks.values()):
            async with lock:
                pass

    async def retire_cards(self, job_ids):
        """Durably suppress cards, join publication, then purge uncertain sends.

        Cleanup should await this before deleting the selected sources/threads.
        False means Discord deletion remains durably pending for automatic retry.
        Ordinary user cancellation deliberately does not call this method.
        """
        job_ids = tuple(dict.fromkeys(job_ids))
        for job_id in job_ids:
            if self.get(job_id):
                self.update(job_id, card_retired=1)
        for job_id in job_ids:
            await self.card(job_id)
        return all(
            not job or not job["card_retirement_pending"]
            for job in (self.get(job_id) for job_id in job_ids)
        )

    async def purge_retired_cards(self, job_id):
        """Remove only this bot's retired cards, even after their IDs were cleared."""
        job = self.get(job_id)
        if not job or not job["card_retired"]:
            return
        for channel_id, kind in self.sessions.card_destinations(job_id):
            try:
                channel = self.get_channel(int(channel_id)) or await self.fetch_channel(
                    int(channel_id)
                )
                async for message in channel.history(
                    limit=None, after=discord.Object(id=int(job["source"]))
                ):
                    if not self.user or message.author.id != self.user.id:
                        continue
                    if not any(
                        f"parent-card:{job_id}:{kind}"
                        in (getattr(item.footer, "text", "") or "")
                        or f"Skill Issue #{job_id} ·" in (item.title or "")
                        for item in message.embeds
                    ):
                        continue
                    try:
                        await message.delete()
                    except discord.NotFound:
                        pass
            except discord.NotFound:
                continue

    async def settle_retired_cards(self, job_id):
        """Caller holds the job's card lock until the remote send has settled."""
        try:
            await self.purge_retired_cards(job_id)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            attempt = (self.get(job_id)["card_delivery_attempts"] or 0) + 1
            self.update(
                job_id,
                card_retirement_pending=1,
                card_delivery_attempts=attempt,
                card_delivery_retry=time.time() + recovery_delay(attempt),
            )
            logging.warning(
                "Retired card cleanup for job %s will retry (%s)",
                job_id,
                type(error).__name__,
            )
        else:
            self.update(job_id, card_retirement_pending=0, card_delivery_retry=0)

    def update(self, job_id, **fields):
        if fields.get("state") in {"queued", "approved"}:
            fields.setdefault("next_retry", 0)
        with self.db:
            previous = self.get(job_id)
            legacy_retirement = fields.get("state") in {"done", "cancelled"} and all(
                key in fields and fields[key] is None
                for key in ("card", "thread_card", "thread")
            )
            retiring = fields.get("card_retired") or legacy_retirement
            if previous and retiring:
                # Persist destinations before losing IDs, including sends whose
                # Discord response has not arrived. Cancellation alone is not
                # retirement: its editable review must remain visible.
                self.remember_card_destinations(previous)
            if previous and any(
                key in fields and fields[key] != previous[key]
                for key in ("state", "version", "request", "proposal", "note", "thread")
            ):
                fields.setdefault("card_delivery_pending", 1)
                fields.setdefault("card_delivery_retry", 0)
                fields.setdefault("card_delivery_attempts", 0)
                fields["card_generation"] = (previous["card_generation"] or 0) + 1
            if previous and (previous["card_retired"] or retiring):
                fields.update(
                    card_retired=1,
                    card_delivery_pending=0,
                )
                if retiring:
                    fields.setdefault("card_retirement_pending", 1)
                    fields.update(
                        card_delivery_retry=0,
                        card_delivery_attempts=0,
                        card_generation=(previous["card_generation"] or 0) + 1,
                    )
            if (
                previous
                and fields.get("version", previous["version"]) != previous["version"]
            ):
                fields.setdefault("recovery_attempts", 0)
                if fields.get("state") == "queued":
                    fields.setdefault("recovery_target", "queued")
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
            and previous["state"]
            in {
                "planning",
                "queued",
                "rewinding",
                "recovering",
                "waiting",
                "interrupted",
            }
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

    def recovery_target(self, job):
        """Recover an approval only when the durable audit proves this revision."""
        if job["state"] in {"approved", "applying"}:
            return "approved"
        if job["state"] in {"recovering", "waiting"}:
            return job["recovery_target"] or "queued"
        if job["proposal"]:
            for row in self.db.execute(
                "SELECT snapshot FROM audit WHERE job=? ORDER BY id DESC", (job["id"],)
            ):
                previous = json.loads(row[0])
                if previous.get("version") != job["version"]:
                    continue
                if previous.get("state") in {"approved", "applying"}:
                    return "approved"
                if previous.get("state") in {
                    "queued",
                    "planning",
                    "awaiting",
                    "clarify",
                }:
                    break
        return "queued"

    def migrate_recovery_jobs(self):
        """Migrate old failures without touching reviewed, cancelled or done jobs."""
        # A pre-upgrade process may have saved review state before losing its
        # Discord acknowledgement. Re-publishing is idempotent and never re-plans.
        with self.db:
            self.db.execute(
                "UPDATE jobs SET card_delivery_pending=1,card_delivery_retry=0 "
                "WHERE proposal IS NOT NULL AND state IN ('awaiting','clarify') "
                "AND card_delivery_pending=0 AND card_retired=0"
            )
        for job in self.db.execute(
            "SELECT * FROM jobs WHERE state IN ('failed','rewinding','planning') "
            "OR (thread IS NULL AND state IN ('queued','approved','opening'))"
        ).fetchall():
            if job["card_retired"] or job["id"] in self.active_jobs:
                continue
            if not job["thread"]:
                self.update(
                    job["id"],
                    state="clarify",
                    recovery_controls_pending=1,
                    note=(job["note"] or "")
                    + "\n\nThis request has no usable thread. Resubmit it using /modify in the maintenance channel.",
                )
            elif (
                "operator attention" in job["note"].lower()
                or "rollback needs" in job["note"].lower()
            ):
                self.update(
                    job["id"],
                    state="attention",
                    recovery_controls_pending=1,
                    note="Rollback needs operator attention. Deployment queue stopped.",
                )
                self.deploy_blocked = True
            else:
                interrupted = job["state"] == "planning"
                target = self.recovery_target(job)
                revision = job["version"]
                if target == "approved" and self.self_deployment_enabled():
                    from . import self_deploy

                    transaction = self_deploy.status(self.config, job["id"], revision)
                    if transaction and transaction["terminal"]:
                        if transaction["phase"] in {"done", "cancelled", "blocked"}:
                            state = {
                                "done": "done",
                                "cancelled": "cancelled",
                                "blocked": "attention",
                            }[transaction["phase"]]
                            self.update(
                                job["id"],
                                state=state,
                                recovery_controls_pending=1,
                                note="Recovered the durable deployment outcome: "
                                + transaction["phase"]
                                + ".",
                            )
                            self.deploy_blocked |= state == "attention"
                            continue
                        revision += 1
                self.update(
                    job["id"],
                    state="recovering",
                    next_retry=time.time(),
                    version=revision,
                    recovery_target=target,
                    recovery_controls_pending=1,
                    note=(
                        "Planning process was interrupted. Resuming its checkpoint automatically."
                        if interrupted
                        else "Recovering the saved request automatically. Edit or cancel remains available."
                    ),
                )

    def schedule_recovery(self, job_id, version, error, target=None):
        job = self.get(job_id)
        if (
            not job
            or job["version"] != version
            or job["state"] in {"cancelled", "done", "awaiting", "clarify", "attention"}
        ):
            return False
        attempt = (job["recovery_attempts"] or 0) + 1
        delay = recovery_delay(attempt)
        kind = recovery_kind(error)
        labels = {
            "step_budget": "Continuing from the saved planning checkpoint",
            "protocol": "Recovering the planning response protocol",
            "source_context": "Waiting for Discord request context",
            "provider": "Waiting for an available free planning route",
        }
        self.update(
            job_id,
            state="waiting",
            recovery_attempts=attempt,
            recovery_target=target or self.recovery_target(job),
            next_retry=time.time() + delay,
            note=f"{labels[kind]}. Automatic retry in {delay}s; edit or cancel remains available.",
        )
        self.wake.set()
        return True

    async def retire_failure_controls(self, job_id):
        """Retire every old bottom Retry control, including untracked legacy posts."""
        job = self.get(job_id)
        if not job or not job["recovery_controls_pending"]:
            return
        version = job["version"]
        if job["thread"]:
            try:
                thread = self.get_channel(job["thread"]) or await self.fetch_channel(
                    job["thread"]
                )
            except discord.NotFound:
                current = self.get(job_id)
                if current and current["version"] == version:
                    fields = {"thread": None, "recovery_controls_pending": 0}
                    if current["state"] not in {
                        "cancelled",
                        "done",
                        "applying",
                        "attention",
                    }:
                        fields.update(
                            state="clarify",
                            note=(current["note"] or "")
                            + "\n\nThis request thread was deleted. Resubmit using /modify in the maintenance channel.",
                        )
                    self.update(job_id, **fields)
                return
            if thread.archived:
                await thread.edit(archived=False)
            async for message in thread.history(limit=None):
                current = self.get(job_id)
                if not current or current["version"] != version:
                    return
                if not self.user or message.author.id != self.user.id:
                    continue
                retry = any(
                    str(getattr(item, "custom_id", "")).startswith(f"parent:{job_id}:")
                    and str(getattr(item, "custom_id", "")).endswith(":retry")
                    for row in getattr(message, "components", [])
                    for item in getattr(row, "children", [])
                )
                content = getattr(message, "content", "") or ""
                legacy_failure = str(message.id) == str(job["failure_message"])
                if retry or legacy_failure:
                    params = {"view": None}
                    if content and (
                        legacy_failure or "request failed:" in content.lower()
                    ):
                        params["content"] = (
                            "🔄 This earlier failure is being recovered automatically. Use the current request card to edit or cancel."
                        )
                    try:
                        await message.edit(**params)
                    except discord.NotFound:
                        pass
        current = self.get(job_id)
        if current and current["version"] == version:
            self.update(job_id, recovery_controls_pending=0, failure_message=None)

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
        self.migrate_recovery_jobs()
        for job in self.db.execute(
            "SELECT * FROM jobs WHERE recovery_controls_pending=1 OR "
            "(thread IS NOT NULL AND thread_card IS NULL)"
        ).fetchall():
            try:
                await self.retire_failure_controls(job["id"])
                await self.card(job["id"])
            except discord.HTTPException:
                logging.warning(
                    "Could not refresh recovery controls for job %s", job["id"]
                )
        self.wake.set()
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
        for task in self.card_deliveries.values():
            task.cancel()
        await asyncio.gather(*self.card_deliveries.values(), return_exceptions=True)
        await self.changes.http.aclose()
        self.sessions.close()
        self.db.close()
        await super().close()

    async def progress(self, job_id, message):
        job = self.get(job_id)
        if not job or job["card_retired"] or not job["thread"]:
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
        """Publish independently of planning; retain review state on delivery loss."""
        lock = self.card_locks.setdefault(job_id, asyncio.Lock())
        async with lock:
            job = self.get(job_id)
            if not job:
                return
            if job["card_retired"]:
                await self.settle_retired_cards(job_id)
                return
            busy = getattr(self, "cleanup_running", None)
            if busy and busy():
                return
            generation = job["card_generation"]
            self.update(job_id, card_delivery_pending=1)
            try:
                delivered = await self.publish_card(
                    job_id, attach=attach or bool(job["proposal"])
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # Transport errors belong to this card, never to another job or
                # the saved approval/planning state. Do not expose provider text.
                logging.warning(
                    "Card delivery for job %s will retry (%s)",
                    job_id,
                    type(error).__name__,
                )
                delivered = False
            current = self.get(job_id)
            if current and current["card_retired"]:
                await self.settle_retired_cards(job_id)
                self.wake.set()
                return
            if not current or current["card_generation"] != generation:
                self.wake.set()
                return
            if delivered:
                self.update(
                    job_id,
                    card_delivery_pending=0,
                    card_delivery_retry=0,
                    card_delivery_attempts=0,
                )
            else:
                attempt = (current["card_delivery_attempts"] or 0) + 1
                self.update(
                    job_id,
                    card_delivery_attempts=attempt,
                    card_delivery_retry=time.time() + recovery_delay(attempt),
                )
            self.wake.set()

    async def publish_card(self, job_id, attach=False):
        job = self.get(job_id)
        if not job or job["card_retired"]:
            return False
        generation = job["card_generation"]

        def current():
            latest = self.get(job_id)
            busy = getattr(self, "cleanup_running", None)
            return (
                latest
                and not latest["card_retired"]
                and latest["card_generation"] == generation
                and not (busy and busy())
            )

        channel_id = job["source_channel"] or self.config["channel_id"]
        try:
            channel = self.get_channel(channel_id) or await self.fetch_channel(
                channel_id
            )
            if job["state"] in {"awaiting", "clarify"} and job["proposal"]:
                await channel.fetch_message(int(job["source"]))
        except discord.NotFound:
            if not current():
                return False
            if job["state"] not in {"cancelled", "done", "applying", "attention"}:
                self.update(
                    job_id,
                    state="cancelled",
                    version=job["version"] + 1,
                    note=job["note"]
                    + "\n\nSource request deleted. No changes will be applied.",
                )
            # Keep the source identity and human request. The configured parent
            # channel can still expose cancellation/resubmission if it survives.
            fallback = self.config["channel_id"]
            if int(fallback) == int(channel_id):
                return False
            channel = self.get_channel(fallback) or await self.fetch_channel(fallback)
            job = self.get(job_id)
            generation = job["card_generation"]
        thread = None
        if not job["thread"] and job["state"] in {
            "queued",
            "planning",
            "recovering",
            "waiting",
            "awaiting",
            "approved",
        }:
            if not current():
                return False
            self.update(
                job_id,
                state="clarify",
                note=job["note"]
                + "\n\nThis request has no usable thread. Resubmit using /modify in the maintenance channel; the saved request and proposal are retained.",
            )
            job = self.get(job_id)
            generation = job["card_generation"]
        if job["thread"]:
            try:
                thread = self.get_channel(job["thread"]) or await self.fetch_channel(
                    job["thread"]
                )
            except discord.NotFound:
                if not current():
                    return False
                fields = {"thread": None, "thread_card": None}
                if job["state"] not in {"cancelled", "done", "applying", "attention"}:
                    fields.update(
                        state="clarify",
                        note=job["note"]
                        + "\n\nThis request thread was deleted. Resubmit using /modify in the maintenance channel; the saved request and proposal are retained.",
                    )
                self.update(job_id, **fields)
                job = self.get(job_id)
                generation = job["card_generation"]
        if not current():
            return False
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
            "recovering",
            "waiting",
            "interrupted",
            "attention",
        }:
            view.add_item(RequestButton(job_id, job["version"], "cancel"))
        if job["state"] == "interrupted" and job["thread"]:
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
        elif job["state"] == "clarify":
            embed.add_field(
                name="Continue this request",
                value="Resubmit using /modify in the maintenance channel. The saved request and proposal are retained.",
                inline=False,
            )
        footer = {
            "awaiting": "requester or administrator approval required",
            "approved": "approved",
            "applying": "deploying approved changes",
            "done": "deployed",
            "failed": "automatic recovery pending",
            "recovering": "recovering automatically · edit or cancel available",
            "waiting": "waiting for automatic retry · edit or cancel available",
            "interrupted": "planning process interrupted · checkpoint preserved",
            "attention": "operator attention required",
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
                    thread,
                ),
            )
        )
        for field, destination in destinations:
            if not destination:
                continue
            if not current():
                return False
            if getattr(destination, "archived", False):
                await destination.edit(archived=False)
            marker = f"parent-card:{job_id}:{field}"
            embed.set_footer(text=f"Revision {job['version']} · {footer} · {marker}")
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
            msg = None
            if job[field]:
                try:
                    msg = await destination.fetch_message(job[field])
                    if not self.user or msg.author.id != self.user.id:
                        msg = None
                except discord.NotFound:
                    pass
            if msg is None:
                # Reconcile a send accepted by Discord whose response was lost,
                # including after restart. Never replace an uncertain send with
                # another approval card before checking our own history.
                async for candidate in destination.history(
                    limit=None, after=discord.Object(id=int(job["source"]))
                ):
                    if not current():
                        return False
                    if not self.user or candidate.author.id != self.user.id:
                        continue
                    if any(
                        marker in (getattr(item.footer, "text", "") or "")
                        or (
                            "parent-card:"
                            not in (getattr(item.footer, "text", "") or "")
                            and f"Skill Issue #{job_id} ·" in (item.title or "")
                        )
                        for item in candidate.embeds
                    ):
                        msg = candidate
                        break
            if not current():
                return False
            self.sessions.record_card_destination(job_id, destination.id, field)
            if msg is None:
                params.pop("attachments", None)
                msg = await destination.send(**params, files=files)
            else:
                # Edit the same card when publishing saved attachments again.
                # A lost edit acknowledgement cannot create another approval.
                await msg.edit(**params)
            self.sessions.record_output(job_id, msg.id, destination.id, field)
            if not current():
                return False
            fields = (
                {"card": msg.id, "thread_card": msg.id}
                if same_destination
                else {field: msg.id}
            )
            self.update(job_id, **fields)
        return True

    async def revise(self, interaction, job_id, version, text):
        job = self.get(job_id)
        if (
            not self.allowed(interaction.channel)
            or not job
            or job["card_retired"]
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
        if (
            job
            and not job["card_retired"]
            and self.can_manage(author, job)
            and job["state"] != "applying"
        ):
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
                        state="recovering" if thread else "clarify",
                        thread=thread.id if thread else None,
                        next_retry=time.time() + 3,
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
            or job["card_retired"]
            or job["state"] in {"applying", "opening", "cancelled"}
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

        current = self.get(job["id"])
        if not current or current["card_retired"]:
            return False
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
                    if not job or job["card_retired"] or not job["thread"]:
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
        await self.wait_until_ready()
        self.migrate_recovery_jobs()
        if self.self_deployment_enabled():
            recovered = await self.resume_deployment_handoffs()
            self.deploy_blocked = (
                any(
                    transaction and transaction.get("phase") == "blocked"
                    for transaction in recovered
                )
                or self.deploy_blocked
            )
        # In-process installs are never retried until the approved baseline is restored.
        for job in self.db.execute(
            "SELECT * FROM jobs WHERE state='applying'"
        ).fetchall():
            if self.self_deployment_enabled():
                continue
            try:
                self.changes.restore(json.loads(job["proposal"]))
                await self.changes.restart()
                self.update(
                    job["id"],
                    state="recovering",
                    recovery_target="approved",
                    next_retry=time.time() + 3,
                    note="Interrupted deployment restored. Retrying the reviewed change automatically.",
                )
            except Exception:
                self.deploy_blocked = True
                self.update(
                    job["id"],
                    state="attention",
                    note="Rollback needs operator attention. Deployment queue stopped.",
                )
        while not self.is_closed():
            self.wake.clear()
            self.queue_card_deliveries()
            for job in self.db.execute(
                "SELECT * FROM jobs WHERE card_retired=0 AND state IN ('approved','queued','recovering','waiting') "
                "AND thread IS NOT NULL AND COALESCE(next_retry,0)<=? ORDER BY id",
                (time.time(),),
            ).fetchall():
                if job["id"] not in self.active_jobs and not (
                    self.deploy_blocked
                    and (
                        job["state"] == "approved"
                        or (
                            job["state"] in {"recovering", "waiting"}
                            and job["recovery_target"] == "approved"
                        )
                    )
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

    def queue_card_deliveries(self):
        """One failed destination must not starve other jobs or re-run planning."""
        busy = getattr(self, "cleanup_running", None)
        if busy and busy():
            return
        for job in self.db.execute(
            "SELECT id FROM jobs WHERE ((card_delivery_pending=1 AND card_retired=0) "
            "OR (card_retired=1 AND card_retirement_pending=1)) "
            "AND COALESCE(card_delivery_retry,0)<=? ORDER BY id",
            (time.time(),),
        ).fetchall():
            job_id = job["id"]
            if job_id in self.card_deliveries:
                continue
            task = asyncio.create_task(self.card(job_id))
            self.card_deliveries[job_id] = task
            task.add_done_callback(
                lambda task, job_id=job_id: self.card_delivery_finished(job_id, task)
            )

    def card_delivery_finished(self, job_id, task):
        self.card_deliveries.pop(job_id, None)
        if not task.cancelled() and task.exception():
            logging.warning(
                "Card delivery for job %s interrupted (%s)",
                job_id,
                type(task.exception()).__name__,
            )
        self.wake.set()

    def job_finished(self, job_id):
        self.active_jobs.pop(job_id, None)
        self.wake.set()

    async def request_context(self, job):
        """Resolve the request's live Discord context and actual image bytes."""
        source_channel = job["source_channel"]
        source = parent = None
        if source_channel:
            try:
                parent = self.get_channel(source_channel) or await self.fetch_channel(
                    source_channel
                )
                source = await parent.fetch_message(int(job["source"]))
            except discord.NotFound as exc:
                raise SourceDeleted from exc
        try:
            thread = self.get_channel(job["thread"]) or await self.fetch_channel(
                job["thread"]
            )
        except discord.NotFound as exc:
            raise ContextClarification(
                "This request thread was deleted. Resubmit using /modify in the maintenance channel."
            ) from exc
        if parent is None:
            parent = getattr(thread, "parent", None) or thread
            try:
                source = await parent.fetch_message(int(job["source"]))
            except discord.NotFound as exc:
                raise SourceDeleted from exc
        # Insertion order preserves source attachment order and explicit link
        # order. Values optionally restrict a nearby message to selected indices.
        messages, errors = {source.id: source}, []
        nearby = []
        async for message in parent.history(limit=30, before=source):
            if not message.author.bot:
                messages[message.id] = message
                nearby.append(message)
        scanned = retained = 0
        followups = []
        async for message in thread.history(limit=500):
            scanned += 1
            if not message.author.bot and message.id >= source.id:
                messages[message.id] = message
                followups.append(message)
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

        def image_entries(message):
            return [
                (index, item)
                for index, item in enumerate(message.attachments)
                if (
                    getattr(item, "content_type", None)
                    or mimetypes.guess_type(item.filename)[0]
                    or ""
                ).split(";")[0]
                in {"image/png", "image/jpeg", "image/webp", "image/gif"}
            ]

        def image_key(message, index, attachment):
            return (
                getattr(attachment, "id", None)
                or getattr(attachment, "url", None)
                or (message.id, index)
            )

        def selected_keys(scope):
            return {
                image_key(messages[message_id], index, attachment)
                for message_id, indices in scope.items()
                for index, attachment in image_entries(messages[message_id])
                if indices is None or index in indices
            }

        original_request = job["request"].split("\n\nFollow-up\n", 1)[0]
        anchors = [(source, original_request)] + [
            (item, item.content)
            for item in sorted(followups, key=lambda item: item.id)
            if item.id != source.id
        ]
        anchor_images = {}
        resolved = {}
        # Every anchor owns its direct images, explicit links and recursive
        # replies. Later attachments cannot satisfy an earlier "above" request.
        # Resolve explicit evidence first, then validate each anchor separately.
        for anchor, text in anchors:
            scope = {anchor.id: None}
            links = re.findall(
                r"https?://(?:www\.)?(?:discord\.com|discordapp\.com)/channels/(\d+)/(\d+)/(\d+)",
                text,
            )
            for guild_id, channel_id, message_id in links:
                if int(guild_id) != int(self.config["guild_id"]):
                    continue
                key = (int(channel_id), int(message_id))
                if key not in resolved:
                    try:
                        channel = self.get_channel(key[0]) or await self.fetch_channel(
                            key[0]
                        )
                        resolved[key] = await channel.fetch_message(key[1])
                    except discord.NotFound as exc:
                        raise ContextClarification(
                            "A requested Discord message or its channel was deleted. Supply a current message link or attach the requested screenshot."
                        ) from exc
                message = resolved[key]
                messages[message.id] = message
                scope[message.id] = None
            pending = [messages[key] for key in scope]
            visited = set()
            while pending:
                message = pending.pop(0)
                if message.id in visited:
                    continue
                visited.add(message.id)
                reference = getattr(message, "reference", None)
                if not reference or not reference.message_id:
                    continue
                if reference.guild_id and int(reference.guild_id) != int(
                    self.config["guild_id"]
                ):
                    continue
                if len(visited) > 40:
                    raise ContextClarification(
                        "The reply chain is too long. Link the specific screenshot message in this request."
                    )
                key = (reference.channel_id or parent.id, reference.message_id)
                if key not in resolved:
                    try:
                        channel = self.get_channel(key[0]) or await self.fetch_channel(
                            key[0]
                        )
                        resolved[key] = await channel.fetch_message(key[1])
                    except discord.NotFound as exc:
                        raise ContextClarification(
                            "A replied-to message or its channel was deleted. Attach the relevant screenshot or provide a current Discord link."
                        ) from exc
                referenced = resolved[key]
                messages[referenced.id] = referenced
                scope[referenced.id] = None
                pending.append(referenced)
            anchor_images[anchor.id] = scope
        # Require a concrete nearby-image reference, not distant occurrences of
        # "screenshot" and "last" in a protocol/code discussion.
        image_noun = r"(?:screenshot|screen\s*shot|image|picture|photo|attachment)s?"
        requested_nearby = re.compile(
            rf"\b{image_noun}\s+(?:(?:shown|posted|attached|from|just|directly|right|I|we)\s+){{0,3}}(?:above|previously|earlier|nearby|last)\b|"
            rf"\b(?:above|previous|earlier|nearby|last)\s+(?:(?:the|my|our|these|those|\d+|two|three|four|five)\s+){{0,2}}{image_noun}\b",
            re.I,
        )
        for anchor, text in anchors:
            text = re.sub(r"```.*?```|`[^`]*`", "", text, flags=re.S)
            if not requested_nearby.search(text):
                continue
            count_match = re.search(
                rf"\b(\d+|two|three|four|five)\s+{image_noun}\b", text, re.I
            )
            count = 1
            if count_match:
                counts = {"two": 2, "three": 3, "four": 4, "five": 5}
                count_text = count_match[1].lower()
                count = counts[count_text] if count_text in counts else int(count_text)
            scope = anchor_images[anchor.id]
            known = selected_keys(scope)
            if len(known) >= count:
                continue
            candidates = (
                nearby
                if anchor.id == source.id
                else [
                    item
                    async for item in thread.history(limit=30, before=anchor)
                    if not item.author.bot
                ]
            )
            plural = not count_match and re.search(
                r"\b(?:screenshots|images|pictures|photos|attachments)\b", text, re.I
            )
            selected = []
            for item in candidates:
                entries = image_entries(item)
                if not entries and selected and plural:
                    break
                indices = []
                for index, attachment in entries:
                    key = image_key(item, index, attachment)
                    if key in known:
                        continue
                    known.add(key)
                    indices.append(index)
                    if not plural and len(known) >= count:
                        break
                if indices:
                    selected.append((item, indices))
                if not plural and len(known) >= count:
                    break
            if len(known) < count:
                raise ContextClarification(
                    "I could not find all requested nearby screenshots. Attach them or supply their Discord message links."
                )
            for item, indices in sorted(selected, key=lambda entry: entry[0].id):
                messages[item.id] = item
                if item.id not in scope:
                    scope[item.id] = indices
                elif scope[item.id] is not None:
                    scope[item.id] = sorted(set(scope[item.id]) | set(indices))
        # Merge after all anchor-local requirements are satisfied. Dedupe the
        # eventual downloads globally, retaining anchor/link/attachment order.
        image_message_ids = {}
        for scope in anchor_images.values():
            for message_id, indices in scope.items():
                if message_id not in image_message_ids:
                    image_message_ids[message_id] = indices
                elif indices is None:
                    image_message_ids[message_id] = None
                elif image_message_ids[message_id] is not None:
                    image_message_ids[message_id] = sorted(
                        set(image_message_ids[message_id]) | set(indices)
                    )
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
        seen_images = set()
        for message_id, indices in image_message_ids.items():
            message = messages[message_id]
            for index, attachment in image_entries(message):
                if indices is not None and index not in indices:
                    continue
                key = image_key(message, index, attachment)
                if key in seen_images:
                    continue
                seen_images.add(key)
                mime = (
                    getattr(attachment, "content_type", None)
                    or mimetypes.guess_type(attachment.filename)[0]
                    or ""
                )
                if attachment.size > 8_000_000:
                    raise ContextClarification(
                        "A requested screenshot exceeds 8 MB. Attach a smaller copy so it can be included in planning."
                    )
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
                        raise ContextClarification(
                            "A requested screenshot has unsupported image bytes. Attach a PNG, JPEG, GIF or WebP copy."
                        )
                    context["images"].append(
                        {
                            "url": "data:"
                            + mime.split(";")[0]
                            + ";base64,"
                            + base64.b64encode(raw).decode()
                        }
                    )
                    image_sources.append(str(message.id))
                except discord.NotFound as exc:
                    raise ContextClarification(
                        "A requested screenshot is no longer available. Attach it again so planning can include it."
                    ) from exc
        await self.progress(
            job["id"],
            f"Retrieved {len(context['messages'])} live Discord context messages and {len(context['images'])} image attachments. Image source messages: {', '.join(image_sources) or 'none'}. {len(errors)} retrieval errors.",
        )
        return context

    async def process_job(self, job_id):
        job = self.get(job_id)
        if (
            not job
            or job["card_retired"]
            or job["state"]
            not in {
                "queued",
                "approved",
                "recovering",
                "waiting",
            }
        ):
            return
        version = job["version"]
        try:
            await self.retire_failure_controls(job_id)
            current = self.get(job_id)
            if (
                not current
                or current["card_retired"]
                or current["version"] != version
                or current["state"]
                not in {"queued", "approved", "recovering", "waiting"}
            ):
                return
            job = current
            if job["state"] in {"recovering", "waiting"}:
                self.update(
                    job_id, state=job["recovery_target"] or "queued", next_retry=0
                )
                job = self.get(job_id)
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
                current = self.get(job_id)
                if (
                    current
                    and current["version"] == version
                    and current["state"] in {"planning", "recovering", "waiting"}
                ):
                    self.sessions.save(
                        job_id, state, self.activity_messages.get(job_id)
                    )

            async def planning_progress(message):
                current = self.get(job_id)
                if (
                    not current
                    or current["version"] != version
                    or current["state"] not in {"planning", "recovering", "waiting"}
                ):
                    raise asyncio.CancelledError
                if message.startswith("Eligible free providers unavailable;"):
                    delay_match = re.search(r"in ([0-9.]+)s", message)
                    delay = float(delay_match[1]) if delay_match else 3
                    self.update(
                        job_id,
                        state="waiting",
                        recovery_target="queued",
                        next_retry=time.time() + delay,
                        note="Waiting for an available free planning route. The checkpoint is preserved; edit or cancel remains available.",
                    )
                    await self.card(job_id)
                elif (
                    message.startswith("Requesting free inference:")
                    and current["state"] != "planning"
                ):
                    self.update(
                        job_id,
                        state="planning",
                        next_retry=0,
                        note="Continuing the saved request on an eligible free route.",
                    )
                    await self.card(job_id)
                await self.progress(job_id, message)

            thread = self.get_channel(job["thread"]) or await self.fetch_channel(
                job["thread"]
            )
            async with thread.typing():
                proposal = await self.changes.propose(
                    job["request"],
                    job_id,
                    planning_progress,
                    context=context,
                    resume=resume["state"] if resume else None,
                    checkpoint=checkpoint,
                )
            if self.get(job_id)["version"] != version or self.get(job_id)[
                "state"
            ] not in {"planning", "recovering", "waiting"}:
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
                self.sessions.save(
                    job_id, saved["state"], self.activity_messages.get(job_id)
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
            self.update(
                job_id,
                state=state,
                proposal=json.dumps(proposal),
                note=note,
                next_retry=0,
                recovery_attempts=0,
            )
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
        except asyncio.CancelledError:
            # Cancellation/revision already changed ownership. A shutdown leaves
            # planning durable; startup alone identifies process interruption.
            raise
        except SourceDeleted:
            current = self.get(job_id)
            if (
                current
                and current["version"] == version
                and current["state"] not in {"cancelled", "done"}
            ):
                self.update(
                    job_id,
                    state="cancelled",
                    version=version + 1,
                    note="Source request deleted. No changes will be applied.",
                )
                await self.card(job_id)
        except ContextClarification as exc:
            current = self.get(job_id)
            if (
                current
                and current["version"] == version
                and current["state"] not in {"cancelled", "done"}
            ):
                self.update(job_id, state="clarify", note=str(exc))
                await self.card(job_id)
        except Exception as exc:
            logging.warning(
                "Request %s awaiting recovery (%s)", job_id, type(exc).__name__
            )
            current = self.get(job_id)
            if not current or current["version"] != version:
                return
            if current["state"] == "applying":
                # Deployment ownership stays with the independent watchdog.
                # Never downgrade an uncertain install into planning/reinstall.
                if not self.self_deployment_enabled():
                    self.deploy_blocked = True
                    self.update(
                        job_id,
                        state="attention",
                        note="Deployment outcome needs operator attention before further changes.",
                    )
                self.wake.set()
                return
            if self.schedule_recovery(job_id, version, exc):
                try:
                    await self.card(job_id)
                    await self.progress(job_id, self.get(job_id)["note"])
                except discord.HTTPException:
                    pass

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
        channel_id = job["source_channel"] or self.config["channel_id"]
        try:
            channel = self.get_channel(channel_id) or await self.fetch_channel(
                channel_id
            )
            await channel.fetch_message(int(job["source"]))
        except discord.NotFound as exc:
            raise SourceDeleted from exc
        current = self.get(job_id)
        if (
            not current
            or current["version"] != version
            or current["state"] != "approved"
        ):
            return
        self.update(
            job_id,
            state="applying",
            note="Approved change is being applied. Checking restart health.",
        )
        try:
            await self.card(job_id)
            await self.progress(
                job_id,
                "Source matches the approved preview. Applying the reviewed files and settings.",
            )
        except discord.HTTPException:
            logging.warning("Deployment activity delivery delayed for job %s", job_id)
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
                    state="attention",
                    note="Rollback needs operator attention. Deployment queue stopped.",
                )
                await self.card(job_id)
                self.deploy_blocked = True
                logging.exception("Rollback failed")
                return
            self.schedule_recovery(
                job_id, version, RuntimeError("Deployment restored"), target="approved"
            )
            await self.card(job_id)
            return
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
