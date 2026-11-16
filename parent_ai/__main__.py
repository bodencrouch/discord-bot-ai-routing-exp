import asyncio
import io
import json
import logging
import os
import re
import sqlite3
from pathlib import Path

import discord

from .changes import Changes, StaleProposal


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
        await interaction.client.revise(
            interaction, self.job_id, self.version, self.request.value
        )


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
        bot = interaction.client
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
        if not job or job["version"] != self.version or job["state"] == "applying":
            await interaction.followup.send(
                "This preview changed. Use the latest card."
            )
            return
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
                bot.update(
                    self.job,
                    state="queued",
                    version=self.version + 1,
                    note="The child changed. Preparing a fresh preview for approval.",
                )
            else:
                bot.update(
                    self.job,
                    state="approved",
                    note="Confirmed. Waiting for the deployment queue.",
                )
        elif self.action == "cancel":
            bot.update(
                self.job,
                state="cancelled",
                version=self.version + 1,
                note="Cancelled. No further changes will be applied.",
            )
        elif self.action == "retry" and job["state"] == "failed":
            bot.update(
                self.job,
                state="queued",
                version=self.version + 1,
                note="Queued for another planning attempt.",
            )
        await bot.card(self.job)
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
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS audit (id INTEGER PRIMARY KEY AUTOINCREMENT, job INTEGER, snapshot TEXT, recorded_at TEXT DEFAULT CURRENT_TIMESTAMP)"
        )
        self.db.commit()
        self.wake = asyncio.Event()
        self.worker = None
        self.active_jobs = {}
        self.deploy_lock = asyncio.Lock()
        self.deploy_blocked = False
        self.add_dynamic_items(RequestButton)

    @staticmethod
    def can_manage(user, job):
        return user.id == job["author"] or bool(
            getattr(getattr(user, "guild_permissions", None), "administrator", False)
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
        self.wake.set()

    async def close(self):
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
            content = f"**Activity · #{job_id}** {message}"
            if len(content) <= 1900:
                await thread.send(content)
            else:
                await thread.send(
                    f"**Activity · #{job_id}** Full model or tool output attached.",
                    file=discord.File(
                        io.BytesIO(message.encode()), filename="activity.txt"
                    ),
                )
        except discord.HTTPException:
            logging.exception("Could not post request activity")

    async def card(self, job_id, attach=False):
        job = self.get(job_id)
        channel = self.get_channel(
            self.config["channel_id"]
        ) or await self.fetch_channel(self.config["channel_id"])
        view = discord.ui.View(timeout=None)
        if job["state"] == "awaiting":
            view.add_item(RequestButton(job_id, job["version"], "approve"))
        if job["state"] != "applying":
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
        if job["state"] == "failed":
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
        embed.set_footer(
            text=f"Revision {job['version']} · requester or administrator approval required"
        )
        kwargs = {"embed": embed, "view": view}
        if attach and job["proposal"]:
            p = json.loads(job["proposal"])
            kwargs["attachments"] = [
                discord.File(
                    io.BytesIO(self.changes.diff(p).encode()),
                    filename=f"change-{job_id}-v{job['version']}.diff",
                ),
                discord.File(
                    io.BytesIO(p["plan"]["summary"].encode()),
                    filename=f"proposal-{job_id}-v{job['version']}.md",
                ),
            ]
        for field, destination in (
            ("card", channel),
            ("thread_card", self.get_channel(job["thread"]) if job["thread"] else None),
        ):
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
            if job[field]:
                try:
                    msg = await destination.fetch_message(job[field])
                    await msg.edit(**params)
                    continue
                except discord.NotFound:
                    pass
            params.pop("attachments", None)
            msg = await destination.send(**params, files=files)
            self.update(job_id, **{field: msg.id})

    async def revise(self, interaction, job_id, version, text):
        job = self.get(job_id)
        if (
            not self.allowed(interaction.channel)
            or not job
            or not self.can_manage(interaction.user, job)
        ):
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
                "🛠️ @mention me with a change for Together AI. I’ll draft it, show the diff, and ask you to confirm. The department runs on consent and questionable coffee."
            )
            return
        job = self.db.execute(
            "SELECT * FROM jobs WHERE thread=? ORDER BY id DESC LIMIT 1",
            (message.channel.id,),
        ).fetchone()
        if job and self.can_manage(message.author, job) and job["state"] != "applying":
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
                    "INSERT OR IGNORE INTO jobs(source,author,request,state) VALUES(?,?,?,?)",
                    (str(message.id), message.author.id, prompt, "queued"),
                )
            if not cur.rowcount:
                return
            job_id = cur.lastrowid
            if isinstance(message.channel, discord.Thread):
                thread = message.channel
            else:
                thread = await message.create_thread(
                    name=f"Skill Issue #{job_id} · {prompt[:65]}",
                    auto_archive_duration=1440,
                )
                await thread.add_user(message.author)
            self.update(job_id, thread=thread.id)
            await self.card(job_id)
            await message.reply(
                f"Filed as **Skill Issue #{job_id}**. Follow-ups go in <#{thread.id}>. I’ll ask before applying anything."
            )
        self.wake.set()

    async def on_raw_message_edit(self, payload):
        job = self.db.execute(
            "SELECT * FROM jobs WHERE source=?", (str(payload.message_id),)
        ).fetchone()
        if not job or job["state"] == "applying" or "content" not in payload.data:
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

    async def work(self):
        await self.wait_until_ready()
        # Interrupted writes have a durable pre-change snapshot. Restore before accepting more work.
        for job in self.db.execute(
            "SELECT * FROM jobs WHERE state='applying'"
        ).fetchall():
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
                "SELECT * FROM jobs WHERE state IN ('approved','queued') ORDER BY id"
            ).fetchall():
                if job["id"] not in self.active_jobs and not (
                    self.deploy_blocked and job["state"] == "approved"
                ):
                    task = asyncio.create_task(self.process_job(job["id"]))
                    self.active_jobs[job["id"]] = task
                    task.add_done_callback(
                        lambda _, job_id=job["id"]: self.job_finished(job_id)
                    )
            await self.wake.wait()

    def job_finished(self, job_id):
        self.active_jobs.pop(job_id, None)
        self.wake.set()

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
            proposal = await self.changes.propose(
                job["request"], job_id, lambda message: self.progress(job_id, message)
            )
            if (
                self.get(job_id)["version"] != version
                or self.get(job_id)["state"] != "planning"
            ):
                return
            plan = proposal["plan"]
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
            if self.get(job_id)["version"] == version:
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
                note="Child changed. Preparing a fresh approval preview.",
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
