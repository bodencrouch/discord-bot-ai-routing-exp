"""One supervisor for independently configured Discord clients and existing stores."""

import asyncio
import hashlib
import importlib
import weakref
import json
import logging
import time
from pathlib import Path


from parent_ai.__main__ import Parent
from together_ai.hub import HubTogether, telemetry_value
from .presets import PRESETS


class HubParent(Parent):
    def __init__(self, config, instance, state, restart_chat):
        self.instance, self.hub_state = instance, state
        super().__init__(config)
        self.changes.restart = restart_chat
        # The maintenance agent must remain recoverable while editing its child.
        original_editable = self.changes.editable
        self.changes.editable = lambda name: (
            not Path(name).parts[0] == "bot_hub" and original_editable(name)
        )
        original_post = self.changes.http.post

        async def post(url, **kwargs):
            model = kwargs.get("json", {}).get("model")
            started = time.monotonic()
            self.emit("model.started", {"model": model})
            try:
                response = await original_post(url, **kwargs)
                if not response.is_success:
                    self.emit(
                        "model.failed", {"model": model, "status": response.status_code}
                    )
                    return response
                payload = response.json()
                self.emit(
                    "model.completed",
                    {
                        "model": model,
                        "actual_model": payload.get("model"),
                        "usage": payload.get("usage"),
                        "choices": telemetry_value(payload.get("choices")),
                        "latency_ms": round((time.monotonic() - started) * 1000),
                    },
                )
                return response
            except Exception as error:
                self.emit(
                    "model.failed", {"model": model, "error": type(error).__name__}
                )
                raise

        self.changes.http.post = post

    def emit(self, kind, payload):
        return self.hub_state.record(self.instance["id"], kind, payload)

    def allowed(self, channel):
        channels = {str(value) for value in self.instance.get("channels", [])}
        if not channels:
            return super().allowed(channel)
        return bool(
            channel
            and getattr(channel, "guild", None)
            and str(channel.guild.id) == str(self.config["guild_id"])
            and (
                str(channel.id) in channels
                or str(getattr(channel, "parent_id", "")) in channels
            )
        )

    async def on_ready(self):
        self.emit("connection.ready", {"user_id": str(self.user.id)})
        await super().on_ready()

    async def on_disconnect(self):
        self.emit("connection.disconnected", {})

    def update(self, job_id, **fields):
        super().update(job_id, **fields)
        self.emit(
            "maintenance.updated",
            {
                "job_id": job_id,
                "state": fields.get("state"),
                "version": fields.get("version"),
                "note": fields.get("note"),
            },
        )
        if fields.get("proposal"):
            proposal = json.loads(fields["proposal"])
            self.emit(
                "maintenance.proposal",
                {
                    "job_id": job_id,
                    "plan": proposal.get("plan"),
                    "diff": self.changes.diff(proposal),
                    "dry_run": bool(self.instance.get("dry_run")),
                },
            )

    async def progress(self, job_id, message):
        self.emit("maintenance.progress", {"job_id": job_id, "message": message})
        if message.startswith(("Tool call **", "Tool result **")):
            completed = message.startswith("Tool result")
            self.emit(
                "tool.completed" if completed else "tool.started",
                {"job_id": job_id, "name": message.split("**")[1], "message": message},
            )
        await super().progress(job_id, message)

    async def on_message(self, message):
        if not message.author.bot and self.allowed(message.channel):
            self.emit(
                "message.received",
                {
                    "message_id": str(message.id),
                    "channel_id": str(message.channel.id),
                    "user_id": str(message.author.id),
                    "characters": len(message.content),
                },
            )
            if self.user and self.user.id in message.raw_mentions:
                self.emit(
                    "maintenance.request",
                    {
                        "user_id": str(message.author.id),
                        "channel_id": str(message.channel.id),
                        "request": message.content,
                    },
                )
        await super().on_message(message)

    async def work(self):
        if not self.instance.get("dry_run"):
            await super().work()
            return
        await self.wait_until_ready()
        # Dry runs can re-plan interrupted jobs, but never restore or install source.
        with self.db:
            self.db.execute("UPDATE jobs SET state='queued' WHERE state='planning'")
        while not self.is_closed():
            self.wake.clear()
            for job in self.db.execute(
                "SELECT id FROM jobs WHERE state IN ('queued','approved') ORDER BY id"
            ).fetchall():
                if job["id"] not in self.active_jobs:
                    task = asyncio.create_task(self.process_job(job["id"]))
                    self.active_jobs[job["id"]] = task
                    task.add_done_callback(
                        lambda _, job_id=job["id"]: self.job_finished(job_id)
                    )
            await self.wake.wait()

    async def deploy(self, job_id, version, job):
        if self.instance.get("dry_run"):
            self.update(
                job_id,
                state="awaiting",
                note="Dry-run proposal reviewed. Disable dry run and approve again to apply.",
            )
            self.emit("maintenance.draft", {"job_id": job_id, "reviewed": True})
            await self.card(job_id, attach=True)
            return
        await super().deploy(job_id, version, job)


class Runtime:
    """Manage a registry of clients. start returns after scheduling supervisors."""

    def __init__(self, config, state):
        self.config, self.state = config, state
        self.clients, self.tasks, self.errors, self.tokens = {}, {}, {}, {}
        self.closed_clients = weakref.WeakSet()
        self.component_hosts = {}
        self.component_dispatch_tasks = {}
        self.closed = False
        self.registry_lock = asyncio.Lock()

    def instances(self):
        merged = {
            str(item["id"]): dict(item) for item in self.config.get("instances", [])
        }
        for item in self.state.instances():
            merged.setdefault(str(item["id"]), {}).update(item)
        result = []
        for instance_id, item in merged.items():
            preset = item.get("preset", "chat")
            defaults = PRESETS.get(preset, {}).get("defaults", {})
            instance = {**defaults, **item, "id": instance_id}
            try:
                settings = json.loads(self._config_path(instance).read_text())
                instance["model"] = self._canonical_model(instance, settings)
            except (OSError, ValueError, TypeError):
                # Invalid settings are reported when apply attempts to build the client.
                pass
            result.append(instance)
        return result

    def _config_path(self, instance):
        return (
            Path(
                instance.get("config_file")
                or self.config.get("gateway_config_file", "config.json")
            )
            .expanduser()
            .resolve()
        )

    @staticmethod
    def _canonical_model(instance, settings):
        """Keep each preset's existing config field authoritative over registry metadata."""
        preset = instance.get("preset", "chat")
        field = "model" if preset == "maintenance" else "default_model"
        model = (
            settings.get(field)
            or settings.get("default_model")
            or settings.get("model")
            or instance.get("model")
            or PRESETS[preset]["defaults"]["model"]
        )
        if not isinstance(model, str) or not model.strip():
            raise ValueError("Configure an exact model route ID")
        return model

    def status(self):
        """Report physical connections and attached logical components separately."""
        statuses = []
        for item in self.instances():
            instance_id = item["id"]
            client = self.clients.get(instance_id)
            task = self.tasks.get(instance_id)
            shared = bool(item.get("identity_instance"))
            host = self.clients.get(item.get("identity_instance")) if shared else client
            attached = bool(client and getattr(client, "_hub_attached", False))
            connected = bool(host and host.is_ready() and (not shared or attached))
            running = bool(
                client and (attached if shared else task and not task.done())
            )
            if shared and task and not task.done():
                running = True
            statuses.append(
                {
                    "id": instance_id,
                    "name": item.get("name", instance_id),
                    "preset": item.get("preset", "chat"),
                    "enabled": item.get("enabled", False),
                    "dry_run": item.get("dry_run", False),
                    "identity_instance": item.get("identity_instance"),
                    "connected": connected,
                    "running": running,
                    "error": self.errors.get(instance_id),
                }
            )
        return statuses

    async def start(self):
        self.closed = False
        saved = {str(item["id"]) for item in self.state.instances()}
        for instance in self.config.get("instances", []):
            if str(instance["id"]) not in saved:
                self.state.save_instance(instance)
        for instance in self.instances():
            await self.apply(instance["id"])

    async def apply(self, instance_id):
        """Validate a replacement before closing the currently connected identity."""
        instance_id = str(instance_id)
        async with self.registry_lock:
            instance = next(
                (item for item in self.instances() if item["id"] == instance_id), None
            )
            if self.closed or not instance or not instance.get("enabled", False):
                await self._stop(instance_id)
                self.errors.pop(instance_id, None)
                return
            if instance.get("identity_instance"):
                await self._start_component(instance)
                return
            client = None
            try:
                client, token = self._build(instance)
                digest = hashlib.sha256(token.encode()).hexdigest()
                if any(
                    other != instance_id and value == digest
                    for other, value in self.tokens.items()
                ):
                    raise ValueError(
                        "Discord identity is already assigned to an enabled instance"
                    )
            except Exception as error:
                if client is not None:
                    await self._close_client(client)
                self.errors[instance_id] = type(error).__name__
                self.state.record(
                    instance_id, "instance.failed", {"error": type(error).__name__}
                )
                logging.error(
                    "Hub instance %s failed to configure (%s)",
                    instance_id,
                    type(error).__name__,
                )
                return
            await self._stop(instance_id)
            self.errors.pop(instance_id, None)
            self.clients[instance_id] = client
            self.tokens[instance_id] = digest
            self.tasks[instance_id] = asyncio.create_task(
                self._supervise(instance, token), name=f"bot-hub:{instance_id}"
            )
            await self._start_dependents(instance_id)

    def _build(self, instance):
        preset = instance.get("preset", "chat")
        if preset not in PRESETS:
            raise ValueError("Unknown bot preset")
        path = self._config_path(instance)
        config = json.loads(path.read_text())
        model = self._canonical_model(instance, config)
        instance = {**instance, "model": model}
        config["default_model"] = model
        saved = next(
            (item for item in self.state.instances() if item["id"] == instance["id"]),
            None,
        )
        if saved is None or saved.get("model") != model:
            self.state.save_instance({"id": instance["id"], "model": model})
        if instance.get("identity_instance"):
            if preset != "predictor":
                raise ValueError("Only predictor presets support a shared identity")
            from .predictor import Predictor

            return Predictor(instance, self.state, config), None
        token_file = (
            instance.get("token_file")
            or config.get("discord_token_file")
            or config.get("token_file")
        )
        token = Path(token_file).expanduser().read_text().strip()
        if not token:
            raise ValueError("Empty Discord token file")
        if preset == "chat":
            client = HubTogether(path, config, instance, self.state)
            client.hub_runtime = self
            return client, token
        if preset == "maintenance":
            config["guild_id"] = int(config["guild_id"])
            config["model"] = model
            if instance.get("channels"):
                config["channel_id"] = int(instance["channels"][0])
            return HubParent(config, instance, self.state, self.restart_chat), token
        from .predictor import Predictor

        return Predictor(instance, self.state, config), token

    async def _supervise(self, instance, token):
        instance_id = instance["id"]
        delay = 2
        while not self.closed:
            client = self.clients[instance_id]
            try:
                self.state.record(instance_id, "connection.starting", {})
                await client.start(token, reconnect=True)
                self.errors[instance_id] = "ConnectionClosed"
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.errors[instance_id] = type(error).__name__
                self.state.record(
                    instance_id, "connection.failed", {"error": type(error).__name__}
                )
            await self._stop_dependents(instance_id)
            await self._close_client(client)
            if self.closed or self.errors.get(instance_id) in {
                "LoginFailure",
                "PrivilegedIntentsRequired",
            }:
                return
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)
            try:
                client, token = self._build(instance)
                self.clients[instance_id] = client
                await self._start_dependents(instance_id)
            except Exception as error:
                self.errors[instance_id] = type(error).__name__
                return

    async def _start_component(self, instance):
        instance_id = instance["id"]
        host_id = str(instance["identity_instance"])
        host_config = next(
            (item for item in self.instances() if item["id"] == host_id), None
        )
        component = None
        try:
            if (
                not host_config
                or host_config.get("preset", "chat") != "chat"
                or not host_config.get("enabled")
            ):
                raise ValueError("Shared predictor requires an enabled chat identity")
            component, _ = self._build(instance)
        except Exception as error:
            self.errors[instance_id] = type(error).__name__
            self.state.record(
                instance_id, "component.failed", {"error": type(error).__name__}
            )
            return
        await self._stop(instance_id)
        self.errors.pop(instance_id, None)
        self.clients[instance_id] = component
        self.component_hosts[instance_id] = host_id
        self.component_dispatch_tasks[instance_id] = set()
        self.tasks[instance_id] = asyncio.create_task(
            self._attach_component(instance_id, host_id, component),
            name=f"bot-hub-component:{instance_id}",
        )

    async def _attach_component(self, instance_id, host_id, component):
        try:
            host = self.clients.get(host_id)
            if host is None:
                return
            await host.wait_until_ready()
            if (
                self.clients.get(instance_id) is not component
                or self.clients.get(host_id) is not host
            ):
                return
            await component.attach(host)
            component._hub_attached = True
            self.state.record(
                instance_id, "component.attached", {"identity_instance": host_id}
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.errors[instance_id] = type(error).__name__
            self.state.record(
                instance_id, "component.failed", {"error": type(error).__name__}
            )

    async def _start_dependents(self, host_id):
        for instance in self.instances():
            if instance.get("identity_instance") == host_id and instance.get("enabled"):
                await self._start_component(instance)

    async def _stop_dependents(self, host_id):
        for instance_id, identity_id in list(self.component_hosts.items()):
            if identity_id == host_id:
                await self._stop(instance_id)

    def dispatch_components(self, host_id, message):
        """Schedule prediction and grading without delaying the host conversation."""
        for instance_id, identity_id in list(self.component_hosts.items()):
            component = self.clients.get(instance_id)
            if (
                identity_id == host_id
                and component
                and getattr(component, "_hub_attached", False)
            ):
                if component.allowed(message.channel):
                    self.state.record(
                        instance_id,
                        "component.message_received",
                        {
                            "message_id": str(message.id),
                            "channel_id": str(message.channel.id),
                            "bot": bool(message.author.bot),
                        },
                    )
                task = asyncio.create_task(
                    self._component_message(instance_id, component, message)
                )
                pending = self.component_dispatch_tasks.setdefault(instance_id, set())
                pending.add(task)
                task.add_done_callback(pending.discard)

    async def _component_message(self, instance_id, component, message):
        try:
            await component.on_message(message)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.state.record(
                instance_id, "component.message_failed", {"error": type(error).__name__}
            )

    async def restart_chat(self):
        """Refresh child dependencies/source and reconnect through this supervisor."""
        global HubTogether, telemetry_value
        instances = [
            item
            for item in self.instances()
            if item.get("preset", "chat") == "chat" and item.get("enabled")
        ]
        root = Path(__file__).resolve().parents[1]
        await self._command("uv", "sync", "--project", str(root))
        await self._command("systemctl", "--user", "daemon-reload")
        async with self.registry_lock:
            for instance in instances:
                await self._stop(instance["id"])
            importlib.invalidate_caches()
            for module_name in (
                "together_ai.core",
                "together_ai.routing",
                "together_ai.conversations",
                "together_ai.bot",
                "together_ai.hub",
            ):
                importlib.reload(importlib.import_module(module_name))
            child_module = importlib.import_module("together_ai.hub")
            HubTogether = child_module.HubTogether
            telemetry_value = child_module.telemetry_value
        for instance in instances:
            await self.apply(instance["id"])
        for instance in instances:
            client = self.clients.get(instance["id"])
            if client is None:
                raise RuntimeError("Chat instance could not restart")
            await asyncio.wait_for(client.wait_until_ready(), timeout=90)
            if not client.is_ready():
                raise RuntimeError("Chat instance did not reconnect")

    async def _command(self, *args):
        process = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
        )
        try:
            await asyncio.wait_for(process.wait(), timeout=120)
        except BaseException:
            if process.returncode is None:
                process.kill()
                await process.wait()
            raise
        if process.returncode:
            raise RuntimeError("Child dependency or service metadata refresh failed")

    async def _close_client(self, client):
        if client not in self.closed_clients:
            self.closed_clients.add(client)
            await client.close()

    async def _stop(self, instance_id):
        await self._stop_dependents(instance_id)
        task = self.tasks.pop(instance_id, None)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        pending = self.component_dispatch_tasks.pop(instance_id, set())
        for dispatch_task in pending:
            dispatch_task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        self.component_hosts.pop(instance_id, None)
        client = self.clients.pop(instance_id, None)
        if client:
            try:
                await self._close_client(client)
            except Exception as error:
                self.state.record(
                    instance_id,
                    "connection.close_failed",
                    {"error": type(error).__name__},
                )
        self.tokens.pop(instance_id, None)

    async def close(self):
        self.closed = True
        async with self.registry_lock:
            for instance_id in list(self.clients):
                await self._stop(instance_id)
