"""Loopback dashboard with authenticated sessions and same-origin mutations."""

import asyncio
import hmac
import json
import mimetypes
import secrets
import sqlite3
import threading
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse


class Dashboard:
    def __init__(self, hub, state, config, loop):
        self.hub, self.state, self.config, self.loop = hub, state, config, loop
        self.token = secrets.token_urlsafe(32)
        self.session = secrets.token_urlsafe(32)
        self.static = Path(__file__).parent / "static"
        self.models = []
        self.model_error = None
        self.server = None

    async def refresh_models(self):
        import httpx

        try:
            gateway = json.loads(Path(self.config["gateway_config_file"]).read_text())
            async with httpx.AsyncClient(timeout=30) as client:
                result = await client.get(
                    gateway["base_url"] + "/models",
                    headers={
                        "Authorization": "Bearer "
                        + Path(gateway["api_key_file"]).read_text().strip()
                    },
                )
                result.raise_for_status()
                self.models = result.json()["data"]
                self.model_error = None
        except Exception as exc:
            self.model_error = type(exc).__name__

    def jobs(self):
        jobs = []
        for instance in self.state.instances():
            if instance.get("preset") != "maintenance":
                continue
            try:
                config = json.loads(Path(instance["config_file"]).read_text())
                connection = sqlite3.connect(
                    "file:" + config["database"] + "?mode=ro", uri=True
                )
                connection.row_factory = sqlite3.Row
                for row in connection.execute(
                    "SELECT * FROM jobs ORDER BY id DESC LIMIT 100"
                ):
                    job = dict(row)
                    job["instance_id"] = instance["id"]
                    proposal = (
                        json.loads(job["proposal"]) if job.get("proposal") else None
                    )
                    job["summary"] = (
                        proposal["plan"]["summary"] if proposal else job["note"]
                    )
                    jobs.append(job)
                connection.close()
            except (OSError, sqlite3.Error, ValueError, KeyError):
                continue
        return jobs

    def conversations(self):
        rooms = []
        for instance in self.state.instances():
            if instance.get("preset") != "chat":
                continue
            try:
                config = json.loads(Path(instance["config_file"]).read_text())
                connection = sqlite3.connect(
                    "file:" + config["database"] + "?mode=ro", uri=True
                )
                connection.row_factory = sqlite3.Row
                for room in connection.execute(
                    "SELECT room,COUNT(*) AS message_count FROM messages GROUP BY room ORDER BY MAX(id) DESC LIMIT 100"
                ).fetchall():
                    title = connection.execute(
                        "SELECT title FROM conversations WHERE room=?", (room["room"],)
                    ).fetchone()
                    turns = [
                        dict(row)
                        for row in connection.execute(
                            "SELECT key,prompt,answer,author_name,model,status,version,source_id,output_ids FROM turns WHERE room=? ORDER BY seq DESC LIMIT 50",
                            (room["room"],),
                        )
                    ]
                    rooms.append(
                        {
                            "instance_id": instance["id"],
                            "guild_id": str(config["guild_id"]),
                            "room": room["room"],
                            "title": title["title"] if title else room["room"],
                            "message_count": room["message_count"],
                            "turns": turns,
                        }
                    )
                connection.close()
            except (OSError, sqlite3.Error, ValueError, KeyError):
                continue
        return rooms

    def turn(self, instance_id, key):
        instance = next(
            (
                entry
                for entry in self.state.instances()
                if entry["id"] == instance_id and entry.get("preset") == "chat"
            ),
            None,
        )
        if not instance:
            raise ValueError("Chat instance not found.")
        config = json.loads(Path(instance["config_file"]).read_text())
        with sqlite3.connect(
            "file:" + config["database"] + "?mode=ro", uri=True
        ) as connection:
            connection.row_factory = sqlite3.Row
            record = connection.execute(
                "SELECT * FROM turns WHERE key=?", (key,)
            ).fetchone()
            raw = connection.execute(
                "SELECT payload FROM raw_results WHERE id=?", (key,)
            ).fetchone()
            if not record:
                raise ValueError("Turn not found.")
            return {
                "turn": dict(record),
                "raw": json.loads(raw["payload"]) if raw else None,
            }

    def snapshot(self):
        from .presets import PRESETS

        status = self.hub.status()
        if isinstance(status, list):
            status = {entry["id"]: entry for entry in status}
        events = self.state.events(limit=500)
        feedback = self.state.feedbacks(limit=500)
        predictions = self.state.predictions(limit=200)
        kinds = {}
        for event in events:
            kinds[event["kind"]] = kinds.get(event["kind"], 0) + 1
        presets = list(PRESETS.values()) if isinstance(PRESETS, dict) else PRESETS
        instances = self.state.instances()
        for instance in instances:
            try:
                settings = json.loads(Path(instance["config_file"]).read_text())
                instance["configuration"] = {
                    key: value
                    for key, value in settings.items()
                    if key
                    not in {
                        "discord_token_file",
                        "api_key_file",
                        "token_file",
                        "database",
                        "target",
                    }
                }
            except (OSError, ValueError, KeyError):
                instance["configuration"] = {}
        return {
            "instances": instances,
            "presets": presets,
            "status": status,
            "events": events,
            "predictions": predictions,
            "feedback": feedback,
            "models": self.models,
            "model_error": self.model_error,
            "jobs": self.jobs(),
            "conversations": self.conversations(),
            "analytics": {
                **self.state.analytics(),
                "event_counts_in_window": kinds,
                "events_in_window": len(events),
                "predictions_in_window": len(predictions),
                "feedback_in_window": len(feedback),
            },
        }

    async def mutate(self, path, data):
        from .presets import PRESETS

        parts = path.strip("/").split("/")
        if (
            len(parts) == 4
            and parts[:2] == ["api", "instances"]
            and parts[3] == "config"
        ):
            instance = next(
                (entry for entry in self.state.instances() if entry["id"] == parts[2]),
                None,
            )
            if not instance:
                raise ValueError("Instance not found.")
            if set(data) & {
                "discord_token_file",
                "api_key_file",
                "token_file",
                "database",
                "target",
            }:
                raise ValueError(
                    "Use private credential files for credential settings."
                )
            if "semantic_threshold" in data and (
                type(data["semantic_threshold"]) not in {int, float}
                or not 0 <= data["semantic_threshold"] <= 1
            ):
                raise ValueError("Semantic threshold must be between zero and one.")
            path = Path(instance["config_file"])
            settings = json.loads(path.read_text())
            settings.update(data)
            if instance.get("preset") == "maintenance" and (
                "default_model" in data or "model" in data
            ):
                route = data.get("default_model", data.get("model"))
                settings.update(default_model=route, model=route)
            if (
                ("default_model" in data or "model" in data)
                and self.models
                and data.get("default_model", data.get("model"))
                not in {m["id"] for m in self.models}
            ):
                raise ValueError("Choose an exact gateway route ID.")
            temporary = path.with_name(path.name + ".hub-tmp")
            temporary.write_text(json.dumps(settings, indent=2) + "\n")
            temporary.chmod(0o600)
            temporary.replace(path)
            if "default_model" in data:
                instance["model"] = data["default_model"]
            elif "model" in data:
                instance["model"] = data["model"]
            for key in ("owner_id", "learning_directory", "together_bot_ids"):
                if key in data:
                    instance[key] = data[key]
            self.state.save_instance(instance)
            self.state.record(
                instance["id"], "configuration", {"updated_fields": list(data)}
            )
            await self.hub.apply(instance["id"])
            return {"ok": True}
        if parts[:2] == ["api", "instances"]:
            instance_id = parts[2] if len(parts) == 3 else data.get("id")
            if (
                not isinstance(instance_id, str)
                or not instance_id.replace("-", "").replace("_", "").isalnum()
            ):
                raise ValueError(
                    "Use letters, numbers, hyphens or underscores for the instance ID."
                )
            existing = next(
                (
                    entry
                    for entry in self.state.instances()
                    if entry["id"] == instance_id
                ),
                None,
            )
            fields = {
                "name",
                "preset",
                "enabled",
                "dry_run",
                "token_file",
                "config_file",
                "channels",
                "model",
                "owner_id",
                "learning_directory",
                "together_bot_ids",
                "identity_instance",
                "thread_scope",
            }
            if set(data) - fields - {"id"}:
                raise ValueError("Unknown instance setting.")
            preset_name = data.get("preset") or (existing or {}).get("preset", "chat")
            if not isinstance(preset_name, str) or preset_name not in PRESETS:
                raise ValueError("Choose a bundled preset.")
            defaults = PRESETS.get(preset_name, {}).get("defaults", {})
            instance = {
                **defaults,
                **(existing or {}),
                **{key: value for key, value in data.items() if key in fields},
                "id": instance_id,
                "preset": preset_name,
            }
            if instance.get("preset") not in PRESETS:
                raise ValueError("Choose a bundled preset.")
            for key in ("enabled", "dry_run"):
                if key in instance and type(instance[key]) is not bool:
                    raise ValueError(key + " must be true or false.")
            identity = instance.get("identity_instance", "")
            if not isinstance(identity, str):
                raise ValueError("Choose a shared Discord identity by instance ID.")
            if identity:
                host = next(
                    (item for item in self.state.instances() if item["id"] == identity),
                    None,
                )
                if (
                    instance["preset"] != "predictor"
                    or identity == instance_id
                    or not host
                    or host.get("preset") != "chat"
                    or not host.get("enabled")
                    or host.get("identity_instance")
                ):
                    raise ValueError(
                        "Shared predictors require an enabled chat identity."
                    )
            scope = instance.get("thread_scope", "channels")
            if not isinstance(scope, str) or scope not in {"channels", "conversations"}:
                raise ValueError("Choose channels or conversations for thread scope.")
            if instance.get("thread_scope") == "conversations" and not identity:
                raise ValueError("Conversation scope requires a shared chat identity.")
            channels = instance.get("channels", [])
            if not isinstance(channels, list) or any(
                not str(value).isdigit() for value in channels
            ):
                raise ValueError("Channels must be a list of Discord channel IDs.")
            for key in ("token_file", "config_file"):
                value = instance.get(key, "")
                if not isinstance(value, str):
                    raise ValueError(key + " must be a private file path.")
                required = key == "token_file" and not identity
                required = required or (key == "config_file" and bool(value))
                if required and instance.get("enabled") and not Path(value).is_file():
                    raise ValueError(
                        "Configure an existing private " + key + " path first."
                    )
            if not isinstance(instance.get("model"), str):
                raise ValueError("Choose an exact OmniRoute route ID.")
            if self.models and instance.get("model") not in {
                model["id"] for model in self.models
            }:
                raise ValueError("Choose an exact OmniRoute route ID.")
            if not instance.get("config_file"):
                template = next(
                    (
                        item
                        for item in self.state.instances()
                        if item.get("preset") == instance["preset"]
                    ),
                    None,
                )
                source = Path(
                    template["config_file"]
                    if template
                    else self.config["gateway_config_file"]
                )
                settings = json.loads(source.read_text())
                settings["default_model"] = instance["model"]
                private_directory = Path(self.config["database"]).parent
                if instance["preset"] in {"chat", "maintenance"}:
                    settings["database"] = str(
                        private_directory / (instance_id + "-history.sqlite")
                    )
                if instance["preset"] == "maintenance":
                    settings["model"] = instance["model"]
                settings_path = private_directory / (instance_id + "-config.json")
                if settings_path.exists():
                    raise ValueError(
                        "The generated configuration path already exists. Choose it explicitly or use another ID."
                    )
                with settings_path.open("x") as config_file:
                    config_file.write(json.dumps(settings, indent=2) + "\n")
                settings_path.chmod(0o600)
                instance["config_file"] = str(settings_path)
                if template:
                    for key in ("owner_id", "together_bot_ids", "learning_directory"):
                        if key in template and key not in instance:
                            instance[key] = template[key]
            elif "model" in data:
                settings_path = Path(instance["config_file"])
                settings = json.loads(settings_path.read_text())
                settings["default_model"] = instance["model"]
                if instance["preset"] == "maintenance":
                    settings["model"] = instance["model"]
                temporary = settings_path.with_name(settings_path.name + ".hub-tmp")
                temporary.write_text(json.dumps(settings, indent=2) + "\n")
                temporary.chmod(0o600)
                temporary.replace(settings_path)
            self.state.save_instance(instance)
            self.state.record(
                instance_id,
                "configuration",
                {"changes": {key: instance.get(key) for key in data if key in fields}},
            )
            await self.hub.apply(instance_id)
            return {"ok": True, "instance": instance}
        if (
            len(parts) == 4
            and parts[:2] == ["api", "predictions"]
            and parts[3] == "feedback"
        ):
            prediction_id = parts[2]
            found = self.state.prediction(prediction_id)
            if not found:
                raise ValueError("Prediction not found.")
            kind = data.get("kind")
            value = data.get("value")
            if kind not in {"vote", "edit", "review"} or value is None:
                raise ValueError("Provide a vote, edit or review.")
            if len(str(value)) > 8000:
                raise ValueError("Keep feedback under 8,000 characters.")
            result = self.state.feedback(prediction_id, "dashboard-owner", kind, value)
            if kind == "edit":
                found.setdefault("payload", {})["edited_draft"] = str(value)
                found["payload"]["editor_id"] = "dashboard-owner"
                self.state.save_prediction(found)
                client = self.hub.clients.get(found.get("instance_id"))
                if client and found.get("message_id"):
                    try:
                        import discord

                        channel = client.get_channel(
                            int(found["channel_id"])
                        ) or await client.fetch_channel(int(found["channel_id"]))
                        message = await channel.fetch_message(int(found["message_id"]))
                        embed = discord.Embed(
                            title="Bot prediction · Dashboard-edited draft",
                            description=str(value)[:1800],
                            color=0x78909C,
                        )
                        embed.set_footer(
                            text="Edited in the dashboard · Original prediction retained for evaluation"
                        )
                        await message.edit(content=None, embed=embed)
                    except (discord.HTTPException, AttributeError, KeyError):
                        self.state.record(
                            found["instance_id"],
                            "prediction.edit_delivery_failed",
                            {"prediction_id": prediction_id},
                        )
            return {"ok": True, "feedback": result}
        raise ValueError("Unknown action.")

    def start(self):
        dashboard = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, status, body, mime="application/json"):
                data = (
                    json.dumps(body, ensure_ascii=False).encode()
                    if mime == "application/json"
                    else body
                )
                self.send_response(status)
                self.send_header("Content-Type", mime)
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("X-Frame-Options", "DENY")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def authenticated(self):
                try:
                    cookie = SimpleCookie(self.headers.get("Cookie", ""))
                    token = cookie.get("hub_session")
                    return token is not None and hmac.compare_digest(
                        token.value, dashboard.session
                    )
                except Exception:
                    return False

            def do_GET(self):
                request = urlparse(self.path)
                if request.path == "/login":
                    token = parse_qs(request.query).get("token", [""])[0]
                    if not hmac.compare_digest(token, dashboard.token):
                        self.reply(403, {"error": "Invalid dashboard login."})
                        return
                    self.send_response(303)
                    self.send_header("Location", "/")
                    self.send_header(
                        "Set-Cookie",
                        "hub_session="
                        + dashboard.session
                        + "; HttpOnly; SameSite=Strict; Path=/",
                    )
                    self.send_header("Cache-Control", "no-store")
                    self.end_headers()
                    return
                if not self.authenticated():
                    self.reply(
                        401,
                        b"<h1>Bot Hub</h1><p>Open the private dashboard login link saved alongside the hub configuration.</p>",
                        "text/html",
                    )
                    return
                try:
                    if request.path == "/api/state":
                        self.reply(
                            200,
                            asyncio.run_coroutine_threadsafe(
                                dashboard.get_state(), dashboard.loop
                            ).result(20),
                        )
                    elif request.path.startswith("/api/turns/"):
                        parts = request.path.strip("/").split("/")
                        if len(parts) != 4:
                            raise ValueError("Invalid turn URL.")
                        self.reply(
                            200,
                            asyncio.run_coroutine_threadsafe(
                                dashboard.get_turn(
                                    unquote(parts[2]), unquote(parts[3])
                                ),
                                dashboard.loop,
                            ).result(20),
                        )
                    elif request.path.startswith("/api/export/"):
                        instance_id = request.path.removeprefix("/api/export/")
                        data = asyncio.run_coroutine_threadsafe(
                            dashboard.get_state(), dashboard.loop
                        ).result(20)
                        exported = {
                            key: [
                                entry
                                for entry in data[key]
                                if entry.get("instance_id", entry.get("id"))
                                == instance_id
                            ]
                            for key in ("instances", "jobs")
                        }
                        exported["events"] = asyncio.run_coroutine_threadsafe(
                            dashboard.export_events(instance_id), dashboard.loop
                        ).result(20)
                        self.reply(200, exported)
                    else:
                        name = (
                            "index.html"
                            if request.path == "/"
                            else request.path.removeprefix("/static/").lstrip("/")
                        )
                        path = (dashboard.static / name).resolve()
                        if (
                            not path.is_relative_to(dashboard.static.resolve())
                            or not path.is_file()
                        ):
                            self.reply(404, {"error": "Page not found."})
                            return
                        self.reply(
                            200,
                            path.read_bytes(),
                            mimetypes.guess_type(path.name)[0]
                            or "application/octet-stream",
                        )
                except Exception:
                    self.reply(
                        500,
                        {
                            "error": "Dashboard request failed. Check the hub service log."
                        },
                    )

            def do_POST(self):
                if not self.authenticated():
                    self.reply(401, {"error": "Sign in to the dashboard first."})
                    return
                origin = self.headers.get("Origin")
                host = self.headers.get("Host")
                if origin != "http://" + str(host):
                    self.reply(
                        403, {"error": "A same-origin dashboard request is required."}
                    )
                    return
                try:
                    size = int(self.headers.get("Content-Length", "0"))
                    if not 0 < size <= 65536:
                        raise ValueError("Invalid request size.")
                    data = json.loads(self.rfile.read(size))
                    if not isinstance(data, dict):
                        raise ValueError("Provide an object.")
                    result = asyncio.run_coroutine_threadsafe(
                        dashboard.mutate(urlparse(self.path).path, data), dashboard.loop
                    ).result(90)
                    self.reply(200, result)
                except ValueError as exc:
                    self.reply(400, {"error": str(exc)})
                except Exception:
                    self.reply(
                        500,
                        {
                            "error": "The change could not be applied. Check instance status."
                        },
                    )

        self.server = ThreadingHTTPServer(
            ("127.0.0.1", self.config.get("dashboard_port", 18741)), Handler
        )
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        return f"http://127.0.0.1:{self.server.server_port}/login?token={self.token}"

    async def get_state(self):
        return await asyncio.to_thread(self.snapshot)

    async def get_turn(self, instance_id, key):
        return await asyncio.to_thread(self.turn, instance_id, key)

    async def export_events(self, instance_id):
        predictions = self.state.predictions(instance_id, limit=None)
        exported = {
            "activity": self.state.events(instance_id, limit=None),
            "predictions": predictions,
            "feedback": [
                entry
                for prediction in predictions
                for entry in self.state.feedbacks(prediction["id"], limit=None)
            ],
        }
        instance = next(
            (item for item in self.state.instances() if item["id"] == instance_id), None
        )
        if instance and instance.get("preset") in {"chat", "maintenance"}:
            config = json.loads(Path(instance["config_file"]).read_text())
            with sqlite3.connect(
                "file:" + config["database"] + "?mode=ro", uri=True
            ) as connection:
                connection.row_factory = sqlite3.Row
                tables = (
                    ["messages", "turns", "turn_revisions", "raw_results"]
                    if instance["preset"] == "chat"
                    else ["jobs", "audit"]
                )
                exported["history"] = {
                    table: [
                        dict(row)
                        for row in connection.execute("SELECT * FROM " + table)
                    ]
                    for table in tables
                }
        return exported

    def close(self):
        if self.server:
            self.server.shutdown()
            self.server.server_close()
