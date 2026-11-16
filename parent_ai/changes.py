import asyncio
import difflib
import hashlib
import json
import os
import sys
from pathlib import Path

import httpx

PRIVATE_FIELDS = {"discord_token_file", "api_key_file", "database"}
CONFIG_FIELDS = {
    "default_model",
    "aliases",
    "participation",
    "media_defaults",
    "base_url",
    "connectors",
    "skills",
    "excluded_channels",
    "routing_snapshot_file",
    "guild_id",
    "persona_file",
}


class StaleProposal(ValueError):
    pass


class Changes:
    def __init__(self, config):
        self.config = config
        self.root = Path(config["target"]).resolve()
        self.http = httpx.AsyncClient(timeout=240)

    def snapshot(self):
        files = {}
        for directory, subdirs, names in os.walk(self.root):
            subdirs[:] = [
                name
                for name in subdirs
                if not name.startswith(".")
                and name not in {"parent_ai", "__pycache__", "node_modules"}
                and not (Path(directory) / name).is_symlink()
            ]
            for name in names:
                path = Path(directory) / name
                if (
                    not self.editable(str(path.relative_to(self.root)))
                    or path.is_symlink()
                ):
                    continue
                try:
                    files[str(path.relative_to(self.root))] = path.read_text()
                except UnicodeError:
                    continue
        config = json.loads((self.root / "config.json").read_text())
        return {
            "files": files,
            "config": {k: v for k, v in config.items() if k not in PRIVATE_FIELDS},
        }

    def editable(self, name):
        path = Path(name)
        if (
            path.is_absolute()
            or ".." in path.parts
            or not path.parts
            or not (self.root / path).resolve().is_relative_to(self.root)
        ):
            return False
        if any(
            part.startswith(".") or part in {"parent_ai", "__pycache__", "node_modules"}
            for part in path.parts
        ):
            return False
        if name in {
            "config.json",
            "parent.config.example.json",
            "skill-issue-department.service",
        }:
            return False
        if any(
            word in name.lower()
            for word in ("token", "secret", "credential", ".sqlite")
        ):
            return False
        return (
            path.suffix
            in {
                ".py",
                ".md",
                ".json",
                ".toml",
                ".lock",
                ".service",
                ".yml",
                ".yaml",
                ".txt",
            }
            or name == "together-ai"
        )

    @staticmethod
    def digest(snapshot):
        return hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest()

    async def hook(self, prompt, session):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            self.config["energy_hook"],
            "--host",
            "codex",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self.root,
        )
        payload = {
            "hook_event_name": "UserPromptSubmit",
            "prompt": prompt,
            "session_id": session,
            "cwd": str(self.root),
        }
        try:
            out, _ = await asyncio.wait_for(
                process.communicate(json.dumps(payload).encode()), 30
            )
        except asyncio.TimeoutError:
            process.kill()
            await process.communicate()
            raise RuntimeError(
                "The protecting-your-energy hook timed out. Please retry."
            )
        if process.returncode:
            raise RuntimeError("The protecting-your-energy hook failed. Please retry.")
        result = json.loads(out)
        context = result.get("hookSpecificOutput", {}).get("additionalContext", "")
        if not context or "could not run:" in context.lower():
            raise RuntimeError(
                "The protecting-your-energy hook could not complete. Please retry."
            )
        return context

    async def propose(self, request, job_id, progress=None):
        async def emit(message):
            if progress:
                await progress(message)

        await emit("Reading Together AI source and current allowed settings.")
        base = self.snapshot()
        await emit(
            f"Read {len(base['files'])} source files. Running the protecting-your-energy prompt hook."
        )
        guidance = await self.hook(request, f"skill-issue-{job_id}")
        await emit("Hook completed. Starting a tool-using planning session.")
        system = """You are Skill Issue Department, the parent maintenance agent for Together AI.
Read and search the source using the supplied tools. Determine the user's intended change and inspect the relevant implementation before proposing it.
Use stage_edit to make precise replacements in a draft. Call finish_change with summary/questions/config when ready. Its staged edits become the proposal. You can also return complete JSON with summary/questions/files/config. Inspect only relevant code, then implement. Avoid repeatedly reading the same source.
Files maps relative filenames to COMPLETE replacement contents. Config contains only changed allowed settings.
If clarification is necessary, return questions and empty files/config. Do not claim changes have been applied.
Changes require explicit requester/admin approval. Keep existing behavior except what was requested.
You can edit all listed child project files, including source, documentation, dependency manifests and the child service unit. All visible child settings can be modified, including gateway URL, model routes, aliases, connectors, skills and persona paths. Credential values stay in their existing private files.
Parent runtime files and credential contents are outside this interface.
Do not request secrets or add credential access, arbitrary shell execution, or external data export.
Keep the child READY log and management channel exclusion intact. Dependency changes can be proposed through pyproject.toml.
Use the most recent follow-up as the user's current intent. Be concise and lightly memeish.
"""
        c = json.loads((self.root / "config.json").read_text())

        def tool(name, description, properties, required):
            return {
                "type": "function",
                "function": {
                    "name": name,
                    "description": description,
                    "parameters": {
                        "type": "object",
                        "properties": properties,
                        "required": required,
                        "additionalProperties": False,
                    },
                },
            }

        tools = [
            tool(
                "list_files", "List editable source files and settings names.", {}, []
            ),
            tool(
                "read_file",
                "Read a source file from the request snapshot.",
                {
                    "path": {"type": "string"},
                    "offset": {"type": "integer", "minimum": 1},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500},
                },
                ["path"],
            ),
            tool(
                "search_source",
                "Find text in source files, case insensitive.",
                {"query": {"type": "string"}},
                ["query"],
            ),
            tool(
                "read_settings",
                "Read editable settings from the request snapshot.",
                {},
                [],
            ),
            tool(
                "validate_change",
                "Validate a complete proposal including syntax. Does not install it.",
                {"proposal": {"type": "object"}},
                ["proposal"],
            ),
        ]
        tools.extend(
            [
                tool(
                    "stage_edit",
                    "Stage one exact text replacement in a child file. Use empty old_text for a new file.",
                    {
                        "path": {"type": "string"},
                        "old_text": {"type": "string"},
                        "new_text": {"type": "string"},
                    },
                    ["path", "old_text", "new_text"],
                ),
                tool(
                    "finish_change",
                    "Finish and validate staged edits for approval, or ask clarifying questions.",
                    {
                        "summary": {"type": "string"},
                        "questions": {"type": "array", "items": {"type": "string"}},
                        "config": {"type": "object"},
                    },
                    ["summary", "questions", "config"],
                ),
            ]
        )
        staged = {}
        seen_calls = {}
        force_final = False
        invalid_proposals = 0
        messages = [
            {
                "role": "system",
                "content": system + "\nProtecting-your-energy guidance\n" + guidance,
            },
            {"role": "user", "content": request},
        ]
        headers = {
            "Authorization": "Bearer " + Path(c["api_key_file"]).read_text().strip()
        }
        turn = 0
        while True:
            turn += 1
            await emit(
                f"Model turn {turn}: requesting the next action from {self.config.get('model', 'auto/best-free')}."
            )
            response = await self.http.post(
                c["base_url"] + "/chat/completions",
                headers=headers,
                json={
                    "model": self.config.get("model", "auto/best-free"),
                    "max_tokens": 24000,
                    "messages": messages,
                    "tools": tools,
                    "tool_choice": "none" if force_final else "auto",
                },
            )
            if not response.is_success:
                raise RuntimeError(
                    f"Planning route returned HTTP {response.status_code}. Please retry."
                )
            message = response.json()["choices"][0]["message"]
            content = message.get("content") or ""
            calls = message.get("tool_calls") or []
            if content:
                await emit(f"Model response:\n{content}")
            if not calls:
                if content.startswith("```"):
                    content = content.split("\n", 1)[1].rsplit("```", 1)[0]
                try:
                    plan = json.loads(content)
                    plan["files"] = staged | plan.get("files", {})
                    self.validate(plan)
                except (ValueError, TypeError, KeyError, SyntaxError) as exc:
                    invalid_proposals += 1
                    if invalid_proposals >= 3:
                        raise RuntimeError(
                            "The configured planning route returned three invalid proposals. Its responses are visible above. Retry planning or choose another parent route."
                        ) from exc
                    messages.append({"role": "assistant", "content": content})
                    messages.append(
                        {
                            "role": "user",
                            "content": f"The proposal could not be validated: {exc}. Fix it and return the complete JSON object.",
                        }
                    )
                    await emit(
                        f"Proposal validation failed: {exc}. Asking the model to correct it."
                    )
                    continue
                await emit(
                    f"Final validation passed: {len(plan['files'])} file(s), {len(plan['config'])} setting(s), {len(plan['questions'])} question(s)."
                )
                break
            messages.append(
                {
                    k: v
                    for k, v in message.items()
                    if k in {"role", "content", "tool_calls", "reasoning_details"}
                }
            )
            for call in calls:
                name = call["function"]["name"]
                raw = call["function"].get("arguments", "{}")
                await emit(f"Tool call **{name}**\n```json\n{raw}\n```")
                try:
                    args = json.loads(raw)
                    fingerprint = name + json.dumps(args, sort_keys=True)
                    seen_calls[fingerprint] = seen_calls.get(fingerprint, 0) + 1
                    if seen_calls[fingerprint] > 1 and name not in {
                        "stage_edit",
                        "finish_change",
                        "validate_change",
                    }:
                        force_final = True
                        raise ValueError(
                            "This exact inspection already ran. Use the earlier result and finish the proposal now. Return complete JSON on the next turn."
                        )
                    if name == "stage_edit":
                        path = args["path"]
                        if not self.editable(path):
                            raise ValueError(
                                "Choose a child project file from list_files."
                            )
                        original = staged.get(path, base["files"].get(path, ""))
                        old = args["old_text"]
                        if old and original.count(old) != 1:
                            raise ValueError(
                                "old_text must match exactly once. Read the relevant lines and retry."
                            )
                        if not old and original:
                            raise ValueError("Use old_text for existing files.")
                        staged[path] = (
                            original.replace(old, args["new_text"], 1)
                            if old
                            else args["new_text"]
                        )
                        result = {
                            "staged": path,
                            "diff": self.diff(
                                {"base": base, "plan": {"files": staged, "config": {}}}
                            ),
                        }
                    elif name == "finish_change":
                        plan = {
                            "summary": args["summary"],
                            "questions": args["questions"],
                            "files": staged,
                            "config": args["config"],
                        }
                        self.validate(plan)
                        await emit(
                            "Final proposal validated. Preparing the approval controls and diff."
                        )
                        return {"base": base, "plan": plan, "hook_applied": True}
                    elif name == "list_files":
                        result = {
                            "files": list(base["files"]),
                            "settings": sorted(CONFIG_FIELDS),
                        }
                    elif name == "read_file":
                        lines = staged.get(
                            args["path"], base["files"].get(args["path"], "")
                        ).splitlines(keepends=True)
                        offset = max(1, int(args.get("offset", 1)))
                        limit = min(500, max(1, int(args.get("limit", 250))))
                        result = {
                            "path": args["path"],
                            "offset": offset,
                            "total_lines": len(lines),
                            "content": "".join(lines[offset - 1 : offset - 1 + limit]),
                        }
                    elif name == "read_settings":
                        result = base["config"]
                    elif name == "search_source":
                        query = args["query"]
                        if not isinstance(query, str) or not query.strip():
                            raise ValueError("A nonempty query is required.")
                        result = [
                            {"path": path, "line": index, "text": line}
                            for path, text in base["files"].items()
                            for index, line in enumerate(text.splitlines(), 1)
                            if query.casefold() in line.casefold()
                        ][:100]
                    elif name == "validate_change":
                        self.validate(args["proposal"])
                        result = {
                            "valid": True,
                            "diff": self.diff({"base": base, "plan": args["proposal"]}),
                        }
                    else:
                        raise ValueError("Unknown tool.")
                    output = json.dumps(result, ensure_ascii=False)
                except (
                    ValueError,
                    KeyError,
                    TypeError,
                    SyntaxError,
                    json.JSONDecodeError,
                ) as exc:
                    output = json.dumps({"error": str(exc)})
                await emit(f"Tool result **{name}**\n```json\n{output}\n```")
                messages.append(
                    {"role": "tool", "tool_call_id": call["id"], "content": output}
                )
        return {"base": base, "plan": plan, "hook_applied": True}

    def validate(self, plan):
        if not isinstance(plan.get("summary"), str) or not isinstance(
            plan.get("questions"), list
        ):
            raise ValueError("The model did not return a complete proposal.")
        if not isinstance(plan.get("files"), dict) or not isinstance(
            plan.get("config"), dict
        ):
            raise ValueError("The model did not return a complete change set.")
        if plan["questions"] and (plan["files"] or plan["config"]):
            raise ValueError(
                "Clarifications must be resolved before proposing changes."
            )
        for name, content in plan["files"].items():
            p = Path(name)
            if not self.editable(name):
                raise ValueError(
                    "Choose a child project file; parent runtime files are separate."
                )
            if not isinstance(content, str) or (self.root / p).is_symlink():
                raise ValueError("Invalid source file in proposal.")
            if p.suffix == ".py":
                compile(content, name, "exec")
        if set(plan["config"]) & PRIVATE_FIELDS:
            raise ValueError("The proposal tried to change protected configuration.")
        for key, value in plan["config"].items():
            if key == "default_model":
                if not isinstance(value, str) or not value.strip():
                    raise ValueError("The default model must be a route name.")
            elif key in {
                "aliases",
                "participation",
                "media_defaults",
                "skills",
            } and not isinstance(value, dict):
                raise ValueError(f"{key} must be a settings object.")
        participation = plan["config"].get("participation", {})
        if "level" in participation and participation["level"] not in {
            "off",
            "quiet",
            "balanced",
            "lively",
        }:
            raise ValueError("Unknown participation level.")
        for key, low, high in [("delay", 2, 120), ("cooldown", 0, 3600)]:
            if key in participation and (
                type(participation[key]) is not int
                or not low <= participation[key] <= high
            ):
                raise ValueError(f"Invalid participation {key}.")

    def diff(self, proposal):
        old = dict(proposal["base"]["files"])
        new = dict(proposal["plan"]["files"])
        if proposal["plan"]["config"]:
            old["settings.json"] = (
                json.dumps(proposal["base"]["config"], indent=2) + "\n"
            )
            new["settings.json"] = (
                json.dumps(
                    proposal["base"]["config"] | proposal["plan"]["config"], indent=2
                )
                + "\n"
            )
        return "\n".join(
            "".join(
                difflib.unified_diff(
                    old.get(name, "").splitlines(True),
                    value.splitlines(True),
                    fromfile=name,
                    tofile=name,
                )
            )
            for name, value in new.items()
        )

    def write(self, name, text):
        path = self.root / name
        temp = path.with_name(path.name + ".parent-tmp")
        path.parent.mkdir(parents=True, exist_ok=True)
        temp.write_text(text)
        temp.chmod(0o600 if name == "config.json" else 0o644)
        temp.replace(path)

    def install(self, proposal):
        self.validate(proposal["plan"])
        if self.digest(self.snapshot()) != self.digest(proposal["base"]):
            raise StaleProposal(
                "Together AI changed since this preview. Edit or retry to review a fresh proposal."
            )
        for name, content in proposal["plan"]["files"].items():
            self.write(name, content)
        c = json.loads((self.root / "config.json").read_text())
        c.update(proposal["plan"]["config"])
        self.write("config.json", json.dumps(c, indent=2) + "\n")

    def restore(self, proposal):
        for name in proposal["plan"]["files"]:
            if name in proposal["base"]["files"]:
                self.write(name, proposal["base"]["files"][name])
            else:
                (self.root / name).unlink(missing_ok=True)
        c = json.loads((self.root / "config.json").read_text())
        for key in proposal["plan"]["config"]:
            if key in proposal["base"]["config"]:
                c[key] = proposal["base"]["config"][key]
            else:
                c.pop(key, None)
        self.write("config.json", json.dumps(c, indent=2) + "\n")

    async def command(self, *args):
        process = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        out, _ = await asyncio.wait_for(process.communicate(), 45)
        if process.returncode:
            raise RuntimeError("The child service operation failed.")
        return out.decode()

    async def restart(self):
        await self.command("uv", "sync", "--project", str(self.root))
        await self.command("systemctl", "--user", "daemon-reload")
        await self.command("systemctl", "--user", "restart", "together-ai")
        invocation = (
            await self.command(
                "systemctl",
                "--user",
                "show",
                "together-ai",
                "-p",
                "InvocationID",
                "--value",
            )
        ).strip()
        for _ in range(30):
            await asyncio.sleep(2)
            log = await self.command(
                "journalctl",
                "--user",
                f"_SYSTEMD_INVOCATION_ID={invocation}",
                "--no-pager",
                "-o",
                "cat",
            )
            if "READY bot=" in log:
                return
        raise RuntimeError("Together AI did not reconnect within 60 seconds.")
