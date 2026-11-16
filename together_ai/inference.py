"""Free-only inference with capability filtering and cancellable outage recovery."""

import asyncio
import copy
import inspect
import json
import math
import shlex
import time
from contextvars import ContextVar
from datetime import UTC
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx


class InferenceConfigurationError(ValueError):
    """A configuration problem requiring the owner to repair configuration."""

    category = "inference_configuration"
    retryable = False


def normalize_chat_response(response):
    """Assemble a buffered OpenAI-compatible SSE response into ordinary JSON."""
    is_stream = "text/event-stream" in response.headers.get("content-type", "").lower()
    if not response.is_success or not (
        is_stream or response.content.lstrip().startswith(b"data:")
    ):
        return response
    choices, metadata = {}, {}
    done = False

    def interrupted(reason):
        try:
            request = response.request
        except RuntimeError:
            request = None
        diagnostics = {
            "model": metadata.get("model"),
            "finish_reasons": [
                choice.get("finish_reason") for choice in choices.values()
            ],
            "usage": metadata.get("usage"),
            "visible_chars": sum(
                len(choice["message"].get("content") or "")
                for choice in choices.values()
            ),
            "tool_argument_chars": sum(
                len(tool["function"]["arguments"])
                for choice in choices.values()
                for tool in choice.get("tools", {}).values()
            ),
            "done": done,
        }
        usage = diagnostics["usage"] or {}
        counts = {
            key: usage[key]
            for key in ("prompt_tokens", "completion_tokens", "total_tokens")
            if isinstance(usage, dict) and key in usage
        }
        detail = {
            "model": diagnostics["model"],
            "finish_reasons": diagnostics["finish_reasons"],
            "tokens": counts,
            "visible_chars": diagnostics["visible_chars"],
            "tool_argument_chars": diagnostics["tool_argument_chars"],
        }
        error = httpx.ReadError(
            "Incomplete planning stream: "
            + reason
            + ". Diagnostics "
            + json.dumps(detail),
            request=request,
        )
        error.completion_diagnostics = diagnostics
        return error

    for event in response.text.replace("\r\n", "\n").replace("\r", "\n").split("\n\n"):
        data = "\n".join(
            line[5:].lstrip(" ")
            for line in event.split("\n")
            if line.startswith("data:")
        )
        if not data:
            continue
        if any(
            line.startswith("event:") and line[6:].strip() == "error"
            for line in event.split("\n")
        ):
            raise interrupted("the provider emitted an error event")
        if data.strip() == "[DONE]":
            done = True
            break
        try:
            chunk = json.loads(data)
        except (ValueError, TypeError) as error:
            raise interrupted("invalid SSE event JSON") from error
        if (
            not isinstance(chunk, dict)
            or chunk.get("error")
            or chunk.get("type") == "error"
        ):
            raise interrupted("the provider emitted an error event")
        if chunk.get("model") == "keepalive":
            continue
        for key in ("id", "model", "created", "system_fingerprint", "usage"):
            if chunk.get(key) is not None:
                metadata[key] = chunk[key]
        for item in chunk.get("choices", []):
            index = item.get("index", 0)
            choice = choices.setdefault(
                index,
                {
                    "index": index,
                    "message": {"role": "assistant", "content": ""},
                    "finish_reason": None,
                    "tools": {},
                },
            )
            delta = item.get("delta") or item.get("message") or {}
            if delta.get("role"):
                choice["message"]["role"] = delta["role"]
            content = delta.get("content")
            if isinstance(content, str):
                choice["message"]["content"] += content
            elif isinstance(content, list):
                choice["message"]["content"] += "".join(
                    part.get("text", "")
                    for part in content
                    if isinstance(part, dict) and isinstance(part.get("text"), str)
                )
            for fragment in delta.get("tool_calls") or []:
                tool = choice["tools"].setdefault(
                    fragment.get("index", 0),
                    {
                        "id": "",
                        "type": "function",
                        "function": {"name": "", "arguments": ""},
                    },
                )
                if fragment.get("id"):
                    tool["id"] += fragment["id"]
                function = fragment.get("function") or {}
                if function.get("name"):
                    tool["function"]["name"] += function["name"]
                arguments = function.get("arguments")
                if arguments is not None:
                    tool["function"]["arguments"] += (
                        arguments
                        if isinstance(arguments, str)
                        else json.dumps(arguments)
                    )
            if item.get("finish_reason") is not None:
                choice["finish_reason"] = item["finish_reason"]
    if not done:
        raise interrupted("the connection ended before [DONE]")
    if not choices:
        raise interrupted("the provider returned no completion choices")
    assembled = []
    for index in sorted(choices):
        choice = choices[index]
        if choice["finish_reason"] == "length":
            raise interrupted("the completion hit its output length limit")
        if choice["finish_reason"] in {None, "content_filter"}:
            raise interrupted("the completion did not finish or was filtered")
        tools = choice.pop("tools")
        if tools:
            choice["message"]["tool_calls"] = [tools[key] for key in sorted(tools)]
            if not choice["message"]["content"]:
                choice["message"]["content"] = None
        assembled.append(choice)
    payload = dict(metadata, object="chat.completion", choices=assembled)
    headers = {
        key: value
        for key, value in response.headers.items()
        if key.lower() not in {"content-type", "content-length", "content-encoding"}
    }
    try:
        request = response.request
    except RuntimeError:
        request = None
    return httpx.Response(
        response.status_code,
        json=payload,
        headers=headers,
        request=request,
        extensions=response.extensions,
    )


def _number(value, default, minimum=0.01):
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return result if math.isfinite(result) and result >= minimum else default


def _free(record):
    model = record.get("id", record.get("transport_model", ""))
    if not isinstance(model, str) or model == "auto/fast":
        return False
    if model in {"auto/best-free", "openrouter/free"} or model.endswith(":free"):
        return True
    if record.get("free") is True or record.get("is_free") is True:
        return True
    pricing = _object(record.get("pricing"))
    try:
        return all(
            key in pricing and float(pricing[key]) == 0
            for key in ("prompt", "completion")
        )
    except (TypeError, ValueError):
        return False


def _capability(record, kind):
    explicit = record.get("supports_" + kind)
    if isinstance(explicit, bool):
        return explicit
    capabilities = record.get("capabilities") or {}
    if isinstance(capabilities, dict):
        value = capabilities.get(
            kind, capabilities.get("vision" if kind == "images" else "tool_calling")
        )
        if isinstance(value, bool):
            return value
    if kind == "tools":
        return "tools" in _strings(record.get("supported_parameters"))
    architecture = _object(record.get("architecture"))
    modalities = _strings(
        architecture.get("input_modalities", record.get("input_modalities"))
    )
    modality = architecture.get("modality")
    return "image" in modalities or (
        isinstance(modality, str) and "image" in modality.split("->")[0].split("+")
    )


def _object(value):
    return value if isinstance(value, dict) else {}


def _strings(value):
    return (
        [item for item in value if isinstance(item, str)]
        if isinstance(value, list)
        else []
    )


def _quality_priority(record, images):
    """Task preferences from the captured free catalog, not a universal ranking.

    These IDs confer neither availability nor capabilities. They only order
    eligible catalog records: orchestration/coding for text, vision for images.
    Unknown models use advertised tools/reasoning rather than speed/name words.
    """
    text = (
        "nvidia/nemotron-3-ultra-550b-a55b:free",
        "cohere/north-mini-code:free",
        "particle-ai/particle-1:free",
        "recursive/flint-v1.0:free",
        "nvidia/nemotron-3.5-super-vl:free",
    )
    vision = (
        "nvidia/nemotron-3.5-super-vl:free",
        "particle-ai/particle-1:free",
        "thinkingmachines/inkling-2-small:free",
        "google/gemma-4-31b-it:free",
    )
    preferences = vision if images else text
    identity = record.get("root") or record["id"]
    if isinstance(identity, str):
        identity = identity.removeprefix("openrouter/")
    rank = preferences.index(identity) if identity in preferences else len(preferences)
    reasoning = _object(record.get("capabilities")).get("reasoning") is True or (
        "reasoning" in _strings(record.get("supported_parameters"))
    )
    return rank, not _capability(record, "tools"), not reasoning, record["id"]


class FreeInferenceRouter:
    """Borrow an AsyncClient; try free primary candidates before outage endpoints.

    Health and Retry-After deadlines persist across requests. Waiting is always
    cancellable, and no finite retry count turns a temporary outage into failure.
    """

    def __init__(self, config, http, catalog=None):
        self.config = config
        self.http = http
        self.catalog = catalog
        self.root = Path(config.get("target", ".")).expanduser()
        self.retry_seconds = _number(config.get("free_inference_retry_seconds"), 2)
        self.max_backoff = _number(config.get("free_inference_max_backoff_seconds"), 60)
        self.refresh_seconds = _number(
            config.get("free_inference_catalog_refresh_seconds"), 60
        )
        self._records = {}
        self._refresh_at = {}
        self._health = {}
        self._endpoint_ready = {}
        self._endpoint_health = {}
        self._transport_health = {}
        self._last = ContextVar("free_inference_last_candidate", default=None)
        self.last_route = None
        self._response_format_rejected = set()
        self._endpoints = [
            {
                "name": "omniroute",
                "base_url": config.get("base_url", ""),
                "api_key_file": config.get("api_key_file"),
                "primary": True,
            }
        ]
        configured = config.get("free_inference_endpoints", [])
        if not isinstance(configured, list) or any(
            not isinstance(item, dict) for item in configured
        ):
            raise InferenceConfigurationError(
                "free_inference_endpoints must be a list of endpoint objects"
            )
        configured = list(configured)
        legacy = config.get("planning_endpoint")
        if legacy:
            if not isinstance(legacy, dict):
                raise InferenceConfigurationError(
                    "planning_endpoint must be an endpoint object"
                )
            if not any(
                item.get("base_url") == legacy.get("base_url") for item in configured
            ):
                configured.append(
                    dict(legacy, name=legacy.get("name", "planning-outage"))
                )
        for index, endpoint in enumerate(configured):
            self._endpoints.append(
                dict(
                    endpoint,
                    name=endpoint.get("name", f"outage-{index + 1}"),
                    primary=False,
                )
            )
        for endpoint in self._endpoints:
            url = httpx.URL(endpoint.get("base_url", ""))
            local = url.host in {"localhost", "127.0.0.1", "::1"}
            if (
                not url.host
                or url.userinfo
                or url.query
                or url.fragment
                or (url.scheme != "https" and not (local and url.scheme == "http"))
            ):
                raise InferenceConfigurationError(
                    "Inference endpoints need HTTPS (or loopback HTTP) without URL credentials"
                )
            endpoint["base_url"] = str(url).rstrip("/")
        names = [endpoint["name"] for endpoint in self._endpoints]
        if len(names) != len(set(names)):
            raise InferenceConfigurationError("Inference endpoint names must be unique")

    def _headers(self, endpoint):
        """Read references only; never attach secrets to diagnostics or provenance."""
        try:
            if endpoint.get("credential_json_file"):
                path = self.root / Path(endpoint["credential_json_file"]).expanduser()
                key = json.loads(path.read_text())[endpoint["credential_json_field"]]
            elif endpoint.get("credential_env_file"):
                path = self.root / Path(endpoint["credential_env_file"]).expanduser()
                key = None
                for line in path.read_text().splitlines():
                    line = line.strip().removeprefix("export ").lstrip()
                    name, separator, value = line.partition("=")
                    if separator and name.strip() == endpoint.get(
                        "credential_env_name"
                    ):
                        parts = shlex.split(value, comments=True)
                        if len(parts) == 1:
                            key = parts[0]
            elif endpoint.get("api_key_file"):
                path = self.root / Path(endpoint["api_key_file"]).expanduser()
                key = path.read_text().strip()
            else:
                key = None
            if not isinstance(key, str) or not key.strip():
                raise ValueError("missing credential")
            return {"Authorization": "Bearer " + key.strip()}
        except (OSError, ValueError, TypeError, KeyError):
            raise InferenceConfigurationError(
                "Inference credential reference is missing or unreadable"
            ) from None

    async def _emit(self, progress, message):
        if progress:
            result = progress(message)
            if inspect.isawaitable(result):
                await result

    async def _refresh(self, endpoint):
        name = endpoint["name"]
        if time.monotonic() < self._refresh_at.get(name, 0):
            return
        self._refresh_at[name] = time.monotonic() + self.refresh_seconds
        try:
            async with asyncio.timeout(10):
                if endpoint["primary"] and self.catalog is not None:
                    records = self.catalog() if callable(self.catalog) else self.catalog
                    if inspect.isawaitable(records):
                        records = await records
                else:
                    # Explicitly configured hosts only; no provider discovery creates credentials/routes.
                    response = await self.http.get(
                        endpoint["base_url"] + "/models",
                        headers=self._headers(endpoint),
                    )
                    response.raise_for_status()
                    records = response.json()
            if isinstance(records, dict):
                records = records.get("data")
            if not isinstance(records, list):
                return
            usable = []
            for item in records:
                if (
                    not isinstance(item, dict)
                    or not isinstance(item.get("id"), str)
                    or not item["id"]
                ):
                    continue
                item = copy.deepcopy(item)
                for field in ("root", "transport_model", "canonical_model"):
                    if not isinstance(item.get(field), str) or not item[field]:
                        item.pop(field, None)
                if type(item.get("max_tokens")) is not int or item["max_tokens"] <= 0:
                    item.pop("max_tokens", None)
                usable.append(item)
            if usable:
                self._records[name] = usable
        except (httpx.HTTPError, ValueError, TypeError, TimeoutError):
            # A failed/malformed catalog must not erase the last known candidates.
            return

    def _candidates(self, body, selected_endpoint=None):
        images = any(
            isinstance(part, dict) and part.get("type") in {"image_url", "input_image"}
            for message in body.get("messages", [])
            for part in (
                message.get("content")
                if isinstance(message.get("content"), list)
                else []
            )
        )
        tools = bool(body.get("tools") or body.get("functions")) or any(
            message.get("role") in {"tool", "function"}
            or message.get("tool_calls")
            or message.get("function_call")
            for message in body.get("messages", [])
        )
        requested = body.get("model", "auto/best-free")
        endpoints = (
            [selected_endpoint] if selected_endpoint is not None else self._endpoints
        )
        for endpoint in endpoints:
            if endpoint.get("available") is False or endpoint.get("enabled") is False:
                continue
            records = sorted(
                self._records.get(endpoint["name"], []),
                key=lambda item: _quality_priority(item, images),
            )
            if endpoint["primary"]:
                chosen = next(
                    (
                        item
                        for item in records
                        if item["id"] == requested and _free(item)
                    ),
                    None,
                )
                # Preserve the owner default, but only with actual catalog metadata.
                dynamic = next(
                    (item for item in records if item["id"] == "auto/best-free"), None
                )
                ordered = (
                    ([chosen] if chosen else [])
                    + ([dynamic] if dynamic else [])
                    + records
                )
            else:
                model = endpoint.get("transport_model")
                known = next((item for item in records if item["id"] == model), {})
                configured = (
                    dict(known, **endpoint, id=model)
                    if isinstance(model, str)
                    else None
                )
                # Endpoint limits apply to every discovered model on that transport.
                limits = {
                    key: endpoint[key]
                    for key in (
                        "max_tokens",
                        "supports_response_format",
                        "min_interval_seconds",
                    )
                    if key in endpoint
                }
                ordered = ([configured] if configured else []) + [
                    dict(item, **limits) for item in records
                ]
            seen = set()
            for record in ordered:
                model = record.get("id", "")
                if model in seen or not _free(record):
                    continue
                seen.add(model)
                if record.get("available") is False or record.get("enabled") is False:
                    continue
                architecture = _object(record.get("architecture"))
                outputs = _strings(
                    architecture.get(
                        "output_modalities", record.get("output_modalities", ["text"])
                    )
                )
                if "text" not in outputs:
                    continue
                if record.get("status") in ("unavailable", "disabled", "offline"):
                    continue
                if images and (
                    endpoint.get("supports_images") is False
                    or not _capability(record, "images")
                ):
                    continue
                if tools and (
                    endpoint.get("supports_tools") is False
                    or not _capability(record, "tools")
                ):
                    continue
                canonical = model
                if not endpoint["primary"]:
                    canonical = record.get("canonical_model")
                    if not canonical:
                        primary_records = self._records.get("omniroute", [])
                        match = next(
                            (
                                item
                                for item in primary_records
                                if item.get("root") == model or item["id"] == model
                            ),
                            None,
                        )
                        canonical = match["id"] if match else model
                yield endpoint, record, canonical

    def _account_key(self, endpoint):
        """Aliases sharing a host and credential reference share quota/pacing."""
        url = httpx.URL(endpoint["base_url"])
        for field, selector in (
            ("credential_json_file", "credential_json_field"),
            ("credential_env_file", "credential_env_name"),
            ("api_key_file", None),
        ):
            if endpoint.get(field):
                path = (self.root / Path(endpoint[field]).expanduser()).resolve()
                return url.host, url.port, field, str(path), endpoint.get(selector)
        return url.host, url.port, None

    def _key(self, endpoint, record):
        return self._account_key(endpoint), record.get("transport_model") or record[
            "id"
        ]

    @staticmethod
    def _transport_key(endpoint):
        url = httpx.URL(endpoint["base_url"])
        return url.host, url.port

    def _ready_at(self, endpoint, record):
        account = self._account_key(endpoint)
        return max(
            self._health.get(self._key(endpoint, record), (0, 0))[0],
            self._endpoint_ready.get(account, 0),
            self._endpoint_health.get(account, (0, 0))[0],
            self._transport_health.get(self._transport_key(endpoint), (0, 0))[0],
        )

    @staticmethod
    def _transport_failure(error, response):
        # Gateway and upstream 503s can be ambiguous. Temporarily leave the host
        # instead of spending one timeout per model; retry it after cooldown.
        if response is not None:
            return response.status_code in {502, 503, 504}
        return isinstance(error, (httpx.TransportError, TimeoutError))

    @staticmethod
    def _account_failure(response):
        if response is None:
            return False
        if response.status_code in {401, 403}:
            return True
        if response.status_code != 429:
            return False
        try:
            payload = _object(response.json())
        except ValueError:
            payload = {}
        error = _object(payload.get("error"))
        scope = (
            error.get("scope")
            or payload.get("scope")
            or response.headers.get("x-ratelimit-scope")
        )
        if scope in ("model", "deployment"):
            return False
        if scope in (
            "account",
            "organization",
            "user",
            "endpoint",
            "host",
            "global",
            "api_key",
            "project",
        ):
            return True
        # Providers often expose scope only in the quota error's code/message.
        detail = str(payload.get("error", response.text)).lower()
        if any(
            word in detail
            for word in (
                "account",
                "organization",
                "api key",
                "api_key",
                "endpoint",
                "global",
                "daily quota",
                "insufficient_quota",
            )
        ):
            return True
        # An unscoped Retry-After is an instruction to pace this account. Only
        # explicitly model-scoped limits should immediately hit the host again.
        return bool(response.headers.get("Retry-After")) and "model" not in detail

    def _cooldown(self, key, response=None, *, health=None):
        health = self._health if health is None else health
        previous = health.get(key, (0, 0))
        failures = previous[1] + 1
        delay = min(self.max_backoff, self.retry_seconds * 2 ** min(failures - 1, 10))
        if response is not None and response.headers.get("Retry-After"):
            value = response.headers["Retry-After"]
            try:
                retry_after = float(value)
            except ValueError:
                try:
                    date = parsedate_to_datetime(value)
                    if date.tzinfo is None:
                        date = date.replace(tzinfo=UTC)
                    retry_after = date.timestamp() - time.time()
                except (ValueError, TypeError, OverflowError):
                    retry_after = 0
            if math.isfinite(retry_after):
                delay = max(delay, retry_after)
        health[key] = max(previous[0], time.monotonic() + delay), failures

    def reject_last_response(self, reason="invalid planning action"):
        """Call after application-level validation fails; the next call rotates."""
        key = self._last.get()
        if key:
            self._cooldown(key)

    async def _discover_candidates(self, body):
        # Advancing this iterator reaches outage catalogs only after primary
        # inference has failed or has no eligible ready candidate.
        for endpoint in self._endpoints:
            account = self._account_key(endpoint)
            if (
                max(
                    self._endpoint_health.get(account, (0, 0))[0],
                    self._endpoint_ready.get(account, 0),
                    self._transport_health.get(self._transport_key(endpoint), (0, 0))[
                        0
                    ],
                )
                <= time.monotonic()
            ):
                await self._refresh(endpoint)
            for candidate in self._candidates(body, endpoint):
                yield candidate

    @staticmethod
    def _validate(response):
        payload = response.json()
        if not isinstance(payload, dict) or payload.get("error"):
            raise ValueError("invalid completion envelope")
        choices = payload.get("choices")
        if not isinstance(choices, list) or not choices:
            raise ValueError("missing completion")
        for choice in choices:
            if choice.get("finish_reason") not in {
                "stop",
                "tool_calls",
                "function_call",
            }:
                raise ValueError("incomplete completion")
            message = choice.get("message") or {}
            calls = message.get("tool_calls") or []
            if (
                not message.get("content")
                and not calls
                and not message.get("function_call")
            ):
                raise ValueError("empty completion")
            for call in calls:
                function = call["function"]
                if (
                    not function.get("name")
                    or not call.get("id")
                    or not isinstance(json.loads(function["arguments"]), dict)
                ):
                    raise ValueError("invalid tool call")
        return payload

    async def post(self, body, progress=None):
        original = copy.deepcopy(body)
        while True:
            candidates = []
            async for endpoint, record, canonical in self._discover_candidates(
                original
            ):
                candidates.append((endpoint, record, canonical))
                key = self._key(endpoint, record)
                ready_at = self._ready_at(endpoint, record)
                if ready_at > time.monotonic():
                    continue
                account = self._account_key(endpoint)
                interval = max(
                    _number(alias.get("min_interval_seconds"), 0)
                    for alias in self._endpoints
                    if self._account_key(alias) == account
                )
                if interval:
                    self._endpoint_ready[account] = time.monotonic() + interval
                outgoing = copy.deepcopy(original)
                transport_model = record.get("transport_model") or record["id"]
                outgoing["model"] = transport_model
                if record.get("max_tokens"):
                    outgoing["max_tokens"] = min(
                        outgoing.get("max_tokens", record["max_tokens"]),
                        record["max_tokens"],
                    )
                if (
                    record.get("supports_response_format") is False
                    or key in self._response_format_rejected
                ):
                    outgoing.pop("response_format", None)
                options = {}
                if hasattr(self.http, "positive_timeout"):
                    options = {
                        "planning_progress": progress,
                        "planning_first_output_timeout": self.config.get(
                            "planning_first_output_timeout", 90
                        ),
                        "planning_turn_timeout": self.config.get(
                            "planning_turn_timeout", 240
                        ),
                    }
                else:
                    outgoing["stream"] = False
                await self._emit(
                    progress,
                    f"Requesting free inference: {endpoint['name']} · {canonical} (transport {transport_model}).",
                )
                response = None
                try:
                    headers = self._headers(endpoint)
                    async with asyncio.timeout(
                        _number(self.config.get("planning_turn_timeout"), 240)
                    ):
                        response = await self.http.post(
                            endpoint["base_url"] + "/chat/completions",
                            json=outgoing,
                            headers=headers,
                            **options,
                        )
                    if not response.is_success:
                        if (
                            response.status_code in {400, 422}
                            and "response_format" in response.text.lower()
                        ):
                            self._response_format_rejected.add(key)
                        raise ValueError(f"HTTP {response.status_code}")
                    response = normalize_chat_response(response)
                    payload = self._validate(response)
                except InferenceConfigurationError:
                    raise
                except (
                    httpx.HTTPError,
                    TimeoutError,
                    ValueError,
                    TypeError,
                    KeyError,
                    IndexError,
                    AttributeError,
                ) as error:
                    self._cooldown(key, response)
                    if self._transport_failure(error, response):
                        self._cooldown(
                            self._transport_key(endpoint),
                            response,
                            health=self._transport_health,
                        )
                    if self._account_failure(response):
                        self._cooldown(account, response, health=self._endpoint_health)
                    failure = (
                        f"HTTP {response.status_code}"
                        if response is not None and not response.is_success
                        else type(error).__name__
                    )
                    await self._emit(
                        progress,
                        f"Free route unavailable: {endpoint['name']} · {canonical} ({failure}); trying eligible alternatives with request state preserved.",
                    )
                    continue
                actual = response.headers.get("x-omniroute-model") or payload.get(
                    "model"
                )
                provenance = {
                    "endpoint": endpoint["name"],
                    "canonical_model": canonical,
                    "transport_model": transport_model,
                    "actual_model": actual,
                    "base_url": endpoint["base_url"],
                }
                self._last.set(key)
                self.last_route = provenance
                self._health.pop(key, None)
                self._transport_health.pop(self._transport_key(endpoint), None)
                response.extensions["inference_route"] = provenance
                await self._emit(
                    progress,
                    f"Received complete free inference: {endpoint['name']} · {canonical} · provider-reported model {actual or 'not supplied'}.",
                )
                return response
            now = time.monotonic()
            deadlines = [
                max(
                    self._ready_at(endpoint, record),
                    now + 0.01,
                )
                for endpoint, record, _ in candidates
            ]
            delay = min(
                self.max_backoff,
                max(0.01, min(deadlines, default=now + self.retry_seconds) - now),
            )
            await self._emit(
                progress,
                f"Eligible free providers unavailable; checkpoint retained. Resuming automatically in {delay:.1f}s.",
            )
            await asyncio.sleep(delay)
