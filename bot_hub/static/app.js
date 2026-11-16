"use strict";
const $ = (selector) => document.querySelector(selector);
const escapeHTML = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (char) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        char
      ],
  );
const safeText = (value) =>
  typeof value === "object"
    ? JSON.stringify(
        value,
        (key, item) =>
          /^(?:.*(?:secret|password|authorization|api[_-]?key).*|(?:access_|refresh_|discord_)?token|token_file)$/i.test(
            key,
          )
            ? "[private]"
            : item,
        2,
      )
    : String(value ?? "");
const text = (value) => escapeHTML(safeText(value));
const rows = (value) => (Array.isArray(value) ? value : []);
let state = {
  instances: [],
  presets: [],
  events: [],
  predictions: [],
  feedback: [],
  models: [],
  jobs: [],
  status: {},
};
let selected = sessionStorage.getItem("bot-hub-instance") || "";
let activeTab = "overview";
let loading = false;
let dirty = false;
let configInstance = "";
let signature = "";
let advancedDirty = false;
let reviewDirty = false;
const expandedConversations = new Set();
const expandedEvents = new Set();
const cleanupStates = new Map();
const cleanupTimers = new Map();
const empty = (message) => `<div class="empty">${escapeHTML(message)}</div>`;
const time = (date) => {
  const parsed = new Date(date);
  return Number.isNaN(parsed.getTime())
    ? "Time unavailable"
    : parsed.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
};
const dateTime = (date) => {
  const parsed = new Date(date);
  return Number.isNaN(parsed.getTime())
    ? "Time unavailable"
    : parsed.toLocaleString();
};
const current = () => state.instances.find((item) => item.id === selected);
const scoped = (list) =>
  rows(list).filter(
    (item) => item.instance_id === selected || item.instance === selected,
  );
const newest = (list) =>
  [...list].sort(
    (a, b) => new Date(b.created_at || 0) - new Date(a.created_at || 0),
  );
const payload = (item) =>
  item.payload && typeof item.payload === "object" ? item.payload : item;
const statusFor = (id) => state.status?.[id] || {};
function announce(message, error = false) {
  const notice = $("#notice");
  notice.textContent = message;
  notice.hidden = !message;
  notice.style.borderColor = error ? "#79514b" : "";
}
async function api(path, body) {
  const options = {
    credentials: "same-origin",
    headers: { Accept: "application/json" },
  };
  if (body !== undefined) {
    options.method = "POST";
    options.headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(body);
  }
  const response = await fetch(path, options);
  let data;
  try {
    data = await response.json();
  } catch {
    data = {};
  }
  if (!response.ok) {
    const error = new Error(
      response.status === 401
        ? "Your session needs authentication. Reopen the private dashboard link from the hub."
        : data.error || data.detail || `Request failed (${response.status}).`,
    );
    error.details = data;
    throw error;
  }
  return data;
}
function isRunning(instance) {
  const status = statusFor(instance.id);
  return (
    status.running === true ||
    status.connected === true ||
    ["running", "online", "connected"].includes(status.state || status.status)
  );
}
function modelId(model) {
  return typeof model === "string" ? model : model.id || model.name || "";
}
function presetId(preset) {
  return typeof preset === "string" ? preset : preset.id || preset.name || "";
}
function options() {
  const presets = state.presets;
  const models = state.models;
  for (const select of document.querySelectorAll('select[name="preset"]')) {
    const previous = select.value;
    select.innerHTML = presets.length
      ? presets
          .map(
            (item) =>
              `<option value="${escapeHTML(presetId(item))}">${escapeHTML(item.label || item.name || presetId(item))}</option>`,
          )
          .join("")
      : '<option value="">No presets available</option>';
    if ([...select.options].some((item) => item.value === previous))
      select.value = previous;
  }
  for (const select of document.querySelectorAll('select[name="model"]')) {
    const previous = select.value;
    select.innerHTML =
      '<option value="">Use preset default</option>' +
      models
        .map(
          (item) =>
            `<option value="${escapeHTML(modelId(item))}">${escapeHTML(modelId(item))}</option>`,
        )
        .join("");
    if ([...select.options].some((item) => item.value === previous))
      select.value = previous;
  }
}
function updateIdentityControls(form, usePredictorDefaults = false) {
  const predictor = form.elements.preset.value === "predictor";
  const identity = form.elements.identity_instance;
  const previous = identity.value;
  const ownId =
    form.getAttribute("id") === "config-form"
      ? selected
      : form.elements.id.value.trim();
  const candidates = state.instances.filter(
    (item) =>
      item.id !== ownId &&
      item.preset === "chat" &&
      item.enabled &&
      !item.identity_instance,
  );
  identity.innerHTML =
    '<option value="">Dedicated bot identity</option>' +
    candidates
      .map(
        (item) =>
          `<option value="${escapeHTML(item.id)}">${escapeHTML(item.name || item.id)} · ${escapeHTML(item.id)}</option>`,
      )
      .join("");
  identity.value = candidates.some((item) => item.id === previous)
    ? previous
    : "";
  if (predictor && usePredictorDefaults) {
    identity.value =
      candidates.find((item) => item.id === "together")?.id ||
      candidates.find((item) => item.preset === "chat")?.id ||
      "";
    form.elements.thread_scope.value = identity.value
      ? "conversations"
      : "channels";
  }
  form.querySelector(".identity-settings").hidden = !predictor;
  identity.disabled = !predictor;
  form.elements.thread_scope.disabled = !predictor;
  const shared = predictor && !!identity.value;
  const owner = state.instances.find((item) => item.id === identity.value);
  const conversations = form.elements.thread_scope.value === "conversations";
  form.querySelector(".identity-note").textContent = shared
    ? `Uses ${owner?.name || identity.value}’s Discord identity${conversations ? " across all its conversation threads." : " within the configured channels."}`
    : "Uses a dedicated Discord bot identity with its own private token file.";
  const channelInput = form.elements.channels;
  channelInput.closest("label").hidden = predictor && conversations;
  if (form.elements.token_file) {
    form.elements.token_file.required = !shared;
    form.elements.token_file.disabled = shared;
    form.elements.token_file.closest("label").hidden = shared;
  }
}
function renderNavigation() {
  $("#instances").innerHTML = state.instances.length
    ? state.instances
        .map(
          (item) =>
            `<button class="instance-link ${item.id === selected ? "selected" : ""}" data-instance="${escapeHTML(item.id)}" ${item.id === selected ? 'aria-current="page"' : ""}><span class="instance-avatar">${escapeHTML((item.name || item.id || "?").slice(0, 1).toUpperCase())}</span><span class="instance-label">${escapeHTML(item.name || item.id)}<small>${escapeHTML(item.preset || "Custom instance")}</small></span><span class="status-dot ${isRunning(item) ? "online" : ""}"></span></button>`,
        )
        .join("")
    : empty("No instances yet. Create one to get started.");
}
function renderHeader(instance) {
  $("#instance-name").textContent =
    instance?.name || instance?.id || "Your bot workspace";
  $("#breadcrumb").textContent = instance?.name || instance?.id || "Overview";
  const preset = state.presets.find(
    (item) => presetId(item) === instance?.preset,
  );
  $("#instance-description").textContent =
    preset?.description ||
    "Configuration, conversations, and model behavior in one place.";
  const badge = $("#runtime-badge");
  badge.textContent = instance
    ? isRunning(instance)
      ? "● Running"
      : instance.enabled
        ? "Enabled · offline"
        : "Disabled"
    : "No instance";
  badge.className = `badge ${instance && isRunning(instance) ? "good" : ""}`;
  $("#export").hidden = !instance;
  if (instance)
    $("#export").href = `/api/export/${encodeURIComponent(instance.id)}`;
}
function drawChart(events) {
  const hours = Array(24).fill(0);
  const now = Date.now();
  for (const event of events) {
    const age = Math.floor(
      (now - new Date(event.created_at).getTime()) / 3600000,
    );
    if (age >= 0 && age < 24) hours[23 - age]++;
  }
  const max = Math.max(1, ...hours);
  const total = hours.reduce((sum, n) => sum + n, 0);
  const bars = hours
    .map(
      (n, i) =>
        `<rect x="${38 + i * 20.5}" y="${145 - (n / max) * 115}" width="11" height="${(n / max) * 115}" rx="2" fill="#b9e587"><title>${23 - i} hours ago · ${n} events</title></rect>`,
    )
    .join("");
  $("#chart").innerHTML =
    `<svg viewBox="0 0 540 180" role="img" aria-label="${total} recorded events in the last 24 hours"><line x1="32" y1="30" x2="530" y2="30" stroke="#2b2e36" stroke-dasharray="3 4"/><line x1="32" y1="87" x2="530" y2="87" stroke="#2b2e36" stroke-dasharray="3 4"/><line x1="32" y1="145" x2="530" y2="145" stroke="#343945"/><text x="0" y="33">${max}</text><text x="0" y="148">0</text>${bars}<text x="32" y="171">24h ago</text><text x="270" y="171">12h ago</text><text x="505" y="171">Now</text>${total ? "" : '<text x="170" y="85">Waiting for recorded activity</text>'}</svg>`;
  $("#chart-total").textContent = `${total.toLocaleString()} events`;
}
function eventMarkup(event) {
  const data = payload(event);
  const kind = event.kind || event.type || "event";
  const eventKey = `${event.instance_id || selected}:${event.id || `${event.created_at}:${kind}`}`;
  const summary =
    data.summary ||
    data.message ||
    data.content ||
    data.error ||
    data.tool ||
    data.name ||
    data.model ||
    "";
  return `<div class="event"><span class="event-icon">${/error|fail/i.test(kind) ? "!" : /tool/i.test(kind) ? "⌘" : "↗"}</span><div class="event-body"><div class="event-title"><span>${escapeHTML(kind.replaceAll("_", " "))}</span><time title="${escapeHTML(dateTime(event.created_at))}">${escapeHTML(time(event.created_at))}</time></div>${summary ? `<p>${text(safeText(summary).slice(0, 350))}</p>` : ""}<details class="event-details" data-event-key="${escapeHTML(eventKey)}" ${expandedEvents.has(eventKey) ? "open" : ""}><summary>Full event payload</summary><pre>${text(data)}</pre></details></div></div>`;
}
function renderOverview(instance) {
  const events = scoped(state.events);
  const predictions = scoped(state.predictions);
  const feedback = state.feedback.filter((entry) =>
    predictions.some((item) => String(item.id) === String(entry.prediction_id)),
  );
  const calls = events.filter((item) =>
    ["turn.completed", "model.completed"].includes(item.kind),
  );
  const failures = events.filter((item) =>
    /(?:failed|error)(?:$|[._])/i.test(item.kind || ""),
  );
  const tokenValues = calls
    .map((item) => {
      const usage = payload(item).usage || {};
      if (Number.isFinite(usage.total_tokens)) return usage.total_tokens;
      if (
        Number.isFinite(usage.prompt_tokens) ||
        Number.isFinite(usage.completion_tokens)
      )
        return (usage.prompt_tokens || 0) + (usage.completion_tokens || 0);
      return null;
    })
    .filter((value) => value !== null);
  const latencies = calls
    .map((item) => payload(item).latency_ms)
    .filter((value) => Number.isFinite(value));
  const tools = events.filter((item) => /tool/i.test(item.kind || ""));
  const scores = predictions
    .map(
      (item) =>
        payload(item).semantic_match_score ??
        payload(item).semantic_score ??
        payload(item).match_score ??
        payload(item).score ??
        payload(item).comparison?.score,
    )
    .filter((item) => typeof item === "number" && Number.isFinite(item));
  const metrics = [
    [
      "Recorded events",
      events.length.toLocaleString(),
      "Loaded event window",
      "↗",
    ],
    [
      "Completed model calls",
      calls.length.toLocaleString(),
      "turn.completed + model.completed · loaded window",
      "⌘",
    ],
    [
      "Predictions",
      predictions.length.toLocaleString(),
      `${feedback.length} recorded feedback entries`,
      "◇",
    ],
    [
      "Response match",
      scores.length
        ? `${Math.round((scores.reduce((a, b) => a + b, 0) / scores.length) * 100)}%`
        : "—",
      scores.length
        ? `${scores.length} scored outcomes · automated estimate`
        : "Awaiting scored outcomes",
      "≈",
    ],
    [
      "Reported tokens",
      tokenValues.length
        ? tokenValues.reduce((sum, value) => sum + value, 0).toLocaleString()
        : "—",
      `${tokenValues.length} completed calls with usage · loaded window`,
      "#",
    ],
    [
      "Average call latency",
      latencies.length
        ? `${Math.round(latencies.reduce((sum, value) => sum + value, 0) / latencies.length).toLocaleString()} ms`
        : "—",
      `${latencies.length} timed completed calls · loaded window`,
      "◷",
    ],
    [
      "Failure events",
      failures.length.toLocaleString(),
      "Recorded failures · loaded event window",
      "!",
    ],
  ];
  $("#metrics").innerHTML = metrics
    .map(
      ([label, value, note, mark]) =>
        `<div class="metric"><div class="metric-label">${label}<span class="metric-mark">${mark}</span></div><div class="metric-value">${escapeHTML(value)}</div><small>${escapeHTML(note)}</small></div>`,
    )
    .join("");
  drawChart(events);
  const status = instance ? statusFor(instance.id) : {};
  const details = [
    [
      "Status",
      instance
        ? status.connected
          ? "Connected"
          : isRunning(instance)
            ? "Running · awaiting connection"
            : status.state || status.status || "Offline"
        : "No instance",
    ],
    ["Mode", instance?.dry_run ? "Dry run" : "Live"],
    ["Preset", instance?.preset || "—"],
    [
      "Identity",
      instance?.identity_instance
        ? state.instances.find((item) => item.id === instance.identity_instance)
            ?.name || instance.identity_instance
        : "Dedicated",
    ],
    [
      "Thread scope",
      instance?.thread_scope === "conversations"
        ? "All Together AI conversations"
        : "Explicit channels",
    ],
    ["Model", instance?.model || status.model || "Preset default"],
    [
      "Channels",
      rows(instance?.channels).length
        ? rows(instance.channels).join(", ")
        : "Preset configuration",
    ],
    ["Runtime error", status.error || "None recorded"],
    [
      "Gateway catalog",
      state.model_error || `${state.models.length} routes available`,
    ],
  ];
  $("#runtime-details").innerHTML = details
    .map(
      ([label, value]) => `<div><dt>${label}</dt><dd>${text(value)}</dd></div>`,
    )
    .join("");
  $("#event-count").textContent = `${events.length} events`;
  $("#activity-feed").innerHTML = events.length
    ? newest(events).slice(0, 15).map(eventMarkup).join("")
    : empty(
        "Activity appears here after this instance records its first event.",
      );
  const usage = new Map();
  for (const event of calls) {
    const id =
      payload(event).actual_model ||
      payload(event).model ||
      "Unspecified model";
    usage.set(id, (usage.get(id) || 0) + 1);
  }
  $("#model-usage").innerHTML = usage.size
    ? [...usage]
        .sort((a, b) => b[1] - a[1])
        .map(
          ([id, count]) =>
            `<div class="usage-row"><div class="usage-label"><span>${escapeHTML(id)}</span><span class="mono quiet">${count} completed</span></div><div class="bar-track"><div class="bar-fill" style="width:${(count / calls.length) * 100}%"></div></div></div>`,
        )
        .join("")
    : empty("No model calls recorded in the loaded history.");
  $("#tool-timeline").innerHTML = tools.length
    ? newest(tools).slice(0, 6).map(eventMarkup).join("")
    : empty("Tool calls will appear as they run.");
}
function predictionFeedback(id) {
  return state.feedback.filter(
    (item) => String(item.prediction_id) === String(id),
  );
}
function renderConversations() {
  const conversationOwner = current()?.identity_instance || selected;
  const conversations = rows(state.conversations).filter(
    (item) =>
      item.instance_id === selected || item.instance_id === conversationOwner,
  );
  $("#conversation-count").textContent = conversations.length;
  const query = $("#conversation-search").value.trim().toLowerCase();
  const filtered = conversations.filter((item) =>
    `${item.title || ""} ${item.room || ""}`.toLowerCase().includes(query),
  );
  $("#conversation-list").innerHTML = filtered.length
    ? filtered
        .map((item) => {
          const turns = rows(item.turns);
          const room = String(item.room || "");
          const discordURL =
            /^\d+$/.test(String(item.guild_id)) && /^\d+$/.test(room)
              ? `https://discord.com/channels/${item.guild_id}/${room}`
              : "";
          return `<details class="panel conversation-card" data-room="${escapeHTML(room)}" ${expandedConversations.has(room) ? "open" : ""}><summary><span><strong>${text(item.title || room || "Conversation")}</strong><small>${text(room)} · ${turns.length} stored turns · ${text(item.message_count ?? "Unknown")} context messages</small></span><span class="quiet">⌄</span></summary><div class="conversation-content">${discordURL ? `<a class="text-link" href="${discordURL}" target="_blank" rel="noopener noreferrer">Open Discord thread ↗</a>` : ""}${turns.length ? turns.map((turn) => `<article class="conversation-turn"><div class="turn-meta"><strong>${text(turn.author_name || "Unknown author")}</strong><span class="badge">${text(turn.status || "Status unavailable")}</span><span class="mono quiet">${text(turn.model || "Route unavailable")}${turn.version != null ? ` · v${text(turn.version)}` : ""}</span><a href="/api/turns/${encodeURIComponent(item.instance_id)}/${encodeURIComponent(turn.key)}" target="_blank" rel="noopener noreferrer">Raw JSON ↗</a></div><h3>Prompt</h3><p>${text(turn.prompt || "Prompt unavailable.")}</p><h3>Answer</h3><p>${text(turn.answer || "Answer unavailable.")}</p></article>`).join("") : empty("No stored turns were returned for this thread.")}</div></details>`;
        })
        .join("")
    : empty(
        conversations.length
          ? "No threads match this search."
          : "No stored conversation threads are available for this instance.",
      );
}
function renderPredictions() {
  const predictions = newest(scoped(state.predictions));
  $("#prediction-count").textContent = predictions.length;
  const filter = $("#prediction-filter").value;
  const visible = predictions.filter(
    (item) =>
      filter === "all" ||
      (filter === "reviewed") === !!predictionFeedback(item.id).length,
  );
  $("#prediction-list").innerHTML = visible.length
    ? visible
        .map((item) => {
          const data = payload(item);
          const feedback = predictionFeedback(item.id);
          const score =
            data.semantic_match_score ??
            data.semantic_score ??
            data.match_score ??
            data.score ??
            data.comparison?.score;
          const expected =
            data.expected ??
            data.prediction ??
            data.predicted_response ??
            data.expected_response ??
            data.text ??
            data.content;
          const predictionState = data.delivery_failed
            ? "Delivery failed"
            : data.superseded_by
              ? "Superseded"
              : null;
          const actual =
            data.actual ??
            data.actual_response ??
            data.observed_response ??
            data.metadata?.actual_response;
          return `<article class="panel prediction-card"><div class="prediction-head"><div><h3>${text(data.user_name || data.target_user || data.user_id || "Response prediction")}</h3><small>${escapeHTML(dateTime(item.created_at))} · ${escapeHTML(item.id)}</small></div><span class="badge ${actual ? "good" : ""}">${predictionState || (actual ? "Outcome recorded" : "Awaiting outcome")}</span></div><div class="comparison"><div><h4>ORIGINAL EXPECTED RESPONSE</h4><p>${expected ? text(expected) : "Expected response unavailable."}</p>${data.edited_draft != null ? `<h4 class="edited-label">OWNER-EDITED RESPONSE</h4><p>${text(data.edited_draft)}</p>` : ""}${data.superseded_by ? `<p class="prediction-state">Superseded by ${text(data.superseded_by)}</p>` : ""}${data.delivery_failed ? '<p class="prediction-state">Discord delivery failed.</p>' : ""}</div><div><h4>ACTUAL RESPONSE</h4><p>${actual ? text(actual) : "An actual response has not been recorded."}</p></div></div><div class="score-row">${(data.match_method || data.comparison?.method) === "lexical_fallback" ? "Lexical overlap" : "Semantic match"} ${typeof score === "number" ? `<strong>${Math.round(score * 100)}%</strong> · automated estimate` : "— awaiting a scored outcome"}${data.uncertainty != null ? ` · Uncertainty ${text(data.uncertainty)}` : " · Confidence interval unavailable"}${data.match_method || data.comparison?.method ? ` · ${text(data.match_method || data.comparison.method)}` : ""}${typeof data.comparison?.equivalent === "boolean" ? ` · Equivalent ${data.comparison.equivalent ? "yes" : "no"}` : ""}${data.comparison?.reason ? ` · ${text(data.comparison.reason)}` : ""}</div><form class="feedback-form" data-prediction="${escapeHTML(item.id)}"><div class="feedback-actions"><button type="button" data-vote="1">↑ Helpful</button><button type="button" data-vote="-1">↓ Needs work</button></div><label>Corrected expected response<textarea name="edit" rows="2" placeholder="Write a better expected response"></textarea></label><div class="feedback-actions"><button type="button" data-submit-edit>Save correction</button></div><label>Review<textarea name="review" rows="2" placeholder="Describe accuracy, context, or missed intent"></textarea></label><div class="review-row"><button type="submit" class="primary">Submit review</button></div></form>${feedback.length ? `<div class="feedback-history">${feedback.map((entry) => `${text(entry.kind || payload(entry).kind || "Feedback")} · ${text(entry.value ?? payload(entry).value ?? "")}`).join("<br>")}</div>` : ""}</article>`;
        })
        .join("")
    : empty(
        predictions.length
          ? "No predictions match this filter."
          : "Predictions appear here when a predictor instance records an expected response.",
      );
}
function renderConfig(instance) {
  const form = $("#config-form");
  for (const control of form.elements) control.disabled = !instance;
  if (instance && (!dirty || configInstance !== instance.id)) {
    updateIdentityControls(form);
    form.elements.name.value = instance.name || instance.id;
    form.elements.preset.value = instance.preset || "";
    const model = instance.model || "";
    if (
      model &&
      ![...form.elements.model.options].some((option) => option.value === model)
    )
      form.elements.model.add(
        new Option(`${model} (configured · absent from catalog)`, model),
      );
    form.elements.model.value = model;
    form.elements.channels.value = rows(instance.channels).join("\n");
    form.elements.enabled.checked = !!instance.enabled;
    form.elements.dry_run.checked = !!instance.dry_run;
    form.elements.identity_instance.value = instance.identity_instance || "";
    form.elements.thread_scope.value = instance.thread_scope || "channels";
    updateIdentityControls(form);
    dirty = false;
    configInstance = instance.id;
  }
  const advancedForm = $("#advanced-form");
  for (const control of advancedForm.elements) control.disabled = !instance;
  if (!advancedDirty) {
    advancedForm.elements.configuration.value = JSON.stringify(
      instance?.configuration || {},
      null,
      2,
    );
    $("#advanced-state").textContent =
      "Private credential fields are managed by the server.";
  }
  const preset = state.presets.find(
    (item) => presetId(item) === instance?.preset,
  );
  $("#config-details").innerHTML = instance
    ? `<h3>${text(preset?.label || preset?.name || instance.preset || "Custom preset")}</h3><p>${text(preset?.description || "Preset details are supplied by the hub.")}</p><h3>Instance ID</h3><p class="mono">${escapeHTML(instance.id)}</p><h3>Discord identity</h3><p>${text(instance.identity_instance ? state.instances.find((item) => item.id === instance.identity_instance)?.name || instance.identity_instance : "Dedicated bot identity")}</p>${instance.preset === "predictor" ? `<h3>Conversation scope</h3><p>${instance.thread_scope === "conversations" ? "All Together AI conversation threads" : "Configured channels"}</p>` : ""}${instance.identity_instance ? "" : `<h3>Credential file</h3><p class="mono">${text(instance.token_file || "Managed by the server")}</p>`}<h3>Configuration file</h3><p class="mono">${text(instance.config_file || "Preset configuration")}</p>`
    : "Create an instance to configure a bot.";
}
function renderJobs() {
  const jobs = state.jobs.filter(
    (item) => !item.instance_id || item.instance_id === selected,
  );
  $("#jobs-list").innerHTML = jobs.length
    ? newest(jobs)
        .map((item) => {
          const data = payload(item);
          const reference =
            data.diff_ref ||
            data.diff_path ||
            data.commit ||
            data.change_ref ||
            data.proposal?.diff_ref ||
            data.proposal?.diff ||
            data.version;
          const url = data.diff_url || data.url;
          let link = "";
          if (typeof url === "string" && /^https?:\/\//.test(url))
            link = `<p><a href="${escapeHTML(url)}" target="_blank" rel="noopener noreferrer">Open change reference ↗</a></p>`;
          return `<article class="panel job"><div class="job-head"><h3>${text(data.name || data.kind || item.kind || item.id || "Maintenance job")}</h3><span class="badge">${text(data.status || item.status || data.state || "Unknown status")}</span></div><p>${text(data.summary || data.result || data.error || data.note || "No result recorded.")}</p>${reference ? `<p class="mono">${text(reference)}</p>` : ""}${data.proposal ? `<details><summary>Proposed changes</summary><pre>${text(data.proposal)}</pre></details>` : ""}${link}<small class="quiet">${escapeHTML(dateTime(item.created_at || data.started_at || item.updated_at))}</small></article>`;
        })
        .join("")
    : empty("No maintenance jobs have been recorded.");
}
function publicLink(url, label) {
  if (typeof url !== "string" || !/^https?:\/\//.test(url)) return "";
  return `<a href="${escapeHTML(url)}" target="_blank" rel="noopener noreferrer">${text(label)} ↗</a>`;
}
function projectRoom(room, guildId) {
  if (typeof room !== "object" || room === null) room = { id: room };
  const id = room.id || room.room || room.room_id;
  const url =
    room.url ||
    (/^\d+$/.test(String(guildId)) && /^\d+$/.test(String(id))
      ? `https://discord.com/channels/${guildId}/${id}`
      : "");
  const label = room.title || room.name || id || "Thread";
  return `<li>${publicLink(url, label) || text(label)}${room.message_count != null ? ` <span class="quiet">· ${text(room.message_count)} messages</span>` : ""}</li>`;
}
function renderProjects() {
  const owner = current()?.identity_instance || selected;
  const guildId =
    current()?.configuration?.guild_id ||
    state.instances.find((item) => item.id === owner)?.configuration?.guild_id;
  const projects = rows(state.projects).filter(
    (item) =>
      !item.instance_id ||
      item.instance_id === selected ||
      item.instance_id === owner,
  );
  $("#project-count").textContent = projects.length;
  const query = $("#project-search").value.trim().toLowerCase();
  const visible = projects.filter((item) =>
    `${item.name || ""} ${item.summary || ""}`.toLowerCase().includes(query),
  );
  $("#projects-list").innerHTML = visible.length
    ? visible
        .map((item) => {
          const rooms = rows(item.rooms || item.threads);
          const evidence = rows(item.evidence);
          return `<article class="panel project-card"><div class="panel-heading"><div><h3>${text(item.name || item.id || "Unnamed project")}</h3><p>${text(item.summary || "No project summary recorded.")}</p></div><span class="badge">Inferred group</span></div><div class="project-body"><p class="mono quiet">${rooms.length} threads · ${text(item.evidence_count ?? evidence.length)} evidence references</p><p class="quiet">${text(item.uncertainty || "Project membership is inferred and may need review.")}</p>${rooms.length ? `<h4>Threads</h4><ul>${rooms.map((room) => projectRoom(room, item.guild_id || guildId)).join("")}</ul>` : ""}${evidence.length ? `<details><summary>Evidence references</summary><ul>${evidence.map((entry) => `<li>${typeof entry === "object" && entry !== null ? publicLink(entry.url || entry.message_url, entry.summary || entry.label || entry.message_id || "Message evidence") || text(entry.summary || entry.label || entry.message_id || "Reference unavailable") : text(entry)}</li>`).join("")}</ul></details>` : ""}<h4>Reconstruction provenance</h4><pre>${text(item.reconstruction_source || "Reconstruction source unavailable.")}</pre><small class="quiet">Updated ${escapeHTML(dateTime(item.updated_at))}</small></div></article>`;
        })
        .join("")
    : empty(
        projects.length
          ? "No projects match this search."
          : "No reconstructed project groups are available yet.",
      );
  const configuration =
    state.instances.find((item) => item.id === owner)?.configuration ||
    current()?.configuration ||
    {};
  const naming =
    state.naming?.[owner] || state.naming || configuration.thread_naming || {};
  const enabled = naming.enabled ?? configuration.auto_thread_naming;
  $("#naming-status").textContent =
    enabled === true
      ? "Enabled · new conversation threads use auto/fast"
      : enabled === false
        ? "Disabled"
        : "Naming status unavailable";
  $("#naming-detail").textContent =
    naming.description ||
    "Automatic names describe new conversation threads. Project grouping uses recorded evidence and inferred reconstruction.";
}
function renderCleanup() {
  const local = cleanupStates.get(selected);
  $("#review-cleanup").disabled =
    !current() || !!local?.busy || !!local?.running;
  if (!local) {
    $("#cleanup-preview").innerHTML = "";
    return;
  }
  const preview = local.preview || {};
  const permissions = rows(preview.missing_permissions);
  const permissionLinks = (permissions.length ? rows(preview.permission_urls) : [])
    .map((item) =>
      publicLink(item.url, item.name || "Grant cleanup permissions"),
    )
    .filter(Boolean);
  const readGaps = rows(preview.read_gaps);
  const blocked =
    permissions.length > 0 || readGaps.length > 0 || preview.blocked === true;
  const retryable = !!preview.snapshot_id && (
    !!local.error || ["error", "partial", "failed", "interrupted"].includes(preview.status || local.result?.status)
  );
  $("#cleanup-preview").innerHTML =
    `${local.busy ? '<p class="quiet">Loading cleanup scope…</p>' : ""}${local.error ? `<p class="error">${text(local.error)}</p>` : ""}${preview.snapshot_id ? `<dl class="cleanup-scope"><div><dt>Guild</dt><dd>${text(preview.guild_id || "Unavailable")}</dd></div><div><dt>Scope</dt><dd>${text(preview.scope || "All guild messages and AI threads")}</dd></div><div><dt>Messages</dt><dd>${text(preview.message_count ?? "Unavailable")}</dd></div><div><dt>AI threads</dt><dd>${text(preview.thread_count ?? "Unavailable")}</dd></div></dl>` : ""}${permissions.length ? `<p class="error">Missing cleanup permissions ${text(permissions.join(", "))}</p>` : ""}${readGaps.length ? `<div class="error"><p>Cleanup is blocked because parts of the server could not be read.</p><ul>${readGaps.map((gap) => `<li>${text(gap)}</li>`).join("")}</ul></div>` : ""}${preview.blocked ? '<p class="error">The server blocked this cleanup preview.</p>' : ""}${permissionLinks.length ? `<div class="permission-links"><h3>Grant cleanup permissions</h3>${permissionLinks.join(" ")}</div>` : ""}${local.running ? '<p class="quiet" role="status">Cleanup is running. Progress refreshes automatically.</p>' : ""}${local.result ? `<details open><summary>Cleanup result</summary><pre>${text(local.result)}</pre></details>` : ""}${preview.snapshot_id && !blocked && !retryable && !local.running && !local.result && !local.busy ? '<p class="cleanup-warning">The reviewed snapshot includes human messages. Deletion is permanent.</p><button class="danger-button" type="button" data-execute-cleanup>Clear server messages and AI threads</button>' : ""}${retryable && !blocked && !local.running && !local.busy ? '<button class="danger-button" type="button" data-retry-cleanup>Retry same cleanup snapshot</button>' : ""}`;
}
async function reviewCleanup(instanceId) {
  clearTimeout(cleanupTimers.get(instanceId));
  cleanupTimers.delete(instanceId);
  cleanupStates.set(instanceId, { busy: true });
  renderCleanup();
  try {
    const preview = await api(
      `/api/cleanup/${encodeURIComponent(instanceId)}?refresh=1`,
    );
    const running =
      preview.running === true ||
      ["running", "queued", "in_progress"].includes(preview.status);
    const snapshotId = preview.snapshot_id;
    cleanupStates.set(instanceId, { preview, snapshotId, running, result: preview.result });
    if (running && !cleanupTimers.has(instanceId))
      cleanupTimers.set(
        instanceId,
        setTimeout(() => pollCleanup(instanceId, snapshotId), 3000),
      );
  } catch (error) {
    cleanupStates.set(instanceId, {
      error: error.message,
      preview: error.details || {},
    });
  }
  if (selected === instanceId) renderCleanup();
}
async function pollCleanup(instanceId, snapshotId) {
  const before = cleanupStates.get(instanceId);
  snapshotId ||= before?.snapshotId || before?.preview?.snapshot_id;
  if (!snapshotId || (before?.snapshotId || before?.preview?.snapshot_id) !== snapshotId) return;
  cleanupTimers.delete(instanceId);
  try {
    const response = await api(
      `/api/cleanup/${encodeURIComponent(instanceId)}?snapshot_id=${encodeURIComponent(snapshotId)}`,
    );
    const local = cleanupStates.get(instanceId) || {};
    if ((local.snapshotId || local.preview?.snapshot_id) !== snapshotId) return;
    const running = response.running === true ||
      ["running", "queued", "in_progress"].includes(response.status || response.state);
    cleanupStates.set(instanceId, {
      ...local,
      snapshotId,
      preview: { ...local.preview, ...response },
      running,
      error: null,
      result: response.result || (running
        ? { status: response.status || response.state, progress: response.progress }
        : response),
    });
    if (running) cleanupTimers.set(instanceId, setTimeout(() => pollCleanup(instanceId, snapshotId), 3000));
  } catch (error) {
    const local = cleanupStates.get(instanceId) || {};
    if ((local.snapshotId || local.preview?.snapshot_id) !== snapshotId) return;
    cleanupStates.set(instanceId, { ...local, running: false, error: error.message });
    cleanupTimers.delete(instanceId);
  }
  if (selected === instanceId) renderCleanup();
}
async function executeCleanup(instanceId, resume = false) {
  const local = cleanupStates.get(instanceId);
  const snapshotId = local?.snapshotId || local?.preview?.snapshot_id;
  if (!snapshotId || local.running || local.busy || local.preview.blocked ||
      rows(local.preview.missing_permissions).length || rows(local.preview.read_gaps).length) return;
  clearTimeout(cleanupTimers.get(instanceId));
  cleanupTimers.delete(instanceId);
  cleanupStates.set(instanceId, { ...local, snapshotId, running: true, error: null });
  renderCleanup();
  try {
    const response = await api(
      `/api/cleanup/${encodeURIComponent(instanceId)}`,
      { snapshot_id: snapshotId, ...(resume ? { resume: true } : {}) },
    );
    const currentLocal = cleanupStates.get(instanceId) || {};
    if ((currentLocal.snapshotId || currentLocal.preview?.snapshot_id) !== snapshotId) return;
    const running = response.running === true ||
      ["running", "queued", "in_progress"].includes(response.status || response.state);
    cleanupStates.set(instanceId, {
      ...currentLocal,
      snapshotId,
      preview: { ...currentLocal.preview, ...response },
      running,
      error: null,
      result: response.result || response,
    });
    if (running) cleanupTimers.set(instanceId, setTimeout(() => pollCleanup(instanceId, snapshotId), 3000));
  } catch (error) {
    const currentLocal = cleanupStates.get(instanceId) || {};
    if ((currentLocal.snapshotId || currentLocal.preview?.snapshot_id) !== snapshotId) return;
    cleanupStates.set(instanceId, { ...currentLocal, running: false, error: error.message, result: error.details || currentLocal.result });
  }
  if (selected === instanceId) renderCleanup();
}
function render() {
  renderNavigation();
  const instance = current();
  renderHeader(instance);
  renderOverview(instance);
  renderConfig(instance);
  renderJobs();
  renderConversations();
  renderProjects();
  renderCleanup();
  // Preserve unfinished review drafts during background refreshes.
  if (!reviewDirty && !$("#predictions").contains(document.activeElement))
    renderPredictions();
}
async function refresh(manual = false) {
  if (loading) return;
  loading = true;
  $("#refresh").disabled = true;
  try {
    const data = await api("/api/state");
    state = {
      ...data,
      instances: rows(data.instances),
      events: rows(data.events),
      predictions: rows(data.predictions),
      feedback: rows(data.feedback || data.feedbacks),
      jobs: rows(data.jobs),
      models: rows(data.models),
      presets: rows(data.presets),
    };
    if (!state.instances.some((item) => item.id === selected))
      selected = state.instances[0]?.id || "";
    const nextSignature = JSON.stringify([state.presets, state.models]);
    if (nextSignature !== signature) {
      options();
      signature = nextSignature;
    }
    render();
    $("#connection").textContent = "Hub connected";
    $(".connection-dot").classList.remove("offline");
    $("#last-updated").textContent = `Last synced ${time(Date.now())}`;
    if (manual || $("#notice").dataset.fetchError) {
      announce(manual ? "Workspace refreshed." : "");
      delete $("#notice").dataset.fetchError;
    }
  } catch (error) {
    $("#connection").textContent = "Connection interrupted";
    $(".connection-dot").classList.add("offline");
    announce(error.message, true);
    $("#notice").dataset.fetchError = "true";
  } finally {
    loading = false;
    $("#refresh").disabled = false;
  }
}
function openTab(tab) {
  activeTab = tab;
  for (const panel of document.querySelectorAll(".tab-panel"))
    panel.hidden = panel.id !== tab;
  for (const button of document.querySelectorAll("[data-tab]")) {
    if (button.dataset.tab === tab) button.setAttribute("aria-current", "page");
    else button.removeAttribute("aria-current");
  }
}
function channels(value) {
  return value
    .split(/[\s,]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}
function formValues(form) {
  const values = {
    name: form.elements.name.value.trim(),
    preset: form.elements.preset.value,
    channels: channels(form.elements.channels.value),
    enabled: form.elements.enabled.checked,
    dry_run: form.elements.dry_run.checked,
    identity_instance:
      form.elements.preset.value === "predictor"
        ? form.elements.identity_instance.value
        : "",
    thread_scope:
      form.elements.preset.value === "predictor"
        ? form.elements.thread_scope.value
        : "channels",
  };
  if (form.elements.model.value) values.model = form.elements.model.value;
  return values;
}
$("#instances").addEventListener("click", (event) => {
  const button = event.target.closest("[data-instance]");
  if (!button || button.dataset.instance === selected) return;
  if (
    (dirty || advancedDirty || reviewDirty) &&
    !window.confirm("Discard unsaved changes and review drafts?")
  )
    return;
  selected = button.dataset.instance;
  sessionStorage.setItem("bot-hub-instance", selected);
  dirty = false;
  advancedDirty = false;
  reviewDirty = false;
  render();
});
for (const button of document.querySelectorAll("[data-tab]"))
  button.addEventListener("click", () => openTab(button.dataset.tab));
for (const link of document.querySelectorAll("[data-open-tab]"))
  link.addEventListener("click", (event) => {
    event.preventDefault();
    openTab(link.dataset.openTab);
  });
for (const feed of [$("#activity-feed"), $("#tool-timeline")])
  feed.addEventListener(
    "toggle",
    (event) => {
      const detail = event.target;
      if (!detail.matches("details[data-event-key]")) return;
      if (detail.open) expandedEvents.add(detail.dataset.eventKey);
      else expandedEvents.delete(detail.dataset.eventKey);
    },
    true,
  );
$("#refresh").addEventListener("click", () => refresh(true));
$("#prediction-filter").addEventListener("change", renderPredictions);
$("#conversation-search").addEventListener("input", renderConversations);
$("#conversation-list").addEventListener(
  "toggle",
  (event) => {
    const card = event.target;
    if (!card.matches("details[data-room]")) return;
    if (card.open) expandedConversations.add(card.dataset.room);
    else expandedConversations.delete(card.dataset.room);
  },
  true,
);
for (const form of [$("#config-form"), $("#create-form")]) {
  form.addEventListener("change", (event) => {
    if (
      ["preset", "identity_instance", "thread_scope"].includes(
        event.target.name,
      )
    )
      updateIdentityControls(
        form,
        form.getAttribute("id") === "create-form" &&
          event.target.name === "preset" &&
          event.target.value === "predictor",
      );
    if (
      form.getAttribute("id") === "create-form" &&
      event.target.name === "preset"
    )
      form.elements.dry_run.checked = !!state.presets.find(
        (item) => presetId(item) === form.elements.preset.value,
      )?.defaults?.dry_run;
  });
}
$("#config-form").addEventListener("input", () => {
  dirty = true;
  $("#config-state").textContent = "Unsaved changes";
});
$("#config-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!current()) return;
  const form = event.currentTarget;
  const button = form.querySelector('[type="submit"]');
  button.disabled = true;
  try {
    await api(
      `/api/instances/${encodeURIComponent(selected)}`,
      formValues(form),
    );
    dirty = false;
    $("#config-state").textContent = "Configuration saved";
    announce("Instance configuration saved.");
    await refresh();
  } catch (error) {
    announce(error.message, true);
  } finally {
    button.disabled = false;
  }
});
function showCreate() {
  $("#create-error").textContent = "";
  const form = $("#create-form");
  form.elements.dry_run.checked = !!state.presets.find(
    (item) => presetId(item) === form.elements.preset.value,
  )?.defaults?.dry_run;
  updateIdentityControls(form, form.elements.preset.value === "predictor");
  $("#create-dialog").showModal();
}
for (const id of ["add-instance", "create-sidebar"])
  $(`#${id}`).addEventListener("click", showCreate);
for (const id of ["close-dialog", "cancel-create"])
  $(`#${id}`).addEventListener("click", () => $("#create-dialog").close());
$("#create-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = event.currentTarget;
  const button = form.querySelector('[type="submit"]');
  button.disabled = true;
  try {
    const values = {
      ...formValues(form),
      id: form.elements.id.value.trim(),
      config_file: form.elements.config_file.value.trim(),
    };
    if (!values.identity_instance)
      values.token_file = form.elements.token_file.value.trim();
    await api("/api/instances", values);
    selected = values.id;
    $("#create-dialog").close();
    form.reset();
    announce("Instance created.");
    await refresh();
  } catch (error) {
    $("#create-error").textContent = error.message;
  } finally {
    button.disabled = false;
  }
});
async function sendFeedback(form, kind, value) {
  const buttons = form.querySelectorAll("button");
  buttons.forEach((button) => (button.disabled = true));
  try {
    await api(
      `/api/predictions/${encodeURIComponent(form.dataset.prediction)}/feedback`,
      { kind, value },
    );
    announce("Prediction feedback saved.");
    reviewDirty = false;
    form.reset();
    await refresh();
    renderPredictions();
  } catch (error) {
    announce(error.message, true);
  } finally {
    buttons.forEach((button) => (button.disabled = false));
  }
}
$("#prediction-list").addEventListener("input", () => {
  reviewDirty = true;
});
$("#advanced-form").addEventListener("input", () => {
  advancedDirty = true;
  $("#advanced-state").textContent = "Unsaved bot configuration";
});
$("#advanced-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!current()) return;
  const form = event.currentTarget;
  const button = form.querySelector("button");
  try {
    const configuration = JSON.parse(form.elements.configuration.value);
    if (
      !configuration ||
      typeof configuration !== "object" ||
      Array.isArray(configuration)
    )
      throw new Error("Configuration must be a JSON object.");
    button.disabled = true;
    await api(
      `/api/instances/${encodeURIComponent(selected)}/config`,
      configuration,
    );
    advancedDirty = false;
    announce("Bot configuration saved.");
    await refresh();
  } catch (error) {
    announce(error.message, true);
  } finally {
    button.disabled = false;
  }
});
$("#prediction-list").addEventListener("click", (event) => {
  const button = event.target.closest("button");
  if (!button) return;
  const form = button.closest("form");
  if (button.hasAttribute("data-vote"))
    sendFeedback(form, "vote", Number(button.dataset.vote));
  if (button.hasAttribute("data-submit-edit")) {
    const value = form.elements.edit.value.trim();
    if (value) sendFeedback(form, "edit", value);
    else {
      form.elements.edit.focus();
      announce("Enter a corrected response before saving.");
    }
  }
});
$("#prediction-list").addEventListener("submit", (event) => {
  event.preventDefault();
  const form = event.target;
  const value = form.elements.review.value.trim();
  if (value) sendFeedback(form, "review", value);
  else {
    form.elements.review.focus();
    announce("Enter review notes before submitting.");
  }
});
window.addEventListener("beforeunload", (event) => {
  if (dirty || advancedDirty || reviewDirty) {
    event.preventDefault();
    event.returnValue = "";
  }
});
$("#project-search").addEventListener("input", renderProjects);
$("#review-cleanup").addEventListener("click", () => reviewCleanup(selected));
$("#cleanup-preview").addEventListener("click", (event) => {
  if (event.target.closest("[data-execute-cleanup]")) executeCleanup(selected);
  if (event.target.closest("[data-retry-cleanup]")) executeCleanup(selected, true);
});
$("#metrics").innerHTML = '<div class="skeleton"></div>'.repeat(4);
refresh();
setInterval(() => {
  if (!document.hidden && !$("#create-dialog").open) refresh();
}, 5000);
document.addEventListener("visibilitychange", () => {
  if (!document.hidden) refresh();
});
