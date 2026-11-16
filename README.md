# Discord Bot Hub

The active application runs Together AI, Skill Issue Department and the Reply Guy predictor preset under one supervisor and dashboard. Together AI retains the chat identity. Reply Guy shares that identity and listens in Together AI conversation threads. Skill Issue Department retains its existing identity and management-channel scope.

Run `python -m bot_hub --config /absolute/private/hub.json`, or use `bot-hub.service` on this host. `hub.example.json` lists the bundled instances. Configuration changes, model routes, channel scope, enabled state and dry-run mode are managed in the dashboard. The private dashboard login link is written to the configured `dashboard_login_file`. The HTTP server binds to loopback only and requires a session cookie for data and controls.

The dashboard shows live connection state, conversation history, model and tool activity, maintenance proposals, prediction feedback and session exports. Activity charts reflect recorded events. Existing conversation and maintenance SQLite history is reused. New telemetry is retained in the hub database. Source archives and the private learning index stay outside the repository.

## Cleanup, projects and automatic thread names

Both Discord identities expose `/status`, `/cleanup` and `/projects`. Status includes a cleanup button. A server administrator can review a snapshot, then confirm clearing its server messages and managed AI threads. The snapshot includes members’ messages. Newer conversation activity is preserved. The dashboard exposes the same review and execution controls to its authenticated operator. Dry-run instances retain preview behavior.

The permission button opens Discord’s authorization page with the bot selected for this server. Discord must grant Manage Messages and Manage Threads before full cleanup can run. Channel overrides and inaccessible private history appear in the review. The bot cannot grant itself permissions. Thread-starter and membership system entries disappear when their thread is deleted.

Project organization uses the installed Thought Reconstruction skill, its evidence contract and Protect Your Energy’s sustain-momentum guidance. Source paths and hashes are retained with the inferred groups. The classifier recovers purpose, relationships, corrections and uncertainty from current server activity. It uses `auto/fast`, considers bounded recent context and project candidates, and requires attributed human evidence. Message edits and deletions invalidate affected evidence. Historical archive data remains in the separate predictor workflow. Project groups and source links appear in `/projects` and the dashboard.

Default-titled chat threads receive a coherent name from `auto/fast` after their first meaningful prompt. New maintenance threads use the same naming route with their issue number retained. Explicit user-supplied titles take precedence. Failed inference leaves an explicit naming failure in activity records. These features require the installed Thought Reconstruction and Protect Your Energy guidance on the bot host.

## Prediction and review

Reply Guy defaults to dry-run with the `auto/fast` route. Skill Issue Department uses `auto/best-free`. It posts a labelled proposed follow-up with Helpful, Missed, Edit and Review controls. Votes, edited drafts and reviews persist and inform later predictions. A subsequent message from the configured owner is compared with the original prediction by intent and constraints. A semantic score of at least 0.8 marks an estimated match. The predictor configuration can override this with `semantic_threshold`. Lexical fallback is labelled separately and establishes equivalence only for normalized exact text. These are uncalibrated estimates, and human reviews remain visible alongside them.

The local archive index currently contains 22,470 deduplicated assistant/owner follow-up pairs. The private coverage record identifies reviewed collections and omissions, including three oversized Codex records. This is archive-informed retrieval with feedback, and exact imitation of a person is unverified. A bounded review of 46 pairs across 44 conversations supplies 11 contextual behavior/style guidelines, with aggregate style measurements across the index. Raw historical conversations stay local. The gateway receives inferred guidance, recent Discord feedback and current scoped Discord messages.

Dry-run defaults off for chat and maintenance. Chat dry-run produces reviewable drafts and suppresses connector execution. Maintenance dry-run prepares a proposal and keeps installation blocked. Predictor dry-run always presents its draft explicitly as a bot prediction. All instance modes are configurable through the dashboard.

## Migration on this host

`bot-hub.service` replaces the two independently running service processes. The old `together-ai.service` and `skill-issue-department.service` units are disabled during migration. Their source modules remain reusable implementation components. Credentials stay in their existing private files, and the single hub registry references them. Maintenance reconnects chat through the supervisor so it can continue reporting progress while child code reloads.

The reference sections below describe the reused chat and maintenance capabilities and their standalone commands.

# Together AI

A collaborative Discord bot backed by an OpenAI-compatible AI gateway, with shared channel conversations, dedicated threads, skills and MCP connectors.

## In Discord

Mention the bot or reply to one of its messages. Everyone sees the response. Each channel or thread has its own persistent history. All captured conversation messages remain available to the model, subject to the selected provider’s context window. Deleting a captured Discord message removes it from that history.

| Command | Action |
| --- | --- |
| `/chat message` | Ask the current model, optionally selecting a model or skill |
| `/new title` | Start a dedicated shared conversation thread |
| `/thread with_user` or `/convo with_user` | Start or adopt a conversation thread, add friends and ping them there |
| `/retry model` | Retry the latest answer, optionally through another route |
| `/history` | Export the conversation messages currently included in the AI context |
| `/participation` | Select Off, Quiet, Balanced or Lively and adjust the pause and cooldown |
| `/video prompt model` | Generate a video through a connected gateway route |
| `/model route` | Choose this channel's model or route alias |
| `/models search` | Attach matching route IDs and a JSON routing-pipeline report |
| `/routes` | List favorite aliases, or save an alias with Manage Server permission |
| `/skills` and `/skill` | Discover and invoke skills |
| `/connectors` and `/tool` | Discover and invoke MCP connector/plugin tools |
| `/summarize` | Summarize recent messages in this channel |
| `/reset` | Clear this channel's AI context |
| `/status` and `/help` | Show configuration and usage |

You can also write `@Together AI @explain your question` to select a skill.

Responses are public. Raw provider responses and connector results sit behind persistent **Show raw JSON** and **Hide raw JSON** buttons. Large JSON payloads appear as complete downloadable files when expanded. These buttons continue working after a bot restart.

New answers include **Retry** and **Edit prompt** controls. Retry regenerates the selected answer. The prompt author can edit through the modal or edit the original Discord message. Editing a thread prompt rewinds the same thread, deletes subsequent Discord messages and AI history, and regenerates from the edited prompt. Retrying a thread answer rewinds from that prompt too. This requires Manage Messages permission. Public-channel edits update the linked exchange while preserving the surrounding human discussion. Revision records, controls and message links survive restarts. Existing history remains available, and the new editing controls apply to answers created after this feature was installed.

Use `/convo` (or `/thread`, selecting Together AI’s command) with a friend selected in `with_user`, add more mentions in `participants`, and optionally include an opening `message`. The bot adds everyone and pings them in the thread. Running it inside an existing thread activates shared AI participation there. Every conversation is isolated and keeps its captured history without an exchange-count cap. The model sees speaker names. Native edits and deletions are synchronized with the AI context. Deleting a source prompt also removes its linked bot answer. Deleting a bot answer removes that answer from the context.

The routing report combines live gateway catalog metadata with a timestamped OmniRoute dashboard combo export. It preserves configured target order, weights, selection strategy and retry settings. Dynamic auto routes are marked with their request-time resolution behavior. Update the export after changing custom combos.

## Participation and voice

The bot waits for a conversational pause and defaults to quiet participation. It considers an unaddressed contribution when there is an unanswered question, an invitation for input or another relevant opening. Human-directed mentions, acknowledgments and fragments are left to the participants. It can choose silence after reading the context. Mentions and direct replies request an answer explicitly.

Use `/participation level cooldown delay` in a channel or thread to adjust its behavior live. Quiet defaults to a six-second pause and a sixty-second minimum interval between unprompted contributions. Off keeps explicit requests available. Balanced and Lively broaden the kinds of openings it considers. The level, pause and cooldown are stored per conversation and survive restarts.

The included `persona.md` distills the requested curious, practical, playful builder voice into coherent conversational replies. Public-channel answers are usually short. Threads allow task-appropriate depth. The bot remains visibly identified as Together AI. You can load a different style file through the CLI.

## Terminal configuration

Run commands from this directory, or invoke the `together-ai` script by absolute path.

```bash
./together-ai status
./together-ai models claude
./together-ai route auto/fast
./together-ai route auto/claude-sonnet --channel CHANNEL_ID
./together-ai alias claude auto/claude-sonnet
./together-ai gateway https://omniroute-edge.bodecloud.workers.dev --key-file /absolute/private/ai-key
./together-ai skill my-skill /absolute/path/SKILL.md
./together-ai connector web https://mcp.exa.ai/mcp --allow web_search_exa --allow web_fetch_exa
./together-ai pipelines /absolute/path/omniroute-combos-export.json
./together-ai media video YOUR_VIDEO_ROUTE
./together-ai persona /absolute/path/persona.md
systemctl --user restart bot-hub
```

The running bot loads configuration at startup. Discord route changes take effect immediately. CLI configuration changes take effect after restarting the service. Channel route choices are persisted in SQLite.

The video command calls the gateway's `/v1/videos/generations` endpoint. Autocomplete suggests routes with the matching media capability. Choose a route explicitly or configure a default with the CLI. Video generation still requires a working authenticated provider. Generation requests are submitted once and can wait up to ten minutes for a gateway result. The bot displays returned image URLs, video URLs and base64 media, with raw JSON available behind its button. A connected compatible generation provider is required. Large media is subject to the Discord server's upload limit.

## Install on another host

Use Python 3.13 or newer and uv.

```bash
uv sync
cp config.example.json config.json
# Set your guild ID and private credential/state paths in config.json.
./together-ai run
```

Create an application in Discord's developer portal. Enable Message Content Intent and install it with bot and applications.commands scopes. Grant View Channel, Send Messages, Send Messages in Threads, Create Public Threads, Read Message History, Embed Links, Attach Files and Manage Messages for thread rewinding.

Keep the Discord bot token and gateway API key in separate private files with mode 0600. Set their paths in configuration. Keep state outside the repository. Register only skills and connector tools intended for the shared server.

## Running service

The included service unit is configured for the current host. It starts with the user's systemd session and restarts automatically after failure. Adjust its absolute paths when moving the project.

```bash
systemctl --user status bot-hub
journalctl --user -u bot-hub -f
systemctl --user restart bot-hub
```

The bot runs on this computer. Its availability depends on this computer and the configured gateway being online. Exa provides web search and fetching through its hosted MCP service. Its anonymous service has rate limits.

## Skill Issue Department

The maintenance identity accepts explicit @mentions in the management channel `1571760182248218634` and its threads. Mention it with a Together AI change request. It creates a discussion thread and maintains a public status card with Edit request, approval and cancellation buttons. Thread follow-ups also require an @mention. The requester and server administrators can approve, edit, cancel and refine a proposal.

The parent runs the installed protecting-your-energy `UserPromptSubmit` hook before each model planning call, uses source listing, reading, searching, settings inspection and proposal validation tools, asks necessary clarifying questions, and attaches a proposed diff. The thread shows actual model responses, tool calls and results, with long output attached as files. Confirmation queues the exact reviewed change. Source changes since review require a fresh proposal and approval. Requests are planned concurrently. Approved deployments are coordinated so changes cannot overwrite each other, and a stale preview requires renewed approval.

The parent can edit Together AI’s project files, dependency manifests, child service unit and non-secret configuration, including gateway URLs, routes, aliases, connectors and skills. Credentials remain in their existing private files. Dependencies are synchronized and the service unit reloaded during deployment. The maintenance client stays connected while the supervisor reloads the chat client. Dependency installation or runtime validation failures trigger rollback.

Use Edit request on a completed card or @mention the parent in its thread to refine an implemented change. Each revision needs fresh approval. Changes are recorded locally by the parent in its audit database. Source publication remains an operator action.

The maintenance preset runs within `bot-hub.service` using its private configuration. Together AI ignores the management channel and its threads so the maintenance preset owns that conversation. Both Discord identities run on this host.

The parent defaults to `auto/best-free`. Planning uses staged edits and a finish tool, detects repeated inspection calls, and asks for a final proposal when inspection repeats.
