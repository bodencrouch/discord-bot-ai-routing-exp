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
| `/image prompt model` | Generate an image through a connected gateway route |
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
./together-ai media image YOUR_IMAGE_ROUTE
./together-ai media video YOUR_VIDEO_ROUTE
./together-ai persona /absolute/path/persona.md
systemctl --user restart together-ai
```

The running bot loads configuration at startup. Discord route changes take effect immediately. CLI configuration changes take effect after restarting the service. Channel route choices are persisted in SQLite.

Image and video commands call the gateway's `/v1/images/generations` and `/v1/videos/generations` endpoints. Autocomplete suggests routes with the matching media capability. Choose a route explicitly or configure a default with the CLI. The current installation uses the verified `antigravity/gemini-3.1-flash-image` image route by default. Video generation still requires a working authenticated provider. Generation requests are submitted once and can wait up to ten minutes for a gateway result. The bot displays returned image URLs, video URLs and base64 media, with raw JSON available behind its button. A connected compatible generation provider is required. Large media is subject to the Discord server's upload limit.

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
systemctl --user status together-ai
journalctl --user -u together-ai -f
systemctl --user restart together-ai
```

The bot runs on this computer. Its availability depends on this computer and the configured gateway being online. Exa provides web search and fetching through its hosted MCP service. Its anonymous service has rate limits.
