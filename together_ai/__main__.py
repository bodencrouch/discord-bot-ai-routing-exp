import argparse
import asyncio
import json
from pathlib import Path

from .bot import load_config, run, save_config
from .core import Engine, Store


def main():
    parser = argparse.ArgumentParser(
        description="Together AI Discord bot and route configuration"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="Connect the Discord bot")
    sub.add_parser("status", help="Show gateway and default route")
    models = sub.add_parser(
        "models", help="Fetch available routes from the configured gateway"
    )
    models.add_argument("search", nargs="?", default="")
    route = sub.add_parser("route", help="Configure default or channel route")
    route.add_argument("model")
    route.add_argument("--channel")
    alias = sub.add_parser("alias", help="Add a named route shortcut")
    alias.add_argument("name")
    alias.add_argument("model")
    gateway = sub.add_parser("gateway", help="Set an OpenAI-compatible gateway URL")
    gateway.add_argument("url")
    gateway.add_argument("--key-file", required=True)
    skill = sub.add_parser(
        "skill", help="Register an installed SKILL.md or plugin instruction file"
    )
    skill.add_argument("name")
    skill.add_argument("file")
    pipelines = sub.add_parser(
        "pipelines",
        help="Use an OmniRoute dashboard combo export for /models pipeline details",
    )
    pipelines.add_argument("file")
    media = sub.add_parser(
        "media", help="Set the default image or video generation route"
    )
    media.add_argument("kind", choices=["image", "video"])
    media.add_argument("model")
    persona = sub.add_parser(
        "persona", help="Load a conversational-style instruction file"
    )
    persona.add_argument("file")
    connector = sub.add_parser("connector", help="Register an MCP connector or plugin")
    connector.add_argument("name")
    connector.add_argument("url")
    connector.add_argument(
        "--allow",
        action="append",
        required=True,
        help="Tool name to expose. Repeat for each tool.",
    )
    args = parser.parse_args()
    if args.command == "run":
        run()
        return
    path, config = load_config()
    if args.command == "status":
        print(
            json.dumps(
                {
                    k: config[k]
                    for k in ("guild_id", "base_url", "default_model", "aliases")
                },
                indent=2,
            )
        )
        return
    if args.command == "gateway":
        if not args.url.startswith(
            ("https://", "http://127.0.0.1", "http://localhost")
        ):
            parser.error("Use HTTPS or a local gateway.")
        config["base_url"] = args.url.rstrip("/")
        if not config["base_url"].endswith("/v1"):
            config["base_url"] += "/v1"
        key_path = Path(args.key_file).resolve(strict=True)
        config["api_key_file"] = str(key_path)
    elif args.command == "skill":
        config.setdefault("skills", {})[args.name] = str(
            Path(args.file).resolve(strict=True)
        )
    elif args.command == "pipelines":
        snapshot_path = Path(args.file).resolve(strict=True)
        snapshot = json.loads(snapshot_path.read_text())
        if not isinstance(snapshot.get("combos"), list):
            parser.error(
                "Expected an OmniRoute combo export containing a combos array."
            )
        config["routing_snapshot_file"] = str(snapshot_path)
    elif args.command == "media":
        config.setdefault("media_defaults", {})[args.kind] = args.model
    elif args.command == "persona":
        config["persona_file"] = str(Path(args.file).resolve(strict=True))
    elif args.command == "connector":
        config["connectors"] = [
            c for c in config.get("connectors", []) if c["name"] != args.name
        ]
        config["connectors"].append(
            {"name": args.name, "url": args.url, "allowed_tools": args.allow}
        )
    else:

        async def configure():
            store = Store(config["database"])
            engine = Engine(
                config, Path(config["api_key_file"]).read_text().strip(), store
            )
            try:
                await engine.refresh_models()
                if args.command == "models":
                    print(
                        "\n".join(
                            m for m in engine.models if args.search.lower() in m.lower()
                        )
                    )
                    return
                selected = engine.resolve_model(args.model)
                if args.command == "route" and args.channel:
                    store.set_model(args.channel, selected)
                elif args.command == "route":
                    config["default_model"] = selected
                else:
                    config.setdefault("aliases", {})[args.name] = selected
                print(f"Selected route {selected}")
            finally:
                await engine.http.aclose()
                store.db.close()

        asyncio.run(configure())
        if args.command == "models":
            return
        if args.command == "route" and args.channel:
            print("Channel route updated. It takes effect immediately.")
            return
    save_config(path, config)
    print("Saved. Restart the bot service to load configuration changes.")


try:
    main()
except BrokenPipeError:
    pass
