"""Run all configured Discord identities and the local management dashboard."""

import argparse
import asyncio
import json
import logging
import os
import signal
from pathlib import Path

from .runtime import Runtime
from .state import State
from .web import Dashboard


async def serve(path):
    config = json.loads(path.read_text())
    os.umask(0o077)
    state = State(config["database"])
    runtime = Runtime(config, state)
    dashboard = Dashboard(runtime, state, config, asyncio.get_running_loop())
    login_url = dashboard.start()
    login_path = Path(config["dashboard_login_file"])
    login_path.write_text(login_url + "\n")
    login_path.chmod(0o600)
    logging.info(
        "HUB_READY dashboard=http://127.0.0.1:%s instances=%s",
        dashboard.server.server_port,
        len(runtime.instances()),
    )
    stopping = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, stopping.set)

    async def refresh():
        while not stopping.is_set():
            await dashboard.refresh_models()
            try:
                await asyncio.wait_for(stopping.wait(), 120)
            except asyncio.TimeoutError:
                pass

    task = asyncio.create_task(refresh())
    try:
        await runtime.start()
        await stopping.wait()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await runtime.close()
        await asyncio.to_thread(dashboard.close)
        if hasattr(state, "close"):
            state.close()


def main():
    parser = argparse.ArgumentParser(description="Discord bot presets and dashboard")
    parser.add_argument(
        "--config", default=os.environ.get("BOT_HUB_CONFIG", "hub.json")
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(serve(Path(args.config)))


if __name__ == "__main__":
    main()
