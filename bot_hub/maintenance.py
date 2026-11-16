"""Reviewed code changes hosted by an existing Discord identity."""

import asyncio

from parent_ai.__main__ import Parent, RequestButton
from .runtime import HubParent


class HostMaintenance(HubParent):
    """Run the existing maintenance worker through a chat client's connection."""

    def __init__(self, host, config, runtime):
        self.host = host
        self._maintenance_started = False
        self._maintenance_closed = False
        self._maintenance_start_lock = asyncio.Lock()
        super().__init__(config, host.instance, runtime.state, runtime.restart_chat)
        self.hub_runtime = runtime
        host.add_dynamic_items(RequestButton)

    @property
    def user(self):
        return self.host.user

    def get_channel(self, channel_id):
        return self.host.get_channel(channel_id)

    async def fetch_channel(self, channel_id):
        return await self.host.fetch_channel(channel_id)

    async def wait_until_ready(self):
        await self.host.wait_until_ready()

    def is_ready(self):
        return not self._maintenance_closed and self.host.is_ready()

    def is_closed(self):
        return self._maintenance_closed or self.host.is_closed()

    def allowed(self, channel):
        return self.host.allowed(channel)

    async def change_presence(self, **kwargs):
        # Parent recovery also runs here, while the host owns its presence.
        return None

    async def start(self):
        async with self._maintenance_start_lock:
            if self._maintenance_started or self._maintenance_closed:
                return
            await self.host.wait_until_ready()
            try:
                await Parent.setup_hook(self)
                await Parent.on_ready(self)
            except Exception:
                await self.close()
                raise
            self._maintenance_started = True

    def owns_thread(self, thread_id):
        if self._maintenance_closed:
            return False
        return (
            self.db.execute(
                "SELECT 1 FROM jobs WHERE thread=? LIMIT 1", (thread_id,)
            ).fetchone()
            is not None
        )

    async def handle_message(self, message):
        if not self.owns_thread(message.channel.id):
            return False
        job = self.db.execute(
            "SELECT * FROM jobs WHERE thread=? ORDER BY id DESC LIMIT 1",
            (message.channel.id,),
        ).fetchone()
        if (
            not message.author.bot
            and self.user.id in message.raw_mentions
            and self.can_manage(message.author, job)
        ):
            await self.on_message(message)
        return True

    async def close(self):
        if self._maintenance_closed:
            return
        self._maintenance_closed = True
        await Parent.close(self)
