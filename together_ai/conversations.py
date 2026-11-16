import discord


class EditPrompt(discord.ui.Modal, title="Edit your prompt"):
    prompt = discord.ui.TextInput(
        label="Your prompt", style=discord.TextStyle.paragraph, max_length=4000
    )

    def __init__(self, turn):
        super().__init__(timeout=900)
        self.turn_key, self.version = turn["key"], turn["version"]
        self.prompt.default = turn["prompt"]

    async def on_submit(self, interaction):
        await interaction.response.defer()
        try:
            await interaction.client.revise_turn(
                self.turn_key,
                interaction.user,
                self.prompt.value,
                expected_version=self.version,
            )
        except Exception as error:
            await interaction.channel.send(
                f"Couldn't update that answer. {interaction.client.error_text(error)}"
            )


class TurnButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r"turn:(?P<key>[a-f0-9]{32}):(?P<action>retry|edit):(?P<version>\d+)",
):
    def __init__(self, key, action, version):
        self.key, self.action, self.version = key, action, version
        super().__init__(
            discord.ui.Button(
                label="Retry" if action == "retry" else "Edit prompt",
                style=discord.ButtonStyle.secondary,
                custom_id=f"turn:{key}:{action}:{version}",
            )
        )

    @classmethod
    async def from_custom_id(cls, interaction, item, match):
        return cls(match["key"], match["action"], int(match["version"]))

    async def callback(self, interaction):
        turn = interaction.client.store.turn(self.key)
        if turn is None:
            await interaction.response.send_message(
                "This conversation turn is no longer available."
            )
            return
        if self.action == "edit":
            if str(interaction.user.id) != turn["author_id"]:
                await interaction.response.send_message(
                    "The person who wrote this prompt can edit it. You can add your own message or use Retry."
                )
                return
            if len(turn["prompt"]) > 4000:
                await interaction.response.send_message(
                    "Please edit this long prompt in its original Discord message."
                )
                return
            await interaction.response.send_modal(EditPrompt(turn))
            return
        await interaction.response.defer()
        try:
            await interaction.client.revise_turn(
                self.key, interaction.user, expected_version=self.version
            )
        except Exception as error:
            await interaction.channel.send(
                f"Couldn't retry that answer. {interaction.client.error_text(error)}"
            )
