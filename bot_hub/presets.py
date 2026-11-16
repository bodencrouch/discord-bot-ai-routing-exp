"""Bundled profiles for each independently configured Discord identity."""

PRESETS = {
    "chat": {
        "id": "chat",
        "name": "Together AI",
        "label": "Together AI",
        "description": "Shared conversations, model routes, skills and connected tools.",
        "defaults": {"model": "auto/fast", "dry_run": False, "channels": []},
    },
    "maintenance": {
        "id": "maintenance",
        "name": "Skill Issue Department",
        "label": "Skill Issue Department",
        "description": "Propose reviewed improvements and apply changes after approval.",
        "defaults": {"model": "auto/best-free", "dry_run": False, "channels": []},
    },
    "predictor": {
        "id": "predictor",
        "name": "Predictor",
        "label": "Predictor",
        "description": "Draft predictions, collect feedback and review outcomes.",
        "defaults": {"model": "auto/fast", "dry_run": True, "channels": []},
    },
}


def presets():
    """Return independent profile metadata suitable for JSON API responses."""
    import copy

    return copy.deepcopy(list(PRESETS.values()))
