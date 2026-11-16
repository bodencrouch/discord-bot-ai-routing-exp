"""Represent gateway catalog metadata and exported combo definitions faithfully."""

import json
from datetime import datetime, timezone
from pathlib import Path


def routing_report(config, catalog, matched):
    snapshot = {}
    if config.get("routing_snapshot_file"):
        snapshot = json.loads(Path(config["routing_snapshot_file"]).read_text())
    combos = {combo["name"]: combo for combo in snapshot.get("combos", [])}
    by_id = {model["id"]: model for model in catalog}
    routes = []
    for name in matched:
        metadata = by_id[name]
        if name in combos:
            combo = combos[name]
            route = {
                "route": name,
                "kind": "configured_combo",
                "strategy": combo["strategy"],
                "targets_in_configured_order": combo.get("models", []),
                "retry_and_routing_settings": combo.get("config", {}),
                "context_cache_protection": combo.get("context_cache_protection"),
                "allowed_providers": combo.get("allowedProviders"),
                "description": combo.get("description"),
            }
        elif name == "auto" or name.startswith("auto/"):
            route = {
                "route": name,
                "kind": "dynamic_auto_router",
                "resolution": "OmniRoute resolves eligible connected providers and models at request time.",
                "configured_target_order": None,
                "target_order_status": "The catalog and combo export do not expose a fixed target list for this dynamic route.",
            }
        else:
            route = {
                "route": name,
                "kind": "provider_model"
                if metadata.get("owned_by") != "combo"
                else "unexported_combo",
                "provider": metadata.get("owned_by"),
                "upstream_model": metadata.get("root"),
                "configured_target_order": None,
                "target_order_status": "Additional account failover and runtime selection are not exposed by the model catalog.",
            }
        route["catalog_metadata"] = metadata
        routes.append(route)
    return {
        "gateway": config["base_url"],
        "catalog_fetched_at": datetime.now(timezone.utc).isoformat(),
        "combo_source": "OmniRoute dashboard combo export",
        "combo_snapshot_exported_at": snapshot.get("_meta", {}).get("exportedAt"),
        "routing_notes": [
            "Custom combo definitions reflect the timestamped dashboard export. Re-export after changing gateway combos.",
            "Target order is preserved exactly. Weighted, round-robin and intelligent strategies can choose a different execution order.",
            "Runtime health, quotas, retries, session stickiness and request content can affect the selected target.",
            "Null target lists indicate that the source does not expose the pipeline.",
        ],
        "routes": routes,
    }
