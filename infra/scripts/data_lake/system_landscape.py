#!/usr/bin/env python3
"""Render the Solo Empire system landscape as Markdown from the registries and metadata catalog.

Same inputs and loader as registry_lake_export.py (solo-empire online-systems.yaml, data-sources.yaml,
business-capabilities.yaml; architecture-docs catalog.yaml and lineage.yaml), and the same rules as the dbt
models meta_system_landscape and meta_capability_coverage, so the page and the lake agree. Output is
deterministic (no timestamps): re-render and diff to see what changed. With --check the command exits 1 when
the file at --output differs from a fresh render.

Usage:
    python3 infra/scripts/data_lake/system_landscape.py --registries <dir> --catalog <dir> [--output FILE] [--check]
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "infra" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from data_lake.registry_lake_export import load_records  # noqa: E402

NOT_RUNNING = {"planned", "staged"}
MAX_DEPTH = 10


def _reach(sources: list[dict[str, Any]], edges: list[dict[str, Any]]) -> dict[str, set[str]]:
    """Catalog assets each flow reaches: its own catalog_assets and everything downstream (meta_source_lineage)."""
    downstream: dict[str, set[str]] = defaultdict(set)
    for edge in edges:
        downstream[edge["from_asset"]].add(edge["to_asset"])
    reach: dict[str, set[str]] = {}
    for source in sources:
        seen = set(source["catalog_assets"])
        frontier = set(seen)
        for _ in range(MAX_DEPTH):
            frontier = {nxt for asset in frontier for nxt in downstream.get(asset, ())} - seen
            if not frontier:
                break
            seen |= frontier
        reach[source["id"]] = seen
    return reach


def build(records: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    """Landscape rows per system and coverage rows per domain (same columns as the dbt models)."""
    kinds = {asset["id"]: asset["kind"] for asset in records["catalog_assets"]}
    reach = _reach(records["data_sources"], records["lineage_edges"])
    domains_of: dict[str, list[str]] = defaultdict(list)
    for row in sorted(records["system_capabilities"], key=lambda r: (r["system_id"], r["rank"])):
        domains_of[row["system_id"]].append(row["domain_id"])

    systems = []
    for system in records["systems"]:
        flows = [s for s in records["data_sources"] if s["destination_system"] == system["id"]]
        assets = set().union(*(reach[s["id"]] for s in flows)) if flows else set()
        domains = domains_of.get(system["id"], [])
        systems.append({
            **system,
            "primary_domain": domains[0] if domains else None,
            "domains": domains,
            "inbound_flows": len(flows),
            "automated_flows": sum(s["automation"] == "automated" for s in flows),
            "catalog_assets": len(assets),
            "consumers": sum(kinds.get(a) == "consumer" for a in assets),
        })

    by_id = {s["id"]: s for s in systems}
    coverage = []
    for domain in sorted(records["capability_domains"], key=lambda d: d["number"]):
        serving = [by_id[r["system_id"]] for r in records["system_capabilities"]
                   if r["domain_id"] == domain["id"] and r["system_id"] in by_id]
        running = [s for s in serving if s["status"] not in NOT_RUNNING]
        automated = sum(s["automated_flows"] for s in serving)
        coverage.append({
            **domain,
            "systems": len(serving),
            "primary_systems": sum(s["primary_domain"] == domain["id"] for s in serving),
            "running_systems": len(running),
            "automated_flows": automated,
            "consumers": sum(s["consumers"] for s in serving),
            "system_ids": sorted(s["id"] for s in serving),
            "platform_coverage": "missing" if not running else ("partial" if not automated else "served"),
        })
    unregistered = sorted({s["destination_system"] for s in records["data_sources"]
                           if s["destination_system"] and s["destination_system"] not in by_id})
    return {"systems": systems, "coverage": coverage, "unregistered_destinations": unregistered,
            "flows": len(records["data_sources"]), "assets": len(records["catalog_assets"]),
            "edges": len(records["lineage_edges"])}


def _cell(value: object) -> str:
    return str(value if value not in (None, "") else "—").replace("|", "\\|")


def render(landscape: dict[str, Any]) -> str:
    systems, coverage = landscape["systems"], landscape["coverage"]
    names = {d["id"]: d["name"] for d in coverage}
    unmapped = [s["id"] for s in systems if not s["domains"]]
    counts: defaultdict[str, int] = defaultdict(int)
    for row in coverage:
        counts[row["platform_coverage"]] += 1
    lines = [
        "---",
        'title: "Solo Empire — Generated System Landscape"',
        "status: current",
        "tags: [operations, enterprise-architecture, landscape, capability-map, generated]",
        'description: "Every system in online-systems.yaml mapped to the 15 EOS capability domains, with inbound flows,'
        ' catalog assets and consumers. Generated from the registries and the metadata catalog; do not edit."',
        "related: [./SYSTEM-LANDSCAPE.md, ../../docs/architecture/EOS-GAP-MAP.md, ../../systems/ai-data/metadata-catalog/README.md]",
        "---",
        "",
        "<!-- Generated by bookchaowalit/data-lake-pipelines:infra/scripts/data_lake/system_landscape.py.",
        "     Do not edit by hand: change the registries or the metadata catalog and re-render. -->",
        "",
        "# Solo Empire — Generated System Landscape",
        "",
        "Sources: `bookchaowalit/solo-empire:repository-catalog/registries/` (online-systems.yaml, data-sources.yaml,",
        "business-capabilities.yaml) and `bookchaowalit/architecture-docs:systems/ai-data/metadata-catalog/`",
        "(catalog.yaml, lineage.yaml). The same rows are in the lake as dbt models `meta_system_landscape` and",
        "`meta_capability_coverage`, refreshed daily by the `registry-export` cron.",
        "",
        f"**Totals:** {len(systems)} systems · {len(coverage)} capability domains · {landscape['flows']} inbound flows · "
        f"{landscape['assets']} catalog assets · {landscape['edges']} lineage edges.",
        f"**Platform coverage:** served {counts['served']} · partial {counts['partial']} · missing {counts['missing']}"
        f" · systems without a capability: {len(unmapped)}.",
        "",
        "Platform coverage is mechanical: *missing* = no running system serves the domain; *partial* = systems run",
        "but no automated inbound flow lands in them; *served* = both. It is not the business judgement in",
        "`docs/architecture/EOS-GAP-MAP.md`.",
        "",
        "## 1. Capability coverage",
        "",
        "| # | Domain | Systems (primary) | Running | Automated flows | Consumers | Platform coverage | Systems |",
        "|---|---|---|---|---|---|---|---|",
    ]
    by_id = {s["id"]: s for s in systems}
    for row in coverage:
        listed = ", ".join(f"**{sid}**" if by_id[sid]["primary_domain"] == row["id"] else sid
                           for sid in row["system_ids"])
        lines.append(
            f"| {row['number']} | {_cell(row['name'])} | {row['systems']} ({row['primary_systems']}) | "
            f"{row['running_systems']} | {row['automated_flows']} | {row['consumers']} | "
            f"{row['platform_coverage']} | {_cell(listed)} |")
    lines += ["", "Bold = the domain is the system's primary one.", "",
              "## 2. Systems by primary domain", ""]
    for row in coverage:
        members = sorted((s for s in systems if s["primary_domain"] == row["id"]), key=lambda s: s["id"])
        if not members:
            continue
        lines += [f"### {row['number']}. {row['name']}", "",
                  "| System | Name | Kind | Status | Host | Also serves | Inbound flows (automated) | Assets | Consumers |",
                  "|---|---|---|---|---|---|---|---|---|"]
        for s in members:
            host = "/".join(p for p in (s["host_provider"], s["host_mode"]) if p)
            also = ", ".join(names.get(d, d) for d in s["domains"][1:])
            lines.append(
                f"| `{s['id']}` | {_cell(s['name'])} | {_cell(s['kind'])} | {_cell(s['status'])} | {_cell(host)} | "
                f"{_cell(also)} | {s['inbound_flows']} ({s['automated_flows']}) | {s['catalog_assets']} | "
                f"{s['consumers']} |")
        lines.append("")
    lines += ["## 3. Gaps", ""]
    gaps = [r for r in coverage if r["platform_coverage"] != "served"]
    for row in gaps:
        lines.append(f"- **{row['name']}** — {row['platform_coverage']} "
                     f"({row['running_systems']} running systems, {row['automated_flows']} automated flows).")
    if unmapped:
        lines.append(f"- Systems without a capability domain: {', '.join(f'`{s}`' for s in unmapped)}.")
    if landscape["unregistered_destinations"]:
        lines.append("- Inbound flows land in systems missing from online-systems.yaml: "
                     + ", ".join(f"`{s}`" for s in landscape["unregistered_destinations"]) + ".")
    if not gaps and not unmapped and not landscape["unregistered_destinations"]:
        lines.append("- None.")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--registries", type=Path, required=True,
                        help="solo-empire repository-catalog/registries directory")
    parser.add_argument("--catalog", type=Path, required=True,
                        help="architecture-docs systems/ai-data/metadata-catalog directory")
    parser.add_argument("--output", type=Path, help="write here instead of stdout")
    parser.add_argument("--check", action="store_true", help="exit 1 when --output is not current")
    args = parser.parse_args(argv)
    text = render(build(load_records(args.registries, args.catalog)))
    if args.check:
        if args.output is None:
            parser.error("--check needs --output")
        current = args.output.read_text(encoding="utf-8") if args.output.exists() else ""
        if current != text:
            print(f"FAIL {args.output} is out of date; re-render with system_landscape.py")
            return 1
        print(f"OK {args.output} is current")
        return 0
    if args.output:
        args.output.write_text(text, encoding="utf-8")
        print(f"wrote {args.output}")
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
