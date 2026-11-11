#!/usr/bin/env python3
"""Append one batch of rows to the source coverage registry.

Usage: source_coverage_add_batch.py <rows.yaml> --report docs/source-coverage/batch-NN.md

``rows.yaml`` holds ``batch``, ``checked_at``, ``sources`` (the rows, without ``batch``/``checked_at``)
and ``next_batch`` (list of ids). The script stamps every row, appends it, moves ``current_batch``,
records the report path and replaces ``next_batch``. Run source_coverage_check.py afterwards.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
REGISTRY = ROOT / "repository-catalog" / "registries" / "source-coverage.yaml"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("rows", type=Path)
    parser.add_argument("--report", required=True)
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    args = parser.parse_args(argv)

    spec = yaml.safe_load(args.rows.read_text(encoding="utf-8"))
    batch, day = int(spec["batch"]), str(spec["checked_at"])
    rows = []
    for row in spec["sources"]:
        stamped = {"id": row["id"], "name": row["name"], "batch": batch}
        stamped.update({k: v for k, v in row.items() if k not in {"id", "name"}})
        stamped["checked_at"] = day
        rows.append(stamped)

    text = args.registry.read_text(encoding="utf-8")
    head, sep, _ = text.partition("next_batch:\n")
    if not sep:
        print("ERROR: registry has no next_batch block", file=sys.stderr)
        return 1
    current = yaml.safe_load(text)["current_batch"]
    if batch != current + 1:
        print(
            f"ERROR: batch {batch} does not follow current_batch {current}",
            file=sys.stderr,
        )
        return 1
    head = head.replace(f"current_batch: {current}\n", f"current_batch: {batch}\n", 1)
    marker = f"  {current}: {{checked_at:"
    lines = head.splitlines(True)
    at = next(i for i, line in enumerate(lines) if line.startswith(marker))
    lines.insert(at + 1, f"  {batch}: {{checked_at: '{day}', report: {args.report}}}\n")
    head = "".join(lines)

    block = yaml.safe_dump(rows, sort_keys=False, allow_unicode=True, width=200)
    block = "".join(
        "  " + line if line.strip() else line for line in block.splitlines(True)
    )
    nxt = (
        f"next_batch:\n  number: {batch + 1}\n  sources:\n    ["
        + ", ".join(spec["next_batch"])
        + "]\n"
    )
    args.registry.write_text(head + block + nxt, encoding="utf-8")
    print(f"added {len(rows)} rows as batch {batch}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
