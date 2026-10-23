#!/usr/bin/env python3
"""pyproject.toml (locked by uv.lock, used by CI) must list exactly the runtime requirements file."""

from __future__ import annotations

import re
import tomllib
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def _normalise(spec: str) -> str:
    return re.sub(r"\s+", "", spec).replace("'", '"').lower()


class DependencyManifestTests(unittest.TestCase):
    def test_pyproject_matches_requirements_file(self) -> None:
        requirements = [
            _normalise(line)
            for line in (ROOT / "infra" / "requirements-data-lake.txt").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
        self.assertEqual([_normalise(dep) for dep in project["dependencies"]], requirements)

    def test_root_requirements_points_at_the_same_file(self) -> None:
        lines = (ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines()
        self.assertIn("-r infra/requirements-data-lake.txt", lines)


if __name__ == "__main__":
    unittest.main()
