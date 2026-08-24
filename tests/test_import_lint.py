"""hamsci-physics must never import hf_timestd.

The split's whole point: the science half consumes shared engines from
hamsci-dsp and the timing core's *data products* (the frozen
/var/lib/timestd SQLite store), never the timing core's Python.  An
``import hf_timestd`` here would re-couple the repos and make the timing
package a runtime dependency of every science host.

Mirrors hamsci-dsp's own guard (tests/test_import_lint.py there).
"""
from __future__ import annotations

import re
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src" / "hamsci_physics"
PATTERN = re.compile(r"^\s*(?:from\s+hf_timestd|import\s+hf_timestd)", re.M)


def test_no_module_imports_hf_timestd():
    offenders = []
    for path in sorted(SRC.rglob("*.py")):
        for i, line in enumerate(path.read_text().split("\n"), 1):
            if PATTERN.match(line):
                offenders.append(f"{path.relative_to(SRC)}:{i}: {line.strip()}")
    assert not offenders, (
        "hamsci-physics must not import hf_timestd:\n  " + "\n  ".join(offenders))
