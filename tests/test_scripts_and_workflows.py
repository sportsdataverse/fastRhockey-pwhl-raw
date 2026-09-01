"""Gate the two failure modes bash -n cannot catch on GitHub runners.

CRLF line endings in a .sh fail the runner's syntax stage while Git Bash's
local `bash -n` passes (this reddened a repo's main for days); and workflow
YAML that no longer parses ships silently. The R scraper itself has no python
surface to test -- these gates cover what actually breaks.
"""
import shutil
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = sorted((ROOT / "scripts").glob("*.sh"))
WORKFLOWS = sorted((ROOT / ".github" / "workflows").glob("*.yml"))


def test_scripts_exist():
    assert SCRIPTS, "scripts/ has no .sh files"


def test_no_crlf_in_shell_scripts():
    bad = [s.name for s in SCRIPTS if b"\r\n" in s.read_bytes()]
    assert not bad, f"CRLF line endings (runner bash rejects them): {bad}"


def test_bash_syntax():
    bash = shutil.which("bash")
    if bash is None:
        import pytest
        pytest.skip("bash not on PATH")
    for s in SCRIPTS:
        proc = subprocess.run([bash, "-n", str(s)], capture_output=True, text=True)
        assert proc.returncode == 0, f"{s.name}: {proc.stderr}"


def test_workflow_yaml_parses():
    assert WORKFLOWS, ".github/workflows has no .yml files"
    for w in WORKFLOWS:
        yaml.safe_load(w.read_text(encoding="utf-8"))
