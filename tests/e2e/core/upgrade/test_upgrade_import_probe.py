"""Exercise the upgrade source probe against real miniature git installs."""
import os
import subprocess
import sys

import pytest

from tests.e2e.core.upgrade.test_upgrade_path import IMPORT_PROBE


def _probe(tmp_path, *, failure="import acp", core=False, sdk=False, required=False, healthy=False):
    def write(name, text=""):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    deps = '["agent-client-protocol>=1,<2"]' if core else '[]'
    write("pyproject.toml", f'''[project]
dependencies = {deps}
[project.optional-dependencies]
acp = ["agent-client-protocol>=1,<2"]
[tool.setuptools.packages.find]
include = ["acp_adapter", "hermes_cli"]
''')
    for name in ("acp_adapter/__init__.py", "hermes_cli/__init__.py", "hermes_cli/main.py",
                 "run_agent.py", "hermes_state.py"):
        write(name)
    def git(*args):
        return subprocess.run(["git", *args], cwd=tmp_path, check=True, capture_output=True, text=True)
    git("init", "-q")
    git("add", ".")
    git("-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-qm", "base")
    base = git("rev-parse", "HEAD").stdout.strip()
    for name in ("commands", "content", "model_catalog"):
        write(f"acp_adapter/{name}.py", failure)
    # A missing first-party module beyond the three optional misses must still fail.
    write("hermes_cli/new_core.py", "print('sample core 1')" if healthy else "import hermes_missing_firstparty")
    if healthy:
        write("hermes_cli/new_core2.py", "print('sample core 2')")
        write("hermes_cli/new_core3.py", "print('sample core 3')")
    if sdk:
        write("acp.py")
    if required:
        write("hermes_cli/main.py", "import acp")
    git("add", ".")
    git("-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-qm", "head")
    # Block any host SDK without faking first-party imports.
    blocker = '''import sys
class BlockACP:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "acp":
            raise ModuleNotFoundError("No module named 'acp'", name="acp")
sys.meta_path.insert(0, BlockACP())
'''
    code = ("" if sdk else blocker) + IMPORT_PROBE
    return subprocess.run([sys.executable, "-c", code, str(tmp_path), base], cwd=tmp_path,
                          env={**os.environ, "PYTHONPATH": str(tmp_path)}, capture_output=True, text=True)


@pytest.mark.parametrize("sdk", [False, True])
def test_healthy_probe_with_optional_sdk_absent_or_present(tmp_path, sdk):
    cp = _probe(tmp_path, sdk=sdk, healthy=True)
    assert cp.returncode == 0, cp.stderr
    if not sdk:
        assert cp.stdout.count("optional ACP SDK absent") == 3
        assert all(f"sample core {n}" in cp.stdout for n in (1, 2, 3))
    else:
        assert "optional ACP SDK absent" not in cp.stdout


def test_optional_acp_misses_continue_to_firstparty_added_module(tmp_path):
    cp = _probe(tmp_path)
    assert cp.returncode != 0
    assert cp.stdout.count("optional ACP SDK absent") == 3
    assert "hermes_cli.new_core: ModuleNotFoundError" in cp.stderr
    assert "acp_adapter.commands:" not in cp.stderr


@pytest.mark.parametrize("kwargs, expected", [
    ({"core": True}, "acp_adapter.commands: ModuleNotFoundError"),
    ({"failure": "import acp.missing", "sdk": True}, "acp_adapter.commands: ModuleNotFoundError"),
    ({"failure": "import hermes_missing_firstparty"}, "acp_adapter.commands: ModuleNotFoundError"),
    ({"failure": "raise ImportError('broken symbol')"}, "acp_adapter.commands: ImportError"),
    ({"failure": "raise NameError('broken name')"}, "acp_adapter.commands: NameError"),
    ({"required": True}, "hermes_cli.main: ModuleNotFoundError"),
])
def test_probe_does_not_tolerate_core_or_broken_imports(tmp_path, kwargs, expected):
    cp = _probe(tmp_path, **kwargs)
    assert cp.returncode != 0
    assert expected in cp.stderr
