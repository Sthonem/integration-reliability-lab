import stat
from pathlib import Path

from irlab.config import Settings
from irlab.devtools import PLACEHOLDER, init_env

EXAMPLE = Path(__file__).resolve().parents[1] / ".env.example"


def test_init_env_generates_distinct_secrets_with_private_permissions(tmp_path, capsys):
    target = tmp_path / ".env"
    assert init_env(EXAMPLE, target) == 0

    content = target.read_text()
    assert PLACEHOLDER not in content
    assert stat.S_IMODE(target.stat().st_mode) == 0o600

    settings = Settings(_env_file=target)
    found = [settings.webhook_secrets(s) for s in ("pos", "payments", "loyalty")]
    assert all(len(s) == 1 and len(s[0]) >= 32 for s in found)
    assert len({s[0] for s in found}) == 3
    for s in found:
        assert s[0] not in capsys.readouterr().out


def test_init_env_refuses_to_overwrite(tmp_path):
    target = tmp_path / ".env"
    target.write_text("KEEP=1\n")
    assert init_env(EXAMPLE, target) == 1
    assert target.read_text() == "KEEP=1\n"
    assert init_env(EXAMPLE, target, force=True) == 0


def test_example_file_contains_no_secrets():
    for line in EXAMPLE.read_text().splitlines():
        if "_SECRETS=" in line and not line.startswith("#"):
            assert line.split("=", 1)[1] == PLACEHOLDER
