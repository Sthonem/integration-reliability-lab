"""The Alembic migration builds exactly the schema the models describe (no drift)."""

from pathlib import Path

from alembic import command
from alembic.config import Config

from tests.conftest import TEST_DATABASE_URL

ROOT = Path(__file__).resolve().parents[1]


def _config(url: str) -> Config:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", url)
    cfg.attributes["configure_logger"] = False
    return cfg


def test_upgrade_matches_models_and_downgrade_is_clean(tmp_path):
    cfg = _config(TEST_DATABASE_URL or f"sqlite+aiosqlite:///{tmp_path / 'migrated.db'}")
    command.upgrade(cfg, "head")
    command.check(cfg)  # raises if the models and the migrated schema differ
    command.downgrade(cfg, "base")
    command.upgrade(cfg, "head")
