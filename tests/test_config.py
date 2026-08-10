from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from x_virality.config import RulesConfig, Settings, load_rules


def test_example_rules_are_valid() -> None:
    path = Path(__file__).parents[1] / "config" / "rules.example.yml"

    rules = load_rules(path)

    assert rules.version == 1
    assert sum(rule.enabled for rule in rules.rules) == 3
    assert rules.scoring.post_weights.velocity_percentile == Decimal("0.30")
    assert len(rules.canonical_hash()) == 64
    assert rules.managed_tag("xv", rules.rules[0]).startswith("xv:production:niche-ai-terms:")


def test_rule_graph_rejects_niche_mismatch() -> None:
    with pytest.raises(ValidationError, match="niche differs"):
        RulesConfig.model_validate(
            {
                "version": 1,
                "environment": "test",
                "topics": [{"key": "ai", "niche": "software", "label": "AI"}],
                "rules": [
                    {
                        "key": "ai",
                        "topic": "ai",
                        "niche": "hardware",
                        "query": "AI lang:en",
                    }
                ],
            }
        )


def test_rule_rejects_negative_only_query() -> None:
    with pytest.raises(ValidationError, match="positive term"):
        RulesConfig.model_validate(
            {
                "version": 1,
                "environment": "test",
                "topics": [{"key": "ai", "niche": "software", "label": "AI"}],
                "rules": [
                    {
                        "key": "ai",
                        "topic": "ai",
                        "niche": "software",
                        "query": "-is:retweet -lang:ja",
                    }
                ],
            }
        )


def test_settings_require_role_specific_secrets(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        rules_path=tmp_path / "missing.yml",
        x_bearer_token=None,
    )

    with pytest.raises(ValueError, match="XV_X_BEARER_TOKEN"):
        settings.validate_role("stream")


def test_settings_reject_non_postgresql_database() -> None:
    with pytest.raises(ValidationError, match="PostgreSQL"):
        Settings(_env_file=None, database_url="sqlite:///tmp.db")


def test_settings_validate_discord_security_values() -> None:
    with pytest.raises(ValidationError, match="64-character hexadecimal"):
        Settings(_env_file=None, discord_public_key="not-a-key")
    with pytest.raises(ValidationError, match="at least 32"):
        Settings(_env_file=None, admin_token="short")
    with pytest.raises(ValidationError, match="5-32 digits"):
        Settings(_env_file=None, discord_application_id="not-a-snowflake")
