"""`.env.example` is the reference users copy; keep it honest.

Settings uses ``extra="ignore"``, so a variable named wrongly in the example file
would be silently discarded rather than rejected -- the worst failure mode for a
config file. This test makes the drift a build failure instead.
"""

from __future__ import annotations

import re
from pathlib import Path

from pgmcp.config import Settings

ENV_EXAMPLE = Path(__file__).resolve().parent.parent / ".env.example"

# Documented in the example but not fields on Settings; consumed by the tests.
TEST_ONLY = {"PGMCP_TEST_DSN", "PGMCP_WRITE_TEST_DSN"}


def documented_names() -> set[str]:
    text = ENV_EXAMPLE.read_text()
    return set(re.findall(r"^#?\s*(PGMCP_[A-Z0-9_]+)=", text, flags=re.MULTILINE))


def test_env_example_exists() -> None:
    assert ENV_EXAMPLE.is_file(), ".env.example is part of the published repository"


def test_every_documented_variable_is_a_real_setting() -> None:
    valid = {f"PGMCP_{name.upper()}" for name in Settings.model_fields} | TEST_ONLY
    invented = documented_names() - valid
    assert not invented, (
        f".env.example documents variables that Settings does not read: {sorted(invented)}. "
        "They would be silently ignored."
    )


def test_every_setting_is_documented() -> None:
    documented = documented_names()
    undocumented = sorted(
        f"PGMCP_{name.upper()}" for name in Settings.model_fields if f"PGMCP_{name.upper()}" not in documented
    )
    assert not undocumented, f"settings missing from .env.example: {undocumented}"


def test_documented_values_are_valid() -> None:
    """Uncommented defaults in the file must actually load."""
    text = ENV_EXAMPLE.read_text()
    values = dict(re.findall(r"^(PGMCP_[A-Z0-9_]+)=(.*)$", text, flags=re.MULTILINE))
    kwargs = {
        name[len("PGMCP_") :].lower(): value
        for name, value in values.items()
        if value != "" and name not in TEST_ONLY
    }
    settings = Settings(**kwargs)
    assert settings.access_mode.value == "read-only", "the example must default to read-only"
