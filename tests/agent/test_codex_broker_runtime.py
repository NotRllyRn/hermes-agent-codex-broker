from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def broker_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HERMES_CODEX_BROKER_URL", "https://broker.test")
    monkeypatch.setenv("HERMES_CODEX_BROKER_CLIENT_KEY", "cbk_test")
    monkeypatch.delenv("HERMES_CODEX_BROKER_CA_CERT", raising=False)


def test_runtime_resolution_bypasses_native_codex_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker_env(monkeypatch)
    from hermes_cli import runtime_provider

    monkeypatch.setattr(
        runtime_provider,
        "resolve_codex_runtime_credentials",
        lambda *_a, **_k: pytest.fail("native credentials were read"),
    )
    runtime = runtime_provider.resolve_runtime_provider(requested="openai-codex")
    assert runtime["source"] == "codex-broker"
    assert runtime["api_key"] == "broker-managed"
    assert runtime["api_mode"] == "codex_responses"


def test_agent_initializes_without_native_codex_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker_env(monkeypatch)
    from run_agent import AIAgent

    with patch("agent.process_bootstrap.OpenAI") as openai:
        openai.return_value = MagicMock()
        agent = AIAgent(
            provider="openai-codex",
            api_mode="codex_responses",
            model="gpt-5.4",
            credential_pool=MagicMock(),
            quiet_mode=True,
            skip_context_files=True,
            skip_memory=True,
        )
    assert getattr(agent, "_codex_broker", None) is not None
    assert getattr(agent, "_credential_pool", None) is None
    assert agent.api_key == "broker-managed"
    assert getattr(agent, "_client_kwargs")["api_key"] == "broker-managed"


def test_broker_mode_rejects_other_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    broker_env(monkeypatch)
    from run_agent import AIAgent

    with patch("agent.process_bootstrap.OpenAI", return_value=MagicMock()):
        with pytest.raises(RuntimeError, match="requires the openai-codex provider"):
            AIAgent(
                provider="openrouter",
                api_key="key",
                base_url="https://openrouter.ai/api/v1",
                model="model",
                quiet_mode=True,
                skip_context_files=True,
                skip_memory=True,
            )
