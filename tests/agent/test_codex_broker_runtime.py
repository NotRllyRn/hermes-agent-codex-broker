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


def test_auxiliary_codex_never_reads_native_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker_env(monkeypatch)
    from agent import auxiliary_client

    monkeypatch.setattr(
        auxiliary_client,
        "_select_pool_entry",
        lambda *_a, **_k: pytest.fail("native credential pool was read"),
    )
    monkeypatch.setattr(
        auxiliary_client,
        "_read_codex_access_token",
        lambda: pytest.fail("native token file was read"),
    )
    assert auxiliary_client._build_codex_client("gpt-5.4") == (None, None)
    assert auxiliary_client.resolve_provider_client(
        "openai-codex", model="gpt-5.4", raw_codex=True
    ) == (None, None)


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
