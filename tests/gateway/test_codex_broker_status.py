import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from agent.codex_broker import BrokerStatus, CodexBrokerLeaseManager
from gateway.run import GatewayRunner


def test_broker_status_uses_cached_session_route() -> None:
    runner: Any = object.__new__(GatewayRunner)
    runner._running_agents = {}
    runner._agent_cache = {}
    runner._agent_cache_lock = __import__("threading").Lock()
    runner.session_store = MagicMock()
    runner._async_session_store = MagicMock(_store=runner.session_store)
    runner._async_session_store.get_or_create_session = AsyncMock(
        return_value=SimpleNamespace(
            session_key="session-key", session_id="session-id"
        )
    )
    broker = MagicMock(spec=CodexBrokerLeaseManager)
    status = BrokerStatus("Primary", 80, 60, None, None)
    broker.status_for_session.return_value = status
    broker.format_status.return_value = "Primary · 5h 80% · week 60%"
    runner._agent_cache["session-key"] = (SimpleNamespace(_codex_broker=broker), "signature")

    event: Any = SimpleNamespace(source=SimpleNamespace(), get_command_args=lambda: "")
    result = asyncio.run(runner._handle_broker_status_command(event))

    assert "Status: Primary · 5h 80% · week 60%" in result
    assert "/broker-status set" in result
    broker.status_for_session.assert_called_once_with("session-id")


def test_broker_settings_require_configured_admin(monkeypatch) -> None:
    runner: Any = object.__new__(GatewayRunner)
    runner.config = MagicMock()
    runner._running_agents = {}
    runner._agent_cache = {}
    runner._agent_cache_lock = __import__("threading").Lock()
    runner.session_store = MagicMock()
    runner._async_session_store = MagicMock(_store=runner.session_store)
    runner._async_session_store.get_or_create_session = AsyncMock(
        return_value=SimpleNamespace(session_key="session-key", session_id="session-id")
    )
    policy = SimpleNamespace(enabled=False, is_admin=lambda _user_id: True)
    monkeypatch.setattr("gateway.slash_access.policy_for_source", lambda *_args: policy)
    event: Any = SimpleNamespace(
        source=SimpleNamespace(user_id="user", platform="discord"),
        get_command_args=lambda: "set url https://broker.test",
    )

    result = asyncio.run(runner._handle_broker_status_command(event))

    assert result == "Configure a gateway administrator before changing Codex Broker settings."
