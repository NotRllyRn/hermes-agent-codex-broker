from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from agent.codex_broker import BrokerStatus, CodexBrokerLeaseManager
from gateway.run import GatewayRunner


@pytest.mark.asyncio
async def test_broker_status_uses_cached_session_route() -> None:
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

    event: Any = SimpleNamespace(source=SimpleNamespace())
    result = await runner._handle_broker_status_command(event)

    assert result == "Codex Broker account: Primary · 5h 80% · week 60%"
    broker.status_for_session.assert_called_once_with("session-id")
