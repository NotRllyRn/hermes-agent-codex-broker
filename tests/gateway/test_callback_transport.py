"""Callback admission restores the actual receiving bot, not the runtime bot."""
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.run import GatewayRunner
from gateway.config import Platform
from gateway.platforms.event import MessageEvent
from gateway.session import SessionSource
from gateway.session_identity import identity_of


class Runner(GatewayRunner):
    def _session_key_for_source(self, source):
        return 'agent:main:discord:room'


@pytest.mark.asyncio
async def test_callback_uses_persisted_receiving_bot(tmp_path):
    runner = object.__new__(Runner)
    runner.config = SimpleNamespace(multiplex_profiles=True)
    receiver = SimpleNamespace(supports_async_delivery=True)
    wrong_bot = SimpleNamespace(supports_async_delivery=True)
    runner.adapters = {Platform.DISCORD: wrong_bot}
    runner._profile_adapters = {'maya': {Platform.DISCORD: receiver}}
    source = SessionSource(platform=Platform.DISCORD, chat_id='room', profile='default')
    entry = SimpleNamespace(session_id='original', origin=source, transport_profile='maya')
    runner.session_store = SimpleNamespace()
    runner._async_session_store = SimpleNamespace(_store=runner.session_store,
                                                lookup_by_session_key=AsyncMock(return_value=entry))
    event = MessageEvent(text='peer result', source=source, internal=True, allow_gateway_control=False,
                         metadata={'gateway_session_key': 'agent:main:discord:room',
                                   'gateway_session_id': 'original', 'gateway_session_strict': True})
    assert await runner._validate_callback(event) is receiver
    assert identity_of(event.source).transport_profile == 'maya'
    runner._profile_adapters['maya'] = {}
    from gateway.wake import WakeNotAccepted
    with pytest.raises(WakeNotAccepted):
        await runner._validate_callback(event)
