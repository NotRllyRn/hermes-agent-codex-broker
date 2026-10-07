import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from gateway.callbacks import CallbackLedger, GatewayCallbacksMixin
from gateway.run_inbound import GatewayInboundMixin
from gateway.platforms.event import MessageEvent
from gateway.session import SessionSource
from gateway.config import Platform


def test_durable_dedupe_and_conflict(tmp_path):
    path = tmp_path / 'callbacks.db'
    ledger = CallbackLedger(path)
    assert ledger.admit('one', '{"text":"x"}') == 'queued'
    ledger.set_status('one', 'completed')
    assert CallbackLedger(path).admit('one', '{"text":"x"}') == 'completed'
    with pytest.raises(ValueError):
        ledger.admit('one', '{"text":"y"}')


def test_recover_only_queued(tmp_path):
    path = tmp_path / 'callbacks.db'
    ledger = CallbackLedger(path)
    ledger.admit('queued', '{}')
    ledger.admit('started', '{}')
    ledger.set_status('started', 'running')
    recovered = CallbackLedger(path)
    recovered.recover()
    assert recovered.pending() == [('queued', '{}')]
    assert recovered.admit('started', '{}') == 'uncertain'


@pytest.mark.asyncio
async def test_ready_hook_and_no_generic_resume_marker(tmp_path, monkeypatch):
    from hermes_cli import lifecycle
    hook = AsyncMock(return_value=[])
    monkeypatch.setattr(lifecycle, 'ainvoke_hook', hook)
    runner = Runner(tmp_path)
    await runner._start_callback_recovery()
    await runner._callback_dispatcher
    hook.assert_awaited_once_with('gateway_ready', gateway=runner, session_store=runner.session_store)
    event = runner.event()
    event._callback_id = 'one'
    runner.async_session_store.mark_turn_active = AsyncMock()
    assert await runner._mark_durable_active_turn(event, 'key') is False
    runner.async_session_store.mark_turn_active.assert_not_awaited()




class Runner(GatewayCallbacksMixin, GatewayInboundMixin):
    def __init__(self, home):
        self.session_store = SimpleNamespace(sessions_dir=home / 'sessions')
        self.source = SessionSource(platform=Platform.TELEGRAM, chat_id='123', user_id='456', profile='default')
        self.entry = SimpleNamespace(session_id='original', origin=self.source)
        self.async_session_store = SimpleNamespace(lookup_by_session_key=AsyncMock(side_effect=lambda key: self.entry))
        self.active = True
        self.seen = []
        self.tasks = []
        self.adapter = SimpleNamespace(supports_async_delivery=True, callback_slot_available=lambda key: not self.active,
                                       handle_message=self.deliver)

    def _session_key_for_source(self, source):
        return 'key' if source.to_dict() == self.source.to_dict() else 'wrong'

    def _intake_adapter_for(self, source):
        return self.adapter

    def _is_session_running(self, key):
        return self.active

    def _retain_background_task(self, task):
        self.tasks.append(task)

    async def deliver(self, event):
        event._gateway_accepted = True
        await self._handle_message(event)

    async def _handle_message_inner(self, event):
        self.seen.append(event.text)
        event._callback_processed = True
        return 'answer'

    def event(self):
        return MessageEvent(text='completion', source=self.source, internal=True, allow_gateway_control=False,
                            metadata={'gateway_session_key': 'key', 'gateway_session_id': 'original',
                                      'gateway_session_strict': True})


@pytest.mark.asyncio
async def test_busy_then_complete_and_duplicate(tmp_path):
    runner = Runner(tmp_path)
    assert (await runner.admit_callback('one', runner.event()))['status'] == 'queued'
    await asyncio.sleep(0.02)
    assert runner.seen == []
    runner.active = False
    await runner._callback_dispatcher
    assert runner.seen == ['completion']
    assert (await runner.admit_callback('one', runner.event()))['status'] == 'completed'
    await runner._callback_dispatcher
    assert runner.seen == ['completion']
    assert (await runner.get_callback_receipt('one'))['status'] == 'completed'
    assert await runner.get_callback_receipt('missing') is None


@pytest.mark.asyncio
async def test_cold_start_recovers_and_reset_refuses(tmp_path):
    old = Runner(tmp_path)
    await old.admit_callback('one', old.event())
    old._callback_dispatcher.cancel()
    await asyncio.gather(old._callback_dispatcher, return_exceptions=True)
    new = Runner(tmp_path)
    new.active = False
    await new._start_callback_recovery()
    await new._callback_dispatcher
    assert new.seen == ['completion']
    new.entry.session_id = 'after-new'
    with pytest.raises(ValueError):
        await new.admit_callback('stale', new.event())


@pytest.mark.asyncio
async def test_reset_while_queued_rejected(tmp_path):
    runner = Runner(tmp_path)
    await runner.admit_callback('one', runner.event())
    runner.entry.session_id = 'after-new'
    runner.active = False
    await runner._callback_dispatcher
    assert runner.seen == []
    assert runner._callback_ledger.pending() == []
    with runner._callback_ledger.connect() as db:
        assert db.execute('SELECT status FROM callbacks WHERE id=?', ('one',)).fetchone()[0] == 'rejected'


@pytest.mark.asyncio
async def test_turn_exception_is_uncertain_not_requeued(tmp_path):
    runner = Runner(tmp_path)
    runner.active = False
    runner._handle_message_inner = AsyncMock(side_effect=RuntimeError('turn failed'))
    await runner.admit_callback('one', runner.event())
    with pytest.raises(RuntimeError):
        await runner._callback_dispatcher
    assert (await runner.get_callback_receipt('one'))['status'] == 'uncertain'
    await runner.admit_callback('one', runner.event())
    await runner._callback_dispatcher
    runner._handle_message_inner.assert_awaited_once()


@pytest.mark.asyncio
async def test_shutdown_does_not_mark_callback_for_auto_resume(tmp_path):
    from gateway.run_shutdown import GatewayShutdownMixin
    runner = Runner(tmp_path)
    runner._restart_requested = False
    runner._running_agents = {'key': object()}
    event = runner.event()
    event._callback_id = 'one'
    runner._peek_session_state = lambda key: SimpleNamespace(turn=SimpleNamespace(event=event))
    runner.async_session_store.mark_resume_pending = AsyncMock()
    assert await GatewayShutdownMixin._mark_running_sessions_resume_pending(runner, 'test') == []
    runner.async_session_store.mark_resume_pending.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['profile', 'strict', 'human', 'control', 'metadata'])
async def test_invalid_callbacks(tmp_path, change):
    runner = Runner(tmp_path)
    event = runner.event()
    if change == 'profile':
        event.source = SessionSource(platform=Platform.TELEGRAM, chat_id='123', user_id='456', profile='other')
    elif change == 'strict':
        event.metadata['gateway_session_strict'] = 1
    elif change == 'human':
        event.user_id = '456'
    elif change == 'control':
        event.allow_gateway_control = True
    else:
        event.metadata['untrusted'] = True
    with pytest.raises(ValueError):
        await runner.admit_callback('one', event)

