"""Durable callback outcome includes the real adapter's outbound boundary."""
import asyncio

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from tests.gateway.test_durable_callbacks import Runner


class Adapter(BasePlatformAdapter):
    async def connect(self, *, is_reconnect=False):
        return True

    async def disconnect(self):
        return None

    async def get_chat_info(self, chat_id):
        return {'id': chat_id}

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        self.sending.set()
        await self.release.wait()
        return SendResult(success=self.success, message_id='outbound' if self.success else None)


@pytest.mark.asyncio
@pytest.mark.parametrize('success', [True, False])
async def test_callback_waits_for_adapter_outcome(tmp_path, success):
    runner = Runner(tmp_path)
    adapter = Adapter(PlatformConfig(enabled=True), Platform.TELEGRAM)
    adapter.sending = asyncio.Event()
    adapter.release = asyncio.Event()
    adapter.success = success
    adapter._message_handler = runner._handle_message
    # No transport retry delay in this unit test; the real delivery pipeline still runs.
    adapter._send_with_retry = adapter.send
    event = runner.event()
    event._callback_id = 'one'
    event._callback_done = asyncio.get_running_loop().create_future()
    runner._callback_ledger_for_runner().admit('one', '{}')
    task = asyncio.create_task(adapter._process_message_background(event, 'key'))
    await asyncio.wait_for(adapter.sending.wait(), 5)
    assert (await runner.get_callback_receipt('one'))['status'] == 'running'
    assert not event._callback_done.done()
    adapter.release.set()
    await asyncio.wait_for(task, 5)
    assert (await runner.get_callback_receipt('one'))['status'] == ('completed' if success else 'uncertain')
    assert event._callback_done.done()
