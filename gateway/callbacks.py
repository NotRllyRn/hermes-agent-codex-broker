"""Profile-local durable admission for trusted internal callbacks (not delivery ACKs)."""
from __future__ import annotations

import asyncio
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from gateway.platforms.event import MessageEvent, MessageType
from gateway.session import SessionSource
from gateway.wake import WakeNotAccepted


class CallbackLedger:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        with self.connect() as db:
            db.execute('CREATE TABLE IF NOT EXISTS callbacks (id TEXT PRIMARY KEY, payload TEXT NOT NULL, status TEXT NOT NULL)')

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path)
        try:
            db.execute('PRAGMA synchronous=FULL')
            with db:
                yield db
        finally:
            db.close()

    def admit(self, callback_id, payload):
        with self.connect() as db:
            db.execute('INSERT OR IGNORE INTO callbacks VALUES (?, ?, ?)', (callback_id, payload, 'queued'))
            old_payload, status = db.execute('SELECT payload, status FROM callbacks WHERE id=?', (callback_id,)).fetchone()
            if old_payload != payload:
                raise ValueError('callback_id already belongs to a different payload')
            return status

    def receipt(self, callback_id):
        with self.connect() as db:
            row = db.execute('SELECT status FROM callbacks WHERE id=?', (callback_id,)).fetchone()
            return {'callback_id': callback_id, 'status': row[0]} if row else None

    def set_status(self, callback_id, status):
        with self.connect() as db:
            db.execute('UPDATE callbacks SET status=? WHERE id=?', (status, callback_id))

    def recover(self):
        with self.connect() as db:
            db.execute("UPDATE callbacks SET status='uncertain' WHERE status='running'")

    def pending(self):
        with self.connect() as db:
            return db.execute("SELECT id, payload FROM callbacks WHERE status='queued' ORDER BY rowid").fetchall()


class GatewayCallbacksMixin:
    async def _handle_message(self, event: MessageEvent):
        callback_id = getattr(event, "_callback_id", None)
        if callback_id is None:
            return await self._handle_message_inner(event)
        ledger = self._callback_ledger_for_runner()
        status = "rejected"
        def finish(delivered):
            if ledger.receipt(callback_id)['status'] != 'running':
                return
            outcome = "completed" if delivered and getattr(event, "_callback_processed", False) else "uncertain"
            ledger.set_status(callback_id, outcome)
            done = getattr(event, "_callback_done", None)
            if done is not None and not done.done():
                done.set_result(outcome)
        event._callback_finish = finish
        try:
            await self._validate_callback(event)
            ledger.set_status(callback_id, "running")
            status = "uncertain"
            result = await self._handle_message_inner(event)
            status = ("running" if getattr(event, "_callback_processed", False) else
                      "uncertain" if getattr(event, "_callback_execution_started", False) else "rejected")
            return result
        finally:
            ledger.set_status(callback_id, status)
            done = getattr(event, "_callback_done", None)
            if status != "running" and done is not None and not done.done():
                done.set_result(status)


    def _callback_ledger_for_runner(self):
        ledger = getattr(self, '_callback_ledger', None)
        if ledger is None:
            # SessionStore is constructed with the runner's profile-local sessions directory.
            ledger = self._callback_ledger = CallbackLedger(Path(self.session_store.sessions_dir).parent / 'gateway_callbacks.db')
        return ledger

    async def _validate_callback(self, event):

        if not isinstance(event, MessageEvent) or event.internal is not True or event.allow_gateway_control is not False:
            raise ValueError('callback requires internal=True, allow_gateway_control=False')
        allowed = {'gateway_session_key', 'gateway_session_id', 'gateway_session_strict', 'notification_category', 'gateway_transport_profile'}
        if set(event.metadata) - allowed or event.metadata.get('gateway_session_strict') is not True:
            raise ValueError('callback requires strict routing metadata only')
        if event.metadata.get('notification_category', 'result') not in {'result', 'diagnostic'}:
            raise ValueError('invalid callback notification category')
        key = event.metadata.get('gateway_session_key')
        sid = event.metadata.get('gateway_session_id')
        if not isinstance(key, str) or not key or not isinstance(sid, str) or not sid:
            raise ValueError('callback requires exact session key and id')
        if event.message_type != MessageType.TEXT or not isinstance(event.text, str) or not event.text:
            raise ValueError('callback requires nonempty text')
        if event.user_id or event.user_name or event.message_id or event.raw_message or event.media_urls or event.prompt_response:
            raise ValueError('callback cannot carry human message identity or attachments')
        source = event.source
        if not isinstance(source, SessionSource) or source.profile_route_rejected:
            raise ValueError('callback requires an original SessionSource')
        entry = await self.async_session_store.lookup_by_session_key(key)
        if entry is None or entry.session_id != sid or entry.origin is None or entry.origin.to_dict() != source.to_dict():
            raise ValueError('callback original session id/source/profile is no longer current')
        multiplexed = getattr(getattr(self, 'config', None), 'multiplex_profiles', False)
        expected_transport = event.metadata.get('gateway_transport_profile')
        if expected_transport is not None and expected_transport != (getattr(entry, 'transport_profile', None) or 'default'):
            raise ValueError('callback original receiving bot has changed')
        if multiplexed:
            transport = getattr(entry, 'transport_profile', None)
            if transport is None:
                raise ValueError('callback requires persisted transport identity under multiplexing')
            if hasattr(event, '_callback_transport_profile') and event._callback_transport_profile != transport:
                raise ValueError('callback original receiving bot has changed')
            event._callback_transport_profile = transport
            event.source = source = self._restored_source(entry)
        if self._session_key_for_source(source) != key:
            raise ValueError('callback source does not derive the original session key')
        adapter = self._intake_adapter_for(source)
        from gateway.wake import adapter_supports_push
        if adapter is None:
            raise WakeNotAccepted('callback original adapter is unavailable')
        if not adapter_supports_push(adapter):
            raise ValueError('callback requires a push-capable original adapter')
        return adapter

    async def admit_callback(self, callback_id: str, event: MessageEvent) -> dict:
        """Durably queue a trusted callback; receipt is admission, NOT completion/delivery."""
        if not isinstance(callback_id, str) or not callback_id or len(callback_id) > 256:
            raise ValueError('callback_id must be a nonempty string of at most 256 characters')
        await self._validate_callback(event)
        payload = json.dumps({'text': event.text, 'source': event.source.to_dict(), 'metadata': event.metadata,
                              'transport_profile': getattr(event, '_callback_transport_profile', None)}, sort_keys=True, separators=(',', ':'))
        ledger = self._callback_ledger_for_runner()
        status = ledger.admit(callback_id, payload)
        self._ensure_callback_dispatcher()
        return {'callback_id': callback_id, 'status': status}

    async def get_callback_receipt(self, callback_id: str):
        """Read the durable admission/turn outcome, or None for an unknown id."""
        return self._callback_ledger_for_runner().receipt(callback_id)

    def _ensure_callback_dispatcher(self):
        task = getattr(self, '_callback_dispatcher', None)
        if task is None or task.done():
            self._callback_dispatcher = asyncio.create_task(self._dispatch_callbacks())
            self._retain_background_task(self._callback_dispatcher)

    async def _start_callback_recovery(self):
        self._callback_ledger_for_runner().recover()
        self._ensure_callback_dispatcher()
        from hermes_cli.lifecycle import ainvoke_hook
        await ainvoke_hook('gateway_ready', gateway=self, session_store=self.session_store)

    async def _dispatch_callbacks(self):
        ledger = self._callback_ledger_for_runner()
        inflight = {}
        while True:
            inflight = {key: done for key, done in inflight.items() if not done.done()}
            rows = ledger.pending()
            if not rows and not inflight:
                return
            for callback_id, payload in rows:
                if callback_id in inflight:
                    continue
                done = await self._dispatch_callback(callback_id, payload)
                if done is not None:
                    inflight[callback_id] = done
            await asyncio.sleep(0.1)

    async def _dispatch_callback(self, callback_id, payload):
        data = json.loads(payload)
        event = MessageEvent(text=data['text'], source=SessionSource.from_dict(data['source']),
                             internal=True, allow_gateway_control=False, metadata=data['metadata'])
        event._callback_transport_profile = data.get('transport_profile')
        try:
            adapter = await self._validate_callback(event)
        except WakeNotAccepted:
            return None
        except ValueError:
            self._callback_ledger_for_runner().set_status(callback_id, 'rejected')
            return None
        key = event.metadata['gateway_session_key']
        # Keep callbacks out of the human pending slot; unrelated idle lanes still drain.
        if not adapter.callback_slot_available(key) or self._is_session_running(key):
            return None
        event._callback_id = callback_id
        event._callback_done = asyncio.get_running_loop().create_future()
        await adapter.handle_message(event)
        return event._callback_done if event._gateway_accepted else None
