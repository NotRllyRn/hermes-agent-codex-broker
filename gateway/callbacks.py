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
    def _callback_ledger_for_runner(self):
        ledger = getattr(self, '_callback_ledger', None)
        if ledger is None:
            # SessionStore is constructed with the runner's profile-local sessions directory.
            ledger = self._callback_ledger = CallbackLedger(Path(self.session_store.sessions_dir).parent / 'gateway_callbacks.db')
        return ledger

    async def _validate_callback(self, event):
        if getattr(getattr(self, 'config', None), 'multiplex_profiles', False):
            raise ValueError('durable callbacks do not yet support multiplex profiles')
        if not isinstance(event, MessageEvent) or event.internal is not True or event.allow_gateway_control is not False:
            raise ValueError('callback requires internal=True, allow_gateway_control=False')
        allowed = {'gateway_session_key', 'gateway_session_id', 'gateway_session_strict', 'notification_category'}
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
        if self._session_key_for_source(source) != key:
            raise ValueError('callback source does not derive the original session key')
        entry = await self.async_session_store.lookup_by_session_key(key)
        if entry is None or entry.session_id != sid or entry.origin is None or entry.origin.to_dict() != source.to_dict():
            raise ValueError('callback original session id/source/profile is no longer current')
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
        payload = json.dumps({'text': event.text, 'source': event.source.to_dict(), 'metadata': event.metadata}, sort_keys=True, separators=(',', ':'))
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
        while True:
            rows = ledger.pending()
            if not rows:
                return
            callback_id, payload = rows[0]
            data = json.loads(payload)
            event = MessageEvent(text=data['text'], source=SessionSource.from_dict(data['source']),
                                 internal=True, allow_gateway_control=False, metadata=data['metadata'])
            try:
                adapter = await self._validate_callback(event)
            except WakeNotAccepted:
                await asyncio.sleep(1)
                continue
            except ValueError:
                ledger.set_status(callback_id, 'rejected')
                continue
            key = event.metadata['gateway_session_key']
            # Do not occupy/merge the adapter's human pending slot. Claim via the ordinary
            # adapter boundary only after the original turn and its outbound cleanup finish.
            if not adapter.callback_slot_available(key) or self._is_session_running(key):
                await asyncio.sleep(0.1)
                continue
            event._callback_id = callback_id
            event._callback_done = asyncio.get_running_loop().create_future()
            await adapter.handle_message(event)
            if not event._gateway_accepted:
                await asyncio.sleep(0.1)
                continue
            await event._callback_done
