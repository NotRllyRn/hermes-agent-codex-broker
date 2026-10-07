"""Exact-session resolution and topic recovery before turn preparation."""
import asyncio
import dataclasses
import logging
from contextlib import suppress

logger = logging.getLogger(__name__)


class GatewayTurnSessionMixin:
    async def _hmwa_resolve_session(self, event, source):
        """Resolve ``source`` to its session entry (topic recovery, internal-route guards, Telegram
        topic-binding heal). Returns ``(source, session_entry, session_key)`` or ``None`` to drop
        the event."""
        # Topic-mode DMs: rewrite a stale/foreign thread_id to the user's last-active topic so a
        # cross-topic Reply doesn't fragment the conversation.
        event_metadata = getattr(event, "metadata", None) or {}
        expected_session_key = str(event_metadata.get("gateway_session_key") or "").strip()
        recovered = (await asyncio.to_thread(self._recover_telegram_topic_thread_id, source)
                     if not expected_session_key else None)
        if recovered is not None:
            logger.info(
                "telegram topic recovery: chat=%s user=%s %r -> %s",
                source.chat_id, source.user_id, source.thread_id, recovered,
            )
            source = dataclasses.replace(source, thread_id=recovered)
            with suppress(Exception):
                event.source = source

        if expected_session_key:
            derived_session_key = self._session_key_for_source(source)
            if derived_session_key != expected_session_key:
                logger.warning(
                    "Dropping internally routed event after route recovery: expected session=%s derived=%s",
                    expected_session_key, derived_session_key,
                )
                return

        strict_session = bool(event_metadata.get("gateway_session_strict"))
        pinned_session_id = str(event_metadata.get("gateway_session_id") or "").strip()
        if strict_session:
            session_entry = await self.async_session_store.lookup_by_session_key(expected_session_key)
            if session_entry is None or not pinned_session_id or session_entry.session_id != pinned_session_id:
                logger.warning(
                    "Dropping internally routed event: expected session id=%s is no longer current for key=%s",
                    pinned_session_id or "missing", expected_session_key or "missing",
                )
                return
        else:
            # Internal wakes observe reset policy without counting as user activity, or periodic
            # notifications keep the routing key alive across every daily/idle boundary.
            session_entry = await self.async_session_store.get_or_create_session(
                source, touch_activity=not bool(getattr(event, "internal", False)),
            )
        session_key = session_entry.session_key
        if not strict_session and pinned_session_id:
            resolved_entry = await self._resolve_async_delegation_session(session_entry, pinned_session_id)
            if resolved_entry is None:
                return
            session_entry = resolved_entry
        self._cache_session_source(session_key, source)
        if not strict_session and await asyncio.to_thread(self._is_telegram_topic_lane, source):
            session_entry = await self._hmwa_heal_telegram_topic_binding(source, session_entry, session_key)
        from gateway.run_heartbeat_acceptance import resolve_heartbeat_owner
        if not await resolve_heartbeat_owner(self, event, session_entry):
            return
        return source, session_entry, session_key
