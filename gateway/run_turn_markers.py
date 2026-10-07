"""Durable active-turn marker ownership and cleanup."""
from __future__ import annotations

import logging
from contextlib import suppress
from typing import Optional

from gateway.platforms.event import MessageEvent

logger = logging.getLogger(__name__)


class GatewayTurnMarkersMixin:
    async def _mark_durable_active_turn(self, event: "MessageEvent", session_key: str) -> bool:
        """Persist the exact resolved routing key for this running turn."""
        # Callback recovery is owned by its ledger: generic auto-resume would replay
        # an uncertain callback outside its idempotency boundary.
        if getattr(event, "_callback_id", None) is not None:
            return False
        try:
            token = await self.async_session_store.mark_turn_active(session_key)
        except Exception as exc:
            logger.warning("Could not persist active-turn marker for %s: %s", session_key, exc, exc_info=True)
            return False
        if not token:
            return False
        # Private event attributes are process-local ownership state: keep the token out of public
        # metadata, transcripts, and platform payloads.
        event._gateway_active_turn_session_key = session_key
        event._gateway_active_turn_token = token
        return True


    async def _clear_durable_active_turn(self, event: "MessageEvent") -> bool:
        """Best-effort CAS clear of the marker owned by *event* (3 attempts; never blocks agent/lease
        release — a stale marker is bounded by the agent timeout and clean-start discard)."""
        session_key = getattr(event, "_gateway_active_turn_session_key", None)
        token = getattr(event, "_gateway_active_turn_token", None)
        try:
            if not session_key or not token:
                return False
            last_error: Optional[Exception] = None
            for attempt in range(1, 4):
                try:
                    return bool(await self.async_session_store.clear_turn_active(session_key, token))
                except Exception as exc:
                    last_error = exc
                    if attempt < 3:
                        logger.debug(
                            "Retrying active-turn marker cleanup for %s (%d/3): %s",
                            session_key, attempt, exc,
                        )
            logger.warning(
                "Could not clear active-turn marker for %s after 3 attempts: %s", session_key, last_error,
            )
            return False
        finally:
            for attr in ("_gateway_active_turn_session_key", "_gateway_active_turn_token"):
                with suppress(AttributeError):
                    delattr(event, attr)

