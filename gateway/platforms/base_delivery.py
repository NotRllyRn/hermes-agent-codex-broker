"""Final text delivery ledger and active-turn marker handoff."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

from gateway.platforms.event import MessageEvent

if TYPE_CHECKING:
    from gateway.platforms.base import BasePlatformAdapter, SendResult

from hermes_cli.observability.shared_metrics_gateway import stop_reply_clock

logger = logging.getLogger(__name__)


class FinalDeliveryMixin:
    async def send_final_ledgered(
        self, event: MessageEvent, session_key: str, text_content: str, metadata: Dict[str, Any], *,
        reply_to: Optional[str], is_ephemeral_response: bool = False,
    ) -> "tuple[SendResult, BasePlatformAdapter]":
        """The delivery-ledger bracket every final text goes through, on the CURRENT transport
        (a reconnect may have replaced this adapter): record the obligation before the send,
        send with retry, finalize from the result — so a refused final (flood control, a dead
        transport) leaves a ledger row the boot sweep / runtime redelivery can act on. ``event``
        supplies the source and the ledger identity (``ledger_message_id`` or ``message_id``).
        Returns the result with the adapter that sent it: that adapter owns ``result.message_id``
        (an ephemeral delete must go to the same transport)."""
        delivery_adapter = self._final_delivery_adapter(event.source)
        logger.info("[%s] Sending response (%d chars) to %s", delivery_adapter.name,
                    len(text_content), event.source.chat_id)
        obligation_id = await self._record_delivery_obligation(
            event, session_key, text_content, delivery_adapter, is_ephemeral_response)
        if obligation_id is not None:
            await self._release_turn_marker(event)  # the ledger now owns the crash recovery
        result = await delivery_adapter._send_with_retry(
            chat_id=event.source.chat_id, content=text_content, reply_to=reply_to, metadata=metadata)
        stop_reply_clock(delivery_adapter, event.source.chat_id, result)
        if obligation_id is not None:
            await self._finalize_delivery_obligation(obligation_id, result, event, delivery_adapter)
        return result, delivery_adapter

    async def _release_turn_marker(self, event: MessageEvent) -> None:
        """Clear the crash-recovery marker the runner handed to this delivery lifecycle
        (``_turn_marker_handoff``): only once the final reply is ledgered or nothing more is owed,
        so no kill leaves a persisted reply with neither marker nor ledger row. Idempotent."""
        if getattr(event, "_turn_marker_handoff", False) and getattr(event, "_gateway_active_turn_token", None):
            await self.gateway_runner._clear_durable_active_turn(event)
