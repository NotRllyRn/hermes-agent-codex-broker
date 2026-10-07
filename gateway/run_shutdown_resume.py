"""Persist generic resume markers without replaying ledger-owned callbacks."""
import logging


class GatewayShutdownResumeMixin:
    async def _mark_running_sessions_resume_pending(self, log_prefix: str) -> list:
        """Mark every non-pending running session resume_pending; returns the keys marked."""
        from gateway.run_shutdown import _log_suppressed
        from gateway.run import _AGENT_PENDING_SENTINEL
        reason = "restart_timeout" if self._restart_requested else "shutdown_timeout"
        marked: list[str] = []
        # Pre-mark sessions as resume_pending BEFORE the drain wait. If the process is killed by the service
        # manager during the drain, the durable marker is already written so the next gateway boot can
        # recover in-flight sessions (#27856).
        for _sk, _agent in list(self._running_agents.items()):
            if _agent is _AGENT_PENDING_SENTINEL:
                continue
            state = self._peek_session_state(_sk)
            if state is not None and getattr(state.turn.event, "_callback_id", None) is not None:
                # The callback ledger owns interrupted-callback recovery, not generic resume.
                continue
            with _log_suppressed(logging.DEBUG, "%s failed for %s: %s", log_prefix, _sk):
                await self.async_session_store.mark_resume_pending(_sk, reason)
                marked.append(_sk)
        return marked
