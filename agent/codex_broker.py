"""Fail-closed per-turn credentials leased from Codex Broker."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math
import os
from pathlib import Path
import threading
import time
from typing import Any, Callable
from urllib.parse import urlparse

import httpx


class CodexBrokerError(RuntimeError):
    """A safe, user-facing broker failure with no credential material."""


@dataclass(frozen=True)
class Lease:
    account_id: str
    account_label: str
    access_token: str
    chatgpt_account_id: str
    expires_at: str
    short_remaining_percent: int | None
    weekly_remaining_percent: int | None
    short_resets_at: str | None
    weekly_resets_at: str | None


@dataclass(frozen=True)
class BrokerStatus:
    account_label: str
    short_remaining_percent: int | None
    weekly_remaining_percent: int | None
    short_resets_at: str | None
    weekly_resets_at: str | None


InterruptCheck = Callable[[], bool]


def codex_broker_configured() -> bool:
    """Whether both required broker settings are present."""
    return bool(os.environ.get("HERMES_CODEX_BROKER_URL", "").strip()) and bool(
        os.environ.get("HERMES_CODEX_BROKER_CLIENT_KEY", "").strip()
    )


def codex_broker_environment_present() -> bool:
    """Whether broker configuration was attempted, including a malformed partial setup."""
    return any(
        os.environ.get(name, "").strip()
        for name in (
            "HERMES_CODEX_BROKER_URL",
            "HERMES_CODEX_BROKER_CLIENT_KEY",
            "HERMES_CODEX_BROKER_CA_CERT",
        )
    )


def _reset_in(value: str) -> str:
    minutes = max(
        0,
        math.ceil(
            (datetime.fromisoformat(value.replace("Z", "+00:00")) - datetime.now(timezone.utc)).total_seconds()
            / 60
        ),
    )
    if minutes >= 1_440:
        return f"{minutes // 1_440}d {(minutes % 1_440) // 60}h"
    if minutes >= 60:
        return f"{minutes // 60}h {minutes % 60}m"
    return f"{minutes}m"


class CodexBrokerLeaseManager:
    def __init__(self, url: str, client_key: str, ca_cert: str | None = None) -> None:
        parsed = urlparse(url)
        if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
            raise CodexBrokerError("Codex Broker URL must be an HTTPS origin")
        if parsed.query or parsed.fragment:
            raise CodexBrokerError("Codex Broker URL must not contain a query or fragment")
        if ca_cert and not Path(ca_cert).is_file():
            raise CodexBrokerError("Codex Broker CA certificate is unavailable")
        self._route_url = f"{url.rstrip('/')}/api/v1/route"
        self._client_key = client_key
        self._verify: bool | str = ca_cert or True
        self._lease_by_turn: dict[tuple[str, str], Lease] = {}
        self._preferred_by_session: dict[str, str] = {}
        self._status_by_session: dict[str, BrokerStatus] = {}
        self._failed_accounts_by_turn: dict[tuple[str, str], set[str]] = {}
        self._lock = threading.RLock()

    @classmethod
    def from_environment(cls) -> CodexBrokerLeaseManager | None:
        url = os.environ.get("HERMES_CODEX_BROKER_URL", "").strip()
        key = os.environ.get("HERMES_CODEX_BROKER_CLIENT_KEY", "").strip()
        ca_cert = os.environ.get("HERMES_CODEX_BROKER_CA_CERT", "").strip() or None
        if not url and not key and not ca_cert:
            return None
        if not url or not key:
            raise CodexBrokerError("Codex Broker URL and client key must both be configured")
        return cls(url, key, ca_cert)

    def lease_for_turn(
        self, session_id: str, turn_id: str, *, interrupted: InterruptCheck
    ) -> Lease:
        turn = (session_id, turn_id)
        with self._lock:
            if turn in self._lease_by_turn:
                return self._lease_by_turn[turn]
            payload: dict[str, Any] = {"session_id": session_id, "turn_id": turn_id}
            if preferred := self._preferred_by_session.get(session_id):
                payload["preferred_account_id"] = preferred
            lease = self._route(payload, interrupted)
            self._remember(turn, lease)
            return lease

    def replace_failed_lease(
        self,
        session_id: str,
        turn_id: str,
        failure_kind: str,
        *,
        interrupted: InterruptCheck,
    ) -> Lease | None:
        turn = (session_id, turn_id)
        with self._lock:
            current = self._lease_by_turn.get(turn)
            if current is None:
                raise CodexBrokerError("Codex Broker has no lease for this turn")
            failed = self._failed_accounts_by_turn.setdefault(turn, set())
            if current.account_id in failed:
                return None
            failed.add(current.account_id)
            lease = self._route(
                {
                    "session_id": session_id,
                    "turn_id": turn_id,
                    "preferred_account_id": current.account_id,
                    "failed_account_id": current.account_id,
                    "failure_kind": failure_kind,
                },
                interrupted,
            )
            if lease.account_id in failed:
                return None
            self._remember(turn, lease)
            return lease

    def discard_turn(self, session_id: str, turn_id: str) -> None:
        with self._lock:
            self._lease_by_turn.pop((session_id, turn_id), None)
            self._failed_accounts_by_turn.pop((session_id, turn_id), None)

    def status_for_session(self, session_id: str) -> BrokerStatus | None:
        with self._lock:
            return self._status_by_session.get(session_id)

    @staticmethod
    def format_status(status: BrokerStatus) -> str:
        def window(remaining: int | None, reset: str | None) -> str:
            percent = "?" if remaining is None else f"{remaining}%"
            return f"{percent} (resets {_reset_in(reset)})" if reset else percent

        return (
            f"{status.account_label} · 5h "
            f"{window(status.short_remaining_percent, status.short_resets_at)} · week "
            f"{window(status.weekly_remaining_percent, status.weekly_resets_at)}"
        )

    def clear_agent(self, agent: Any) -> None:
        """Remove turn credentials from the long-lived agent and client."""
        from agent.codex_headers import codex_cloudflare_headers

        blocked = {"authorization", "chatgpt-account-id", "user-agent", "originator"}
        existing = dict(agent._client_kwargs.get("default_headers") or {})
        kwargs = dict(agent._client_kwargs)
        kwargs["api_key"] = "broker-managed"
        kwargs["default_headers"] = {
            **{key: value for key, value in existing.items() if str(key).lower() not in blocked},
            **codex_cloudflare_headers("", base_url=str(agent.base_url or "")),
        }
        agent.api_key = "broker-managed"
        agent._client_kwargs = kwargs
        if not agent._replace_primary_openai_client(reason="codex_broker_turn_cleanup"):
            agent.client = None
            raise CodexBrokerError("Codex Broker could not clear the turn credential")

    def apply_to_agent(self, agent: Any, lease: Lease) -> None:
        from agent.codex_headers import codex_cloudflare_headers

        headers = codex_cloudflare_headers(
            lease.access_token,
            base_url=str(agent.base_url or ""),
            account_id=lease.chatgpt_account_id,
        )
        blocked = {"authorization", "chatgpt-account-id", "user-agent", "originator"}
        existing = dict(agent._client_kwargs.get("default_headers") or {})
        kwargs = dict(agent._client_kwargs)
        kwargs["api_key"] = lease.access_token
        kwargs["default_headers"] = {
            **{key: value for key, value in existing.items() if str(key).lower() not in blocked},
            **headers,
        }
        agent.api_key = lease.access_token
        agent._client_kwargs = kwargs
        if not agent._replace_primary_openai_client(reason="codex_broker_lease"):
            raise CodexBrokerError("Codex Broker could not activate the leased credential")

    def _remember(self, turn: tuple[str, str], lease: Lease) -> None:
        self._lease_by_turn[turn] = lease
        self._preferred_by_session[turn[0]] = lease.account_id
        self._status_by_session[turn[0]] = BrokerStatus(
            lease.account_label,
            lease.short_remaining_percent,
            lease.weekly_remaining_percent,
            lease.short_resets_at,
            lease.weekly_resets_at,
        )

    def _route(self, payload: dict[str, Any], interrupted: InterruptCheck) -> Lease:
        while True:
            if interrupted():
                raise InterruptedError("Interrupted while waiting for Codex Broker")
            try:
                response = httpx.post(
                    self._route_url,
                    headers={"Authorization": f"Bearer {self._client_key}"},
                    json=payload,
                    timeout=httpx.Timeout(30.0, connect=5.0),
                    verify=self._verify,
                )
            except (httpx.HTTPError, OSError) as exc:
                raise CodexBrokerError("Codex Broker unavailable") from exc
            if response.status_code == 401:
                raise CodexBrokerError("Codex Broker client key rejected")
            try:
                body = response.json()
            except ValueError as exc:
                raise CodexBrokerError("Codex Broker returned an invalid response") from exc
            if not isinstance(body, dict):
                raise CodexBrokerError("Codex Broker returned an invalid response")
            if response.status_code == 200 and body.get("status") == "ok":
                return self._parse_lease(body)
            if response.status_code == 429 and body.get("status") == "wait":
                self._wait(body, interrupted)
                continue
            raise CodexBrokerError("Codex Broker unavailable")

    @staticmethod
    def _parse_lease(body: dict[str, Any]) -> Lease:
        strings = ("account_id", "account_label", "access_token", "chatgpt_account_id", "expires_at")
        percents = ("short_remaining_percent", "weekly_remaining_percent")
        resets = ("short_resets_at", "weekly_resets_at")
        if any(not isinstance(body.get(field), str) or not body[field] for field in strings):
            raise CodexBrokerError("Codex Broker returned an invalid lease")
        if any(body.get(field) is not None and (isinstance(body[field], bool) or not isinstance(body[field], int)) for field in percents):
            raise CodexBrokerError("Codex Broker returned an invalid lease")
        if any(body.get(field) is not None and not isinstance(body[field], str) for field in resets):
            raise CodexBrokerError("Codex Broker returned an invalid lease")
        try:
            for value in (body["expires_at"], *(body[field] for field in resets if body.get(field))):
                datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise CodexBrokerError("Codex Broker returned an invalid lease") from exc
        return Lease(
            *(body[field] for field in (*strings, *percents)),
            *(body.get(field) for field in resets),
        )

    @staticmethod
    def _wait(body: dict[str, Any], interrupted: InterruptCheck) -> None:
        retry = body.get("retry_after_seconds")
        reset = body.get("next_retry_at")
        if (
            body.get("code") != "POOL_EXHAUSTED"
            or isinstance(retry, bool)
            or not isinstance(retry, (int, float))
            or not math.isfinite(retry)
            or retry < 0
            or not isinstance(reset, str)
        ):
            raise CodexBrokerError("Codex Broker returned an invalid wait response")
        try:
            datetime.fromisoformat(reset.replace("Z", "+00:00"))
        except ValueError as exc:
            raise CodexBrokerError("Codex Broker returned an invalid wait response") from exc
        deadline = time.monotonic() + retry
        while (remaining := deadline - time.monotonic()) > 0:
            if interrupted():
                raise InterruptedError("Interrupted while waiting for Codex Broker")
            time.sleep(min(1.0, remaining))
