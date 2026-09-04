from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from agent.codex_broker import CodexBrokerError, CodexBrokerLeaseManager, Lease


class Response:
    def __init__(self, status_code: int, body: dict[str, object]) -> None:
        self.status_code = status_code
        self._body = body

    def json(self) -> dict[str, object]:
        return self._body


def lease_body(account: str = "public-a") -> dict[str, object]:
    return {
        "status": "ok",
        "account_id": account,
        "account_label": "Primary",
        "access_token": f"token-{account}",
        "chatgpt_account_id": f"upstream-{account}",
        "expires_at": "2099-01-01T00:00:00Z",
        "short_remaining_percent": 80,
        "weekly_remaining_percent": 60,
        "short_resets_at": "2099-01-01T00:00:00Z",
        "weekly_resets_at": "2099-01-02T00:00:00Z",
    }


def test_environment_is_disabled_or_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "HERMES_CODEX_BROKER_URL",
        "HERMES_CODEX_BROKER_CLIENT_KEY",
        "HERMES_CODEX_BROKER_CA_CERT",
    ):
        monkeypatch.delenv(name, raising=False)
    assert CodexBrokerLeaseManager.from_environment() is None
    monkeypatch.setenv("HERMES_CODEX_BROKER_URL", "http://broker.test")
    monkeypatch.setenv("HERMES_CODEX_BROKER_CLIENT_KEY", "cbk_test")
    with pytest.raises(CodexBrokerError, match="HTTPS"):
        CodexBrokerLeaseManager.from_environment()


def test_lease_is_cached_per_turn_and_preferred_next_turn(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    ca = tmp_path / "ca.crt"
    ca.write_text("test")
    calls: list[dict[str, Any]] = []

    def post(_url, **kwargs):
        calls.append(kwargs)
        return Response(200, lease_body())

    monkeypatch.setattr("agent.codex_broker.httpx.post", post)
    manager = CodexBrokerLeaseManager("https://broker.test", "cbk_test", str(ca))
    first = manager.lease_for_turn("session", "turn-1", interrupted=lambda: False)
    assert manager.lease_for_turn("session", "turn-1", interrupted=lambda: False) is first
    manager.discard_turn("session", "turn-1")
    manager.lease_for_turn("session", "turn-2", interrupted=lambda: False)
    assert len(calls) == 2
    assert calls[0]["verify"] == str(ca)
    assert calls[1]["json"]["preferred_account_id"] == "public-a"
    assert calls[0]["headers"]["Authorization"] == "Bearer cbk_test"


def test_lease_accepts_broker_without_reset_metadata() -> None:
    body = lease_body()
    body.pop("short_resets_at")
    body.pop("weekly_resets_at")
    lease = CodexBrokerLeaseManager._parse_lease(body)
    assert lease.short_resets_at is None
    assert lease.weekly_resets_at is None


def test_wait_retries_and_can_be_interrupted(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = iter(
        [
            Response(
                429,
                {
                    "status": "wait",
                    "code": "POOL_EXHAUSTED",
                    "next_retry_at": "2099-01-01T00:00:00Z",
                    "retry_after_seconds": 0,
                },
            ),
            Response(200, lease_body()),
        ]
    )
    monkeypatch.setattr("agent.codex_broker.httpx.post", lambda *_a, **_k: next(responses))
    manager = CodexBrokerLeaseManager("https://broker.test", "cbk_test")
    assert manager.lease_for_turn("s", "t", interrupted=lambda: False).account_id == "public-a"

    interrupted = False
    monkeypatch.setattr(
        "agent.codex_broker.httpx.post",
        lambda *_a, **_k: Response(
            429,
            {
                "status": "wait",
                "code": "POOL_EXHAUSTED",
                "next_retry_at": "2099-01-01T00:00:00Z",
                "retry_after_seconds": 2,
            },
        ),
    )

    def interrupt_sleep(_seconds: float) -> None:
        nonlocal interrupted
        interrupted = True

    monkeypatch.setattr("agent.codex_broker.time.sleep", interrupt_sleep)
    with pytest.raises(InterruptedError):
        manager.lease_for_turn("s", "interrupted", interrupted=lambda: interrupted)


def test_replacement_walks_unique_accounts(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def post(_url, **kwargs):
        calls.append(kwargs["json"])
        account = ("public-a", "public-b", "public-c")[min(len(calls) - 1, 2)]
        return Response(200, lease_body(account))

    monkeypatch.setattr("agent.codex_broker.httpx.post", post)
    manager = CodexBrokerLeaseManager("https://broker.test", "cbk_test")
    manager.lease_for_turn("s", "t", interrupted=lambda: False)
    second = manager.replace_failed_lease("s", "t", "auth", interrupted=lambda: False)
    third = manager.replace_failed_lease("s", "t", "quota", interrupted=lambda: False)
    assert second and second.account_id == "public-b"
    assert third and third.account_id == "public-c"
    assert calls[1]["failed_account_id"] == "public-a"
    assert calls[1]["failure_kind"] == "auth"
    assert calls[2]["failed_account_id"] == "public-b"
    assert calls[2]["failure_kind"] == "quota"
    assert manager.replace_failed_lease("s", "t", "quota", interrupted=lambda: False) is None
    assert len(calls) == 4
    status = manager.status_for_session("s")
    assert status and status.account_label == "Primary"
    assert "5h 80%" in manager.format_status(status)


def test_apply_and_clear_agent_use_explicit_account_identity() -> None:
    reasons: list[str] = []
    agent = SimpleNamespace(
        base_url="https://chatgpt.com/backend-api/codex",
        api_key="broker-managed",
        client=object(),
        _client_kwargs={
            "api_key": "broker-managed",
            "base_url": "https://chatgpt.com/backend-api/codex",
            "default_headers": {
                "authorization": "forbidden",
                "chatgpt-account-id": "wrong",
                "user-agent": "wrong",
            },
        },
        _replace_primary_openai_client=lambda *, reason: reasons.append(reason) or True,
    )
    manager = CodexBrokerLeaseManager("https://broker.test", "cbk_test")
    manager.apply_to_agent(
        agent,
        Lease(
            "public",
            "Primary",
            "secret",
            "upstream",
            "2099-01-01T00:00:00Z",
            80,
            60,
            "2099-01-01T00:00:00Z",
            "2099-01-02T00:00:00Z",
        ),
    )
    assert agent.api_key == "secret"
    assert agent._client_kwargs["default_headers"]["ChatGPT-Account-ID"] == "upstream"
    assert not {"authorization", "chatgpt-account-id", "user-agent"} & set(
        agent._client_kwargs["default_headers"]
    )
    manager.clear_agent(agent)
    assert agent.api_key == "broker-managed"
    assert "secret" not in repr(agent._client_kwargs)
    assert reasons == ["codex_broker_lease", "codex_broker_turn_cleanup"]


def test_invalid_payload_and_rejected_key_are_safe(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = CodexBrokerLeaseManager("https://broker.test", "cbk_secret")
    monkeypatch.setattr(
        "agent.codex_broker.httpx.post", lambda *_a, **_k: Response(200, {"status": "ok"})
    )
    with pytest.raises(CodexBrokerError, match="invalid lease") as error:
        manager.lease_for_turn("s", "t", interrupted=lambda: False)
    assert "cbk_secret" not in str(error.value)
    monkeypatch.setattr(
        "agent.codex_broker.httpx.post", lambda *_a, **_k: Response(401, {})
    )
    with pytest.raises(CodexBrokerError, match="client key rejected"):
        manager.lease_for_turn("s", "other", interrupted=lambda: False)
