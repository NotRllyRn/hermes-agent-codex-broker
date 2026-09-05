from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from agent.codex_broker import CodexBrokerError, CodexBrokerLeaseManager, Lease
from agent.turn_api_error import _prepare_broker_continuation, handle_api_error


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


def test_post_output_failover_preserves_partial_and_requests_continuation() -> None:
    messages: list[dict[str, object]] = []
    api_messages: list[dict[str, object]] = []
    agent = SimpleNamespace(
        _current_streamed_assistant_text="partial answer",
        _strip_think_blocks=lambda value: value,
    )

    _prepare_broker_continuation(agent, messages, api_messages)

    assert messages[0]["role"] == "assistant"
    assert messages[0]["content"] == "partial answer"
    assert api_messages[0]["content"] == messages[0]["content"]
    assert api_messages[1]["role"] == "user"
    assert "without repeating" in str(api_messages[1]["content"])


def test_usage_limit_after_output_selects_next_account(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = MagicMock()
    broker.replace_failed_lease.return_value = Lease(
        "replacement", "Backup", "secret", "upstream", "2099-01-01T00:00:00Z",
        80, 60, None, None,
    )
    broker.status_for_session.return_value = None
    agent = SimpleNamespace(
        _codex_broker=broker,
        _codex_broker_output_started=True,
        _current_streamed_assistant_text="partial answer",
        _strip_think_blocks=lambda value: value,
        _extract_api_error_context=lambda _error: {},
        _invoke_api_request_error_hook=lambda **_kwargs: None,
        _interrupt_requested=False,
        thinking_callback=None,
        context_compressor=None,
        provider="openai-codex",
        model="gpt-5.4",
        session_id="session",
        log_prefix="",
        _emit_status=lambda _status: None,
    )
    monkeypatch.setattr(
        "agent.turn_api_error.recover_before_classification",
        lambda *_args, **_kwargs: (False, None),
    )
    error = RuntimeError("you've hit your usage limit")
    error.status_code = 429  # type: ignore[attr-defined]
    messages: list[dict[str, object]] = []
    api_messages: list[dict[str, object]] = []

    verdict = handle_api_error(
        agent, api_error=error, _retry=SimpleNamespace(), thinking_spinner=None,
        messages=messages, api_messages=api_messages, api_kwargs={}, system_message=None,
        active_system_prompt=None, conversation_history=[], approx_tokens=0, retry_count=0,
        max_retries=3, compression_attempts=0, max_compression_attempts=3,
        api_call_count=1, api_request_id="request", api_start_time=0,
        effective_task_id="task", turn_id="turn",
    )

    assert verdict.action == "continue"
    broker.replace_failed_lease.assert_called_once()
    assert broker.replace_failed_lease.call_args.args[2] == "quota"
    broker.apply_to_agent.assert_called_once()
    assert messages[0]["content"] == "partial answer"
    assert "without repeating" in str(api_messages[-1]["content"])


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
