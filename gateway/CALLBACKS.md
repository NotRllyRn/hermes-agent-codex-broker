# Durable internal gateway callbacks

This is an **in-process trusted-plugin API**, not an HTTP ingress or an authentication
mechanism. Call it on the gateway event loop. A plugin registers the lifecycle hook
`gateway_ready`; it receives keyword arguments `gateway` (the live runner) and
`session_store` (its routing store) after adapters are wired and startup restoration
has released the inbound gate. Hook return values are ignored. The hook is called
on cold startup, not when a plugin is hot-loaded into an already running gateway.
Use the ordinary plugin hook registration API, not runner monkeypatching/discovery.

```python
from gateway.platforms.event import MessageEvent

async def gateway_ready(gateway, session_store):
    # Capture these references for the plugin's local callback listener.
    pass

# In the plugin's register(ctx) entrypoint:
# ctx.register_hook("gateway_ready", gateway_ready)

# Capture origin/key/id at dispatch time, from the ORIGINAL routing entry.
event = MessageEvent(
    text="Background task completed: ...",
    source=original_entry.origin,
    internal=True,
    allow_gateway_control=False,
    metadata={
        "gateway_session_key": original_entry.session_key,
        "gateway_session_id": original_entry.session_id,
        "gateway_session_strict": True,
        "notification_category": "result",  # optional; result or diagnostic
    },
)
receipt = await gateway.admit_callback(callback_id, event)
# {"callback_id": callback_id, "status": "queued"} (or an existing status)
outcome = await gateway.get_callback_receipt(callback_id)
# Same shape; None if unknown. Safe to query even after /new invalidates the route.
```

The callback id is a nonempty string, at most 256 characters. IDs are unique for
the runner's profile-local ledger, not per session. Reusing an id with a different
canonical text/source/metadata payload raises `ValueError`. Validation also raises
`ValueError` for a missing/currently different session id, different original
source/profile, a key not derived from that source, non-text events, human event
identity/attachments, commands enabled, or unsupported metadata. An unavailable
original adapter raises `WakeNotAccepted`. Stateless/API-server destinations are
unsupported: this API does not self-post a forged human turn. Caller text is an
internal notification, processed with the gateway's existing internal-event
presentation and authorization semantics; the existing agent pipeline can persist
new notification/assistant rows, but this API never hand-edits historical transcripts.

## Durability and ordering

- Admission commits to `gateway_callbacks.db` beside the runner's profile-local
  `sessions/` directory (SQLite transaction, `synchronous=FULL`) before returning.
  Persistence errors propagate. No broker files or transcript rewrite are involved.
- The dispatcher leaves busy callbacks durably **queued**, outside the adapter's
  human pending slot. It enters through the normal adapter boundary after the
  current turn and adapter cleanup end. Queued callbacks are FIFO, serialized
  globally for this runner; a busy/offline head can delay other sessions.
- Source/key/id are checked at admission, dispatch, and runner entry. `/new` or
  `/reset` invalidates a queued callback; it becomes **rejected**, never follows the
  replacement session. Strict session metadata also fences session resolution.
- Cold startup recovers **queued** rows without relying on plugin listener traffic.
  Offline adapters leave those rows queued until an adapter becomes available.
- Runner entry records **running** before executing. Normal pipeline completion
  records **completed**; a nonexecuted/drop path records **rejected**. Exceptions,
  cancellation, or a crash during a running callback produce **uncertain**. On
  startup running rows become uncertain and are **not automatically replayed**.
  Callback turns bypass generic interrupted-turn auto-resume markers, including
  shutdown pre-marking, to prevent replay outside their callback identity boundary.
- Repeating an id never requeues a completed/rejected/uncertain row. A repeated
  admission still validates the current route; use `get_callback_receipt` to read
  an old receipt after its session has been replaced. IDs are retained indefinitely;
  no automatic pruning/retention policy is introduced here.

`queued` acknowledges durable admission, **not end-of-turn completion**.
`completed` describes the agent pipeline return, **not successful external delivery**.
There is intentionally no exactly-once model/tool side-effect or outbound-delivery
claim: a crash after side effects but before the completion commit is uncertain and
requires operator/application reconciliation. One gateway process per profile is
required; this is not a multi-process distributed execution lock. Tests exercise the
ledger, runner completion seam, busy/cold-start routing, hook payload, stale reset
refusal, identity validation, and existing internal-event regressions; they do not
contact a live model or messaging service.
