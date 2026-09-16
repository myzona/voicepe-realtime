#!/usr/bin/env python3
"""Open one GPT-Live session and print the `session.started` summary.

Pre-deploy smoke test for the add-on's `openai_model: gpt-live-1` path: proves
the key can use the Live API, that the delegation/voice shape is accepted, and
shows the session's `expires_at`. Sends `session.start`, waits for
`session.started` (or an `error`), then `session.close` → `session.closed`.
No audio is sent; the session costs a second or two of Live time.

    OPENAI_API_KEY=... python3 tools/live_probe.py
    OPENAI_API_KEY=... python3 tools/live_probe.py --voice vesper --backend gpt-5.4
    OPENAI_API_KEY=... python3 tools/live_probe.py --with-tool   # include one function tool

The key comes ONLY from the environment and is never printed. Needs only the
`websockets` package (a pipecat dependency, so any venv with the add-on's
requirements works). Not shipped in the add-on image; run it from the repo.
"""
import argparse
import asyncio
import json
import os
import sys
import time

try:
    from websockets.asyncio.client import connect
except ImportError:  # websockets < 13
    from websockets.client import connect  # type: ignore

LIVE_URL = "wss://api.openai.com/v1/live/sessions"


def _summary(session: dict) -> str:
    expires = session.get("expires_at")
    ttl = f" (in {max(0, expires - time.time()) / 60:.0f} min)" if isinstance(expires, (int, float)) else ""
    delegation = session.get("delegation") or {}
    responses = delegation.get("responses") or {}
    audio = session.get("audio") or {}
    return "\n".join([
        f"session.id      : {session.get('id')}",
        f"status          : {session.get('status')}",
        f"model           : {session.get('model')}",
        f"voice           : {(audio.get('output') or {}).get('voice')}",
        f"audio.format    : {audio.get('format')}",
        f"delegation.type : {delegation.get('type')}",
        f"backend model   : {responses.get('model')}",
        f"backend tools   : {[t.get('type') + ':' + str(t.get('name', '')) for t in responses.get('tools') or []]}",
        f"expires_at      : {expires}{ttl}",
        f"instructions    : {(session.get('instructions') or '')[:60]!r}",
    ])


async def probe(model: str, backend: str, voice: str, instructions: str, with_tool: bool, timeout: float) -> int:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        print("OPENAI_API_KEY is not set in the environment", file=sys.stderr)
        return 2

    responses: dict = {"model": backend}
    if with_tool:
        responses["tools"] = [{
            "type": "function",
            "name": "probe_noop",
            "description": "Probe tool; never call it.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        }]
    start = {
        "type": "session.start",
        "event_id": "probe_start",
        "session": {
            "model": model,
            "instructions": instructions,
            "audio": {"output": {"voice": voice}},
            "delegation": {"type": "responses", "responses": responses},
        },
    }

    print(f"connecting to {LIVE_URL} …", file=sys.stderr)
    async with connect(LIVE_URL, additional_headers={"Authorization": f"Bearer {api_key}"}) as ws:
        await ws.send(json.dumps(start))
        started = None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            raw = await asyncio.wait_for(ws.recv(), timeout=max(0.1, deadline - time.monotonic()))
            evt = json.loads(raw)
            etype = evt.get("type")
            if etype == "session.started":
                started = evt["session"]
                print("session.started ✅")
                print(_summary(started))
                break
            if etype == "error":
                err = evt.get("error") or {}
                print("error ❌")
                print(json.dumps(err, indent=2))
                return 1
            print(f"  … {etype}", file=sys.stderr)
        if started is None:
            print("timed out waiting for session.started", file=sys.stderr)
            return 1

        await ws.send(json.dumps({"type": "session.close", "event_id": "probe_close"}))
        try:
            while True:
                evt = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
                if evt.get("type") == "session.closed":
                    print(f"session.closed (reason={evt.get('reason')}, usage={evt.get('usage')})")
                    break
                if evt.get("type") == "error":
                    print(f"error during close: {evt.get('error')}", file=sys.stderr)
                    break
        except (asyncio.TimeoutError, Exception) as e:  # noqa: BLE001
            print(f"no session.closed ({e!r}); the socket is being dropped", file=sys.stderr)
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--model", default="gpt-live-1")
    p.add_argument("--backend", default=os.environ.get("LIVE_BACKEND_MODEL") or "gpt-5.4-mini",
                   help="delegation.responses.model (default gpt-5.4-mini)")
    p.add_argument("--voice", default="marin")
    p.add_argument("--instructions", default="You are a test assistant. Stay silent.")
    p.add_argument("--with-tool", action="store_true", help="include one function tool in the delegation")
    p.add_argument("--timeout", type=float, default=20.0)
    args = p.parse_args()
    try:
        return asyncio.run(probe(args.model, args.backend, args.voice, args.instructions, args.with_tool, args.timeout))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
