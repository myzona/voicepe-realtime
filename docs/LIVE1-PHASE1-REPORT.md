# GPT-Live-1 port — phase 1 report: pipecat 0.0.97 → 1.10.0

*Branch `live1-port` (fork `myzona/voicepe-realtime`), add-on `openai_realtime_voice_agent`
0.16.11 → **0.17.0-live.1**. Scope per `docs/LIVE1-PORT-PLAN.md` phase 1: migrate to pipecat-ai
1.10.0 with behaviour on `gpt-realtime-2` unchanged. No Live-1 code yet (phase 2).*

## What changed

| Area | 0.0.97 | 1.10.0 (this branch) |
|---|---|---|
| `pyproject.toml` | `pipecat-ai[mcp,openai,websocket]==0.0.97` | `==1.10.0`, same extras (`openai` is empty in 1.x — the SDK is a core dep — kept for clarity); **`httpx` added explicitly** (openai 3.x depends on `httpx2`, so `import httpx` in timers/enrollment/ha_sensors/openclaw_tool would have failed). `poetry.lock` regenerated (Poetry 2.4.3, `poetry lock`); the Dockerfile still `pip install`s from pyproject. |
| `RawAudioSerializer` | `FrameSerializerType.BINARY` + `type` property | Both gone in 1.x (transport dispatches on payload type); `super().__init__()` now required; `serialize()` returns `None` for non-audio (same wire behaviour as the old `b""`). |
| `MixedFastAPIWebsocketClient` | `FastAPIWebsocketClient(ws, is_binary, callbacks)` | `(ws, callbacks, ws_close_timeout=)`. The stock 1.x client already carries text+binary; the subclass is kept for the send lock and starlette post-disconnect `RuntimeError` handling. |
| Pipeline task/runner | `PipelineTask` / `PipelineRunner`, `runner.run(task)` | `PipelineWorker(..., enable_rtvi=False)` / `WorkerRunner(handle_sigint=False)`, `add_workers()` + `run()`. **`enable_rtvi=False` matters**: 1.x otherwise prepends an `RTVIProcessor` to the pipeline. |
| `SafeRealtimeLLMService` ctor | `model=`, `session_properties=` | `settings=Settings(model=, session_properties=)` (old kwargs are deprecated shims). pipecat's default model is now `gpt-realtime-2.1`; the add-on always sets it explicitly. |
| `register_function` override | `(name, handler, start_callback=None, *, cancel_on_interruption)` | `(name, handler, *, cancel_on_interruption, timeout_secs, cancellable_by_llm)`; still forces `cancel_on_interruption=False`. |
| Turn frames | service pushed `UserStarted/StoppedSpeakingFrame` + interruption on OpenAI VAD events | 1.x broadcasts `ProposedUser*SpeakingFrame` for the aggregator to resolve. **Overridden back to 0.0.97 semantics** (see "Decisions"). |
| Context aggregators | `LLMContextAggregatorPair(context)` | `RealtimeContextAggregatorPair` with inert external turn strategies, `realtime_service_mode=True`, and a `RealtimeAssistantAggregator` that pushes tool results immediately (see "Decisions"). |
| MCP | `MCPClient.get_tools_schema()` / `register_tools_schema()` cold, fresh transport per call | Both deprecated and require `start()`; session is persistent and never self-heals. `ResilientMCPClient` + `HomeAssistantMCPService.fetch_tools_schema()/register_handlers()` reconnect + retry once (see "Decisions"). Closed once at add-on shutdown. |
| `OutputLeadBuffer` | `StartInterruptionFrame` (deprecated alias) | `InterruptionFrame` (alias removed in 1.x). |
| `TranscriptLogger` | one `TTSTextFrame` per transcript delta | 1.x also pushes an `LLMTextFrame` per delta → would log every reply twice; now prefers TTS text, LLM text only as text-modality fallback. |
| `disconnect_tool` | typed against `WebsocketServerTransport` (renamed in 1.4, and unused since 0.16.8) | typed against `BaseTransport`; closes via the per-device transport client. Tool stays disabled by default. |
| Version / changelog | 0.16.11 | 0.17.0-live.1, CHANGELOG entry. |

Files: `pyproject.toml`, `poetry.lock`, `config.yaml`, `CHANGELOG.md`, `app/{main,session_manager,
websocket_handler,mcp_service,raw_audio_serializer,multi_client_transport,output_lead_buffer,
transcript_logger,disconnect_tool,web_search_tool}.py`, `tests/{test_mixed_transport,
test_output_lead_buffer,test_two_clients}.py`, new `tests/test_pipeline_smoke.py`.

## Decisions (why the migration is not a pure rename)

1. **Turn frames stay service-driven (0.0.97 semantics), not aggregator-driven.**
   pipecat 1.x routes OpenAI's `speech_started/stopped` through `LLMUserAggregator`'s
   `UserTurnController`. That controller (a) ignores a second turn start while it believes a turn
   is open (`if self._user_turn: return`) and (b) force-stops a turn after
   `user_turn_stop_timeout=5 s` with no transcript activity. On this device that is not
   equivalent: `input_audio_buffer.clear` (device "stop", follow-up flush, connect) can end a
   server-VAD segment without a `speech_stopped`, the firmware lifts its post-stop mute **only** on
   a `listening` phase, and input transcription is off by default (so a >5 s question would flip
   the device to `thinking` mid-sentence). `SafeRealtimeLLMService._handle_evt_speech_started/
   stopped` therefore emit exactly what 0.0.97 emitted: `broadcast_interruption()` + one
   `UserStartedSpeakingFrame` / one `UserStoppedSpeakingFrame`. The aggregator pair gets inert
   external strategies so it never resolves proposals or invents turns (and never loads the
   Smart-Turn v3 ONNX model per connection, ~0.7 s).
2. **Tool results are pushed to OpenAI immediately.** 1.x's assistant aggregator drops the post-tool
   context push while the user is speaking and defers it while the bot is speaking. In the
   semantic_vad fragment race this add-on already guards against (`cancel_on_interruption=False`),
   that would leave an HA tool result unsent. `RealtimeAssistantAggregator` restores the 0.0.97
   unconditional push (queued sibling results are still bundled).
3. **MCP session resilience.** 0.0.97 opened a fresh transport per call, so HA restarts were
   invisible. 1.x holds one session and never re-establishes it; without a wrapper, every HA tool
   call after an HA restart would fail until the add-on restarted. `ResilientMCPClient` starts
   lazily and on a dead session closes, reconnects and retries once. The client is process-wide
   (shared by all device pipelines); `SafeRealtimeLLMService.register_function`'s plain-closure
   wrapper also keeps pipecat's per-service `_pipecat_cleanup` hook from closing it when one device
   disconnects.
4. **`enable_rtvi=False`** keeps the exact processor chain; the Voice PE speaks the va_client JSON
   protocol, not RTVI.

## What was verified

- **Unit tests**: `PYTHONPATH=. python -m pytest tests` → **13 passed** (10 pre-existing + 3 new),
  and `python -m unittest discover -s tests` (the CI command) → `Ran 13 tests … OK`, on Python
  3.12 with pipecat-ai 1.10.0 / mcp 2.2.0 / openai 3.14.1 / websockets 17.1. The same 10 pre-existing
  tests pass on the original code with 0.0.97 (baseline run before the migration). Running with
  `-W error::DeprecationWarning` (excluding CPython's `audioop` notice) is clean: no deprecated
  pipecat API is used on the exercised paths.
- **`tests/test_pipeline_smoke.py`** (new): builds the real `SafeRealtimeLLMService` via
  `Application.create_openai_service`, serves a fake Voice PE through
  `WebSocketHandler.serve_connection` with only the OpenAI WebSocket connect stubbed, and asserts:
  the `session.update` payload (model `gpt-realtime-2`, instructions, `marin`/1.0, semantic_vad
  low / create_response / no interrupt_response, transcription model+language, provider-native
  tool dicts incl. web_search/timers/memory/enrollment); pipeline topology equals the 0.0.97 chain
  with no `RTVIProcessor`; every registered tool has `cancel_on_interruption=False`; pre-seeded
  context (no greeting on connect); `speech_started/stopped` → interruption +
  `UserStarted/StoppedSpeakingFrame` (no `Proposed*`); device control frames reach the serializer
  handlers (hello, pong); 16 kHz PCM is resampled to 24 kHz and forwarded as
  `input_audio_buffer.append`; clean teardown (worker finished, device deregistered, recovery/
  phase tasks released, context cached for session reuse).
- **Wire parity**: for identical `SessionProperties`, `SessionUpdateEvent.model_dump(exclude_none=True)`
  is byte-identical on 0.0.97 and 1.10.0 (checked side by side in two venvs). The soxr streaming
  resampler behaves identically (primes on the first ~4×20 ms chunks in both).
- **MCP**: `ResilientMCPClient` exercised against a local mcp 2.x streamable-HTTP server: init
  while the server is down (no raise), list tools, call, kill+restart server → reconnect and the
  call succeeds, server down → pipecat's usual "Sorry, could not call the mcp tool" text, server
  back → works, schema fetch again works.
- **Docker**: `docker build --platform linux/amd64 -f openai_realtime_voice_agent/Dockerfile
  --build-arg BUILD_FROM=ghcr.io/home-assistant/amd64-base-debian:bookworm openai_realtime_voice_agent/`
  — see the "Build result" section at the end (filled in from the actual run).

Not verified here (needs the VM + a device, phase-1 exit criterion in the plan): live regression
on the office PE — wake word, HA tool call, `recall_memory`/`ask_openclaw` via the bridge, announce
endpoint, timers, device stop / follow-up window, 60-min reconnect.

## Open risks / things to watch in the live regression

1. **`TTSStoppedFrame` timing changed in pipecat.** 0.0.97 pushed it on `response.output_audio.done`
   (per item), 1.10 pushes it once on `response.done`. `PhaseEmitter` derives `replying`/`idle` from
   the output transport's `BotStarted/StoppedSpeakingFrame` with a 1.5 s idle debounce, so this
   should be invisible, but the `idle` moment after a reply and the trailing-chunk flush
   (`MediaSender.handle_tts_stopped`) are worth listening for.
2. **`LLMFullResponseEndFrame` on interruption.** In 1.x the service's `_handle_interruption`
   pushes `LLMFullResponseEndFrame` + `TTSStoppedFrame` when a response is active; with the
   service-driven turn frames (decision 1) the service does not process its own interruption
   broadcast, matching 0.0.97, where the frame arriving from upstream only ran
   `_truncate_current_audio_response` (no-op'd) and `stop_all_metrics`.
3. **Output transport write timeout.** 1.x bounds each transport write by
   `audio_out_write_timeout_secs=10` and marks the transport unusable on timeout
   (`processor_unusable_policy=CONTINUE` by default). A device that stops reading for >10 s during
   a reply would now trip this where 0.0.97 hung; the connection is torn down on disconnect anyway.
4. **`FastAPIWebsocketClient.disconnect()`** now waits ≤0.5 s for the close handshake (was
   unbounded) — faster teardown, harmless.
5. **`PIPECAT_ALLOWED_ORIGINS`**: if that env var is ever set in the add-on environment, 1.x rejects
   WebSocket connections without a matching `Origin` header (the Voice PE sends none). It is unset
   in the HA add-on environment; do not add it.
6. **Setup timeout.** The realtime WebSocket now connects in `setup()` (before `StartFrame`) under
   `PipelineWorker.setup_timeout_secs=20`; a >20 s OpenAI connect tears the worker down instead of
   hanging. `ConnectionRecovery` still handles later drops in place.
7. **MCP persistent session** replaces per-call connections. Positive: fewer HTTP handshakes.
   Watch for HA MCP Server session expiry (`Mcp-Session-Id` lifetime) — covered by the
   reconnect-once wrapper, but the first call after an expiry pays one failed round-trip.
8. **Pre-existing, not touched:** `realtime_payload.transform_gpt_transcription_language` looks for
   the legacy top-level `session.input_audio_transcription` field; the GA-shaped payload the add-on
   sends nests transcription under `session.audio.input.transcription`, so the `languages: [..]`
   rewrite for `gpt-transcribe`/`gpt-live-transcribe` is a no-op (on 0.0.97 as well). Fix in a
   separate change if those transcription models are actually used with a language pin.
9. **Docker base**: pipecat 1.10 pins `soxr~=1.0.0`, `onnxruntime~=1.24.3`, `websockets>=13.1`
   (core), `mcp[cli]<3` and pulls openai 3.x + httpx2. All ship manylinux wheels for both add-on
   arches; the aarch64 image was not built here (only amd64 on this Mac).

## Phase 2 notes (Live-1)

- The plan's `ExternalUserTurnStrategies(enable_interruptions=False)` for Live-1 means going through
  the aggregator-side `UserTurnController` — the exact path decision 1 avoids for Realtime. Either
  apply the same service-side override to `OpenAILiveLLMService` (it also broadcasts
  `ProposedUser*` frames), or raise `LLMUserAggregatorParams.user_turn_stop_timeout` and accept the
  swallowed-second-start behaviour; decide after the phase-1 live regression shows how often
  `speech_started` without `speech_stopped` actually happens.
- `OpenAILiveLLMService` lives in `pipecat.services.openai.live` in 1.10.0 and shares the
  `LLMService` base, so `register_function` wrapping, `RealtimeAssistantAggregator` and the MCP path
  carry over unchanged. `SafeRealtimeLLMService`'s Realtime-specific overrides
  (`_truncate_current_audio_response`, benign error codes, `reset_conversation` flags,
  `_receive_task_handler`) need a Live-1 equivalent.
- Live sessions expire (~1 h `expires_at`); `ConnectionRecovery`'s proactive 55-min refresh and the
  wedge watchdog are the hooks.

## Build result

`docker build --platform linux/amd64 -f openai_realtime_voice_agent/Dockerfile --build-arg
BUILD_FROM=ghcr.io/home-assistant/amd64-base-debian:bookworm -t voicepe-live1-port:amd64
openai_realtime_voice_agent/` on this Mac (Apple Silicon, Docker Desktop 29.6.1, amd64 emulation):
**exit 0**, image 609 MB, `pip install` step 145 s, everything from wheels (no source builds).
Resolved in the image: `pipecat-ai 1.10.0, openai 3.14.1, httpx 0.28.1, httpx2 2.13.0, mcp 2.2.0,
websockets 17.1, fastapi 0.141.1, uvicorn 0.53.0, soxr 1.0.0, onnxruntime 1.24.4, numpy 2.4.6,
sherpa-onnx 1.13.8, pydantic 2.13.5`, on Python 3.11.2.

Inside that image: `import app.main` etc. succeed, `make_context_aggregator_pair()` builds the
`LLMUserAggregator` + `RealtimeAssistantAggregator` pair, and the add-on tests run green on the
deployment runtime — `unittest` on the 11 module-style tests (`test_pipeline_smoke`,
`test_output_lead_buffer`, `test_realtime_payload`, `test_device_registry`) → `Ran 11 tests … OK`,
plus the 5 script-style tests (`test_connection_recovery`, `test_two_clients`,
`test_mixed_transport`, `test_recording_ownership`, `test_timer_targeting`) → all PASS.
(`test_public_examples` reads `examples/` from the repo root and is not applicable in the image.)

Not done here, by instruction: no build/install on the HA VM, running add-on untouched. Next step
for phase 1 exit is installing this branch as `local_openai_realtime_voice_agent` on VM 140 and
running the live checklist above.
