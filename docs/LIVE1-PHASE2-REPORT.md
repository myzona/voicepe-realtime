# GPT-Live-1 port — phase 2+3 report: the `gpt-live-1` path

*Branch `live1-port` (fork `myzona/voicepe-realtime`), add-on `openai_realtime_voice_agent`
0.17.0-live.1 → **0.17.0-live.2**. Scope per `docs/LIVE1-PORT-PLAN.md` phases 2 (Live-1 path) and 3
(wake-word / session lifecycle), brief in `docs/LIVE1-PHASE2-BRIEF.md`. The `gpt-realtime-*` path is
untouched in behaviour (pinned by tests). Nothing was built or installed on the HA VM; no running
add-on was touched. Ergo was not reachable from the worktree — decisions are recorded here.*

## What changed

| Area | Change |
|---|---|
| `config.yaml` | `openai_model` dropdown gains `gpt-live-1`; new hidden optional `live_backend_model: str?` (main.py default `gpt-5.4-mini`); version `0.17.0-live.2`. `openai_model_custom` still works (`gpt-live-*` ids also select the Live path). |
| `root/run.sh` | exports `LIVE_BACKEND_MODEL` only when set (same `has_value` guard as the other hidden options). |
| `app/live_service.py` (new) | `SafeLiveLLMService(OpenAILiveLLMService)` + `resolve_live_voice()` / `is_live_model()` / `BACKEND_PREAMBLE`. See "How it works". |
| `app/tool_guard.py` (new) | The speaker-gate + turn-liveness handler wrapper, moved out of `SafeRealtimeLLMService.register_function` so both service classes share it. Behaviour identical (smoke test still asserts `cancel_on_interruption=False` on every registration). |
| `app/main.py` | `initialize()` reads `LIVE_BACKEND_MODEL`, validates the voice for Live and logs which options do not apply. `create_openai_service` keeps the shared tool assembly (disconnect, web_search, enrollment, timers, memory, ask_openclaw/recall_memory, HA MCP) and handler registration, and branches: `_build_live_service(all_tools)` for `gpt-live-1`, `_build_realtime_service(all_tools)` otherwise — the latter is the phase-1 block moved verbatim. `_preseed_context` skips the Live service (it starts from the pipeline's context instead). |
| `app/websocket_handler.py` | `build_pipeline` is Live-aware: bootstrap context handed to the service (no `ContextInitializer` replay), device "stop" → `handle_device_interrupt()`, no `input_audio_buffer.clear` / `response.cancel` / `on_conversation_item_created` on Live, speaker verdict → `session.thinking.append`. `ConnectionRecovery`: reader-death marker generalised to `"receive loop"`, new session-dead marker `"live session closed"`, refresh ahead of the service's `session_expires_at` (5 min before) in addition to the 55-min age rule, busy check via `service.is_busy()` when available. |
| `tests/test_pipeline_smoke_live.py` (new) | 8 tests, no network (see "Verification"). |
| `tools/live_probe.py` (new) | Standalone pre-deploy probe (key from `OPENAI_API_KEY` only). |
| Docs | `DOCS.md` §4a, `docs/configuration.md`, translations en/nl, `CHANGELOG.md`. |

Pipeline topology is unchanged (same 13 processors as phase 1, `SafeLiveLLMService` in the
`SafeRealtimeLLMService` slot, no `RTVIProcessor`).

## How it works (and the decisions behind it)

### `session.start` shape
Built by pipecat from `OpenAILiveLLMService.Settings(model, system_instruction, voice)` +
`ResponsesDelegation(settings=OpenAIResponsesLLMService.Settings(model=live_backend_model,
system_instruction=...))`:

```json
{"type": "session.start", "event_id": "...",
 "session": {"model": "gpt-live-1",
             "instructions": "<persona + household memory notes>",
             "audio": {"output": {"voice": "marin"}},
             "delegation": {"type": "responses",
                            "responses": {"model": "gpt-5.4-mini",
                                          "instructions": "<BACKEND_PREAMBLE + persona>",
                                          "tools": [{"type":"function","name":...,"description":...,"parameters":...}, ...]}},
             "input": [ ...cached text history, only when non-empty... ]}}
```
Matches the verified protocol facts (nested `delegation.responses.model`, never the flat
`delegation.model`; default PCM 24 kHz — `audio.format` is left to the server default, which is
what the pipeline runs at). Verified against pipecat's `events.SessionConfig` / `_responses_delegation_config`.

**Decision — tools:** pipecat expects the backend's tools in the `LLMContext`; here the SAME
provider-native dicts the Realtime path puts in `session.update.tools` are injected in
`_invocation_params()` (context tools would win if a context ever carried any). Handlers are
registered explicitly through `register_function` → never pruned by pipecat's tool sync, always
`cancel_on_interruption=False`, speaker gate + liveness identical to Realtime. HA MCP tools use the
same `HomeAssistantMCPService.register_handlers` path; `ask_openclaw` is re-registered after the MCP
handlers exactly as before.

**Decision — instructions:** `instructions` (+ `memory_instructions()`) → `session.instructions`.
pipecat's `LLMService` appends its "ASYNC TOOLS" guidance whenever a tool has
`cancel_on_interruption=False` (i.e. always here); `_compose_system_instruction` is overridden so the
live model's prompt stays the persona. The backend model gets `BACKEND_PREAMBLE` (voice-context
framing from OpenAI's Live prompting guidance: transcripts may be wrong, use tools, never claim
success without a tool result, plain text) followed by the same persona text, so the tool rules /
"never guess" / language rules reach the model that actually calls the tools. Reversible in one line
if a separate backend prompt is wanted later.

**Decision — voice:** Live accepts `marin` (default), `cedar` and the GPT-Live voices (`quartz ripple
vesper willow stone gleam meridian bossa tempo beacon delta cinder`, from the "Managing GPT-Live
sessions" docs, 2026-09-16). Realtime-only names (`alloy ash ballad coral echo sage shimmer verse`)
are mapped to `marin` with a warning — an invalid voice fails `session.start` and the device would
be deaf. Unknown custom names pass through with a warning (expert escape hatch; the API error is
loud). Not verified against the API from here (no key used) — `tools/live_probe.py --voice X` does
that in seconds.

### Session bootstrap (phase 3)
pipecat starts the Live session on the first `LLMContextFrame` (its examples queue an
`LLMRunFrame` on client connect). This pipeline has no such trigger, so `build_pipeline` hands the
aggregator pair's `LLMContext` to the service (`set_bootstrap_context`) and the service calls
`_handle_context()` right after `StartFrame` (the WebSocket is already connected in `setup()`, as in
phase 1). Cached messages from the previous connection are already inside that context (SessionManager
restores them, capped by `max_context_messages`) and become the session's `input` history — so the
Realtime-only `ContextInitializer` replay is skipped on Live. Wake-gated connect/disconnect is
unchanged: the device holds one WebSocket, the Live session lives as long as the connection (or until
recovery reconnects it in place).

### Turn handling — SERVICE-DRIVEN, as for Realtime (phase-1 decision 1 carried over)
**Finding:** the Live API has no `speech_started/stopped`. pipecat derives user turns from
`session.input_transcript.delta` fragments (a `_TurnGrouper` per role, 0.8 s quiet gap) and then
broadcasts `ProposedUserStarted/StoppedSpeakingFrame` for the aggregator's `UserTurnController` — the
exact path phase 1 bypasses. So the override hooks into the grouper instead of a VAD event:

- `_open_turn("user")` (first fragment of a user turn) → one `UserStartedSpeakingFrame` downstream.
- `_end_turn("user")` (0.8 s gap, or close on reconnect) → final `TranscriptionFrame` upstream (as
  pipecat does) + one `UserStoppedSpeakingFrame` downstream.
- Assistant turns keep pipecat's `LLMFullResponseStart/TTSStarted … TTSStopped/LLMFullResponseEnd`.
- **No interruption broadcast** (plan: `enable_interruptions=False`): the live model yields to
  talk-over itself, a delegation must keep running, and the device mic is gated during a reply
  anyway (`barge_in:false`). The aggregator pair keeps the inert external strategies +
  `realtime_service_mode=True` from phase 1, so it never resolves proposals; the user transcript is
  written to the context when the assistant response starts (pipecat's realtime-mode handoff), which
  is the right fit for service-driven frames.

Consequences for the device contract: `listening` arrives on the first transcript fragment (a few
hundred ms after speech onset rather than on VAD onset — still well inside the firmware's 7 s
no-speech watchdog); `thinking` on the 0.8 s gap unless the bot is already `replying` (PhaseEmitter
case C keeps `replying`); `replying`/`idle` come from the output transport's `BotStarted/Stopped`
as before; the wedge watchdog keys on the same `UserStartedSpeakingFrame` stamp; the dangling-VAD
guard can never fire on Live (a stop always follows a start) and is inert.

### Output audio gate (new, Live-specific)
pipecat pushes Live output as `SpeechOutputAudioRawFrame` because the model streams at real-time
pace **including silence**; the output transport derives `BotStopped` from 0.35 s of silent frames.
The Voice PE treats every binary frame as reply audio, so a continuous silent stream would keep it
in playback forever (no idle, no follow-up window, no `stop` re-arm). `_handle_evt_audio_delta`
forwards speech, keeps forwarding silence for `OUTPUT_SILENCE_HANGOVER_S = 0.5` s after the last
speech chunk (enough for the transport's 0.35 s detector), then drops silence until speech resumes.
Net effect at the device: the same "audio comes in bursts per utterance" shape gpt-realtime produced.
If the API in fact sends no inter-utterance silence, the gate is a no-op.

### Device "stop"
Live has no `response.cancel` for speech and no input buffer. `handle_device_interrupt()` mutes this
utterance's output locally (until the assistant turn closes, the next user turn starts, or 10 s) and
sends `session.instructions.append(delegation_id=null, "The user said stop. Stop speaking
immediately…")` — the documented way to interrupt speech in progress. The device has already silenced
playback itself; the mute keeps PhaseEmitter honest (`BotStopped` → `idle` instead of a `replying`
LED while the model finishes a sentence nobody hears). The post-stop "racing response kill" of the
Realtime path has no Live equivalent and is not wired (nothing to cancel).

`{"type":"start"}` and the follow-up `{"type":"flush"}` are no-ops on Live (no uncommitted buffer;
the device closed its mic). Speaker verdicts go in as `session.thinking.append` (quiet context).

### Recovery, expiry, wedge watchdog, disconnect tool
- `SafeLiveLLMService._receive_task_handler` wraps pipecat's loop and pushes `live receive loop
  died/ended …` (ConnectionRecovery now matches `"receive loop"`). Send-side death floods
  (`Error sending client event …` + close-code marker) are matched as before.
- Unexpected `session.closed` (reason `expired`, `content`, `connection_lost`) pushes `live session
  closed by server (reason=…) — session_expired` → immediate reconnect, even if the socket lingers.
- `session.started.expires_at` is stored as `service.session_expires_at`; ConnectionRecovery's
  quiet-time refresh fires when `now ≥ expires_at − 5 min` (or the 55-min age rule), never while
  `service.is_busy()` (open user/assistant turn, open function call, pending delegated response,
  or output speech in the last 2 s).
- `reset_conversation()` is pipecat's (close open turns → drop socket → reconnect → `session.start`
  from the current context, so the conversation so far is re-seeded as history), flagged so the old
  reader's end is not reported, and the processor is marked usable again afterwards.
- Wedge watchdog: unchanged (12 s after a wake with no `UserStartedSpeakingFrame` →
  `force_reconnect`). `disconnect_client` tool: unchanged (transport-level, opt-in).
- Teardown drops the socket without `session.close` (as Realtime does); final usage is therefore
  not confirmed by the server — cosmetic, `session.usage.updated` snapshots are logged at INFO as
  `💰 live usage: Ns` for the phase-4 cost check.

### Options that do not apply to Live
`vad_eagerness`, `turn_detection_type`/`vad_*`, `openai_speed`, `max_output_tokens`,
`noise_reduction`, `transcription_model`, `transcription_language` — logged once at start as not
applicable; nothing invented to map them.

## Verification

- `PYTHONPATH=. python -m pytest tests` → **21 passed** (13 phase-1 + 8 new), and
  `python -m unittest discover -s tests` → `Ran 21 tests … OK`, on Python 3.12 / pipecat-ai 1.10.0.
  `-W error::DeprecationWarning` (ignoring CPython's `audioop`) is clean for both smoke tests.
- `tests/test_pipeline_smoke_live.py` (no network; only `_connect` stubbed):
  1. `session.start` payload: model `gpt-live-1`, `delegation.type == "responses"`,
     `responses.model == live_backend_model`, backend instructions start with `BACKEND_PREAMBLE`, no
     flat `delegation.model`, `audio.output.voice == marin`, persona instructions without the
     "ASYNC TOOLS" block, tool list == the Realtime list (same 9 names, same `{type,name,description,
     parameters}` shape, no `strict`); every handler `cancel_on_interruption=False`; a second context
     frame does not restart the session.
  2. Cached history is seeded as `input` (`input_text` / `output_text` parts).
  3. Two user transcript fragments → exactly one `UserStartedSpeakingFrame` (downstream); closing the
     turn → one upstream `TranscriptionFrame` ("turn on the lamp") + one `UserStoppedSpeakingFrame`;
     no `Proposed*`, `broadcast_frame`/`broadcast_interruption` never called.
  4. Output gate: idle silence dropped; speech + hangover silence forwarded as 24 kHz
     `SpeechOutputAudioRawFrame`; post-hangover silence dropped; device stop → mute +
     `session.instructions.append(delegation_id=null)`; next user turn lifts the mute.
  5. Voice/model resolution (Realtime-only → marin; Live names pass; custom passes).
  6. ConnectionRecovery reconnects on `live receive loop ended/died` and `live session closed …`,
     not on a delegated-response failure.
  7. End-to-end on a fake Voice PE through `serve_connection`: same 13-processor topology
     (`SafeLiveLLMService` in place of `SafeRealtimeLLMService`, no RTVI), `hello`/`pong`,
     `session.start` is the first client event, a synthetic `session.started` is answered and then
     16 kHz PCM flows as `session.input_audio.append` at 24 kHz, no Realtime-only events, clean
     teardown and context cached.
  8. Selecting `gpt-realtime-2` still produces the phase-1 `session.update` (`type realtime`, model,
     instructions, exact `audio` block, same tool names, no `delegation`, pre-seeded context).
- **Docker**: `docker build --platform linux/amd64 -f openai_realtime_voice_agent/Dockerfile
  --build-arg BUILD_FROM=ghcr.io/home-assistant/amd64-base-debian:bookworm -t voicepe-live1-port:amd64-live2
  openai_realtime_voice_agent/` on this Mac → **exit 0**, 639 MB, `pip install` 145 s, wheels only.
  Inside the image (Python 3.11, amd64): `python3 -m unittest tests.test_pipeline_smoke_live
  tests.test_pipeline_smoke` → `Ran 11 tests … OK`.
- `run.sh`: `bash -n` clean; YAML of `config.yaml` and both translations parses.
- `tools/live_probe.py`: exits 2 with a clear message when `OPENAI_API_KEY` is unset; `--help` works.
  **Not run against the API** (no key used, by instruction).

## Live-regression checklist for the coordinator (Live-1 on the office PE)

Pre-deploy, from the repo with the key in the environment:
1. `OPENAI_API_KEY=… python3 openai_realtime_voice_agent/tools/live_probe.py --with-tool` →
   `session.started ✅`, `delegation.type: responses`, `backend model: gpt-5.4-mini`, `expires_at`
   ~1 h out. Optionally `--voice cedar` / `--voice vesper` to confirm the voice list, and
   `--voice alloy` to see the API's rejection text (the add-on maps that one to marin).

Then set `openai_model: gpt-live-1` (leave `live_backend_model` unset for gpt-5.4-mini), start the add-on:
2. Startup log shows `🛰️ GPT-Live mode: …` and, per device connect, `🛰️ GPT-Live session: …` then
   `🟢 Live session sess_… started (model=gpt-live-1, voice=marin, expires in ~60 min)`.
3. Wake + "what time is it": LED goes `listening` shortly after you start talking, `replying` while
   it answers, `idle` ~1.5 s after — confirm the device does NOT stay in `replying` (that would mean
   the silence gate is not catching the stream; log `_dropped_silence_frames` if needed).
4. HA control: "turn the office lamp on/off" → `🔧`/MCP handler log lines, the lamp changes, the
   spoken confirmation matches reality (backend function call → `response.item.create` +
   `response.create` in debug logs).
5. `recall_memory` / `ask_openclaw` via the bridge (long call: LED stays `thinking` while the tool is
   in flight — the liveness wrapper is shared).
6. Timers: set / list / cancel; expiry announcement still plays (announce lane is unchanged).
7. Announce endpoint while idle and while the model is mid-reply.
8. Device "stop" mid-reply: playback stops, log `🛑 device interrupt → output muted + stop
   instruction appended`, phase goes to `idle` within ~5 s, next wake works normally.
9. Follow-up window: answer back without the wake word within 8 s; then let one window time out
   (`🧽 follow-up cut-off (GPT-Live: nothing to clear)`) and confirm no stale answer follows.
10. Long question (> 5 s) — must not flip to `thinking` mid-sentence.
11. Speaker gate (if configured): a `male_only_tools` tool still refuses for the other voice.
12. Session lifetime: leave it for > 55 min quiet → `🔄 proactive session refresh …` then a new
    `🟢 Live session … started`; or force it by talking past `expires_at` → `live session closed …
    reason=expired` → reconnect within ~5 s and the next turn works.
13. Cost: note `💰 live usage: Ns (cumulative)` and the backend token lines; compare $/min with
    gpt-realtime-2 (plan phase 4).
14. Switch back to `openai_model: gpt-realtime-2` → identical phase-1 behaviour (no Live code runs).

## Open risks / things to watch

1. **Continuous-silence assumption.** The output gate assumes (from pipecat's implementation notes)
   that Live streams silence between utterances. If it does, the 0.5 s hangover / 0.35 s transport
   detector give ~1.5–2 s from end of speech to `idle` (same order as Realtime). If the model's
   speech has intra-sentence pauses > 0.5 s, `BotStopped` fires mid-reply and PhaseEmitter's 1.5 s
   idle debounce absorbs it — as with Realtime's per-sentence segments. Worth listening for a
   premature `idle` (LED) on long answers.
2. **Late `listening`.** On Live the `listening` phase follows the first transcript fragment, not
   speech onset. The firmware lifts its post-stop mute only on `listening`, so a very short
   utterance right after a "stop" could be transcribed but the device's first reply chunk might
   still be suppressed — same class of race as before, slightly wider window. Regression item 8.
3. **"Stop" is advisory.** The Live API has no speech cancel; the appended instruction usually stops
   the model within a phrase. Locally the utterance is muted so the user never hears the tail, but
   the model's transcript/context keeps what it "said". If the model tends to ignore the instruction,
   `session.input_audio.mute`/`unmute` is the next lever (not wired: it does not stop speech either).
4. **Session-start failures do not auto-retry.** A rejected `session.start` (bad voice/backend model,
   quota) is logged loudly and audio is dropped; the next wake's 12 s wedge watchdog reconnects and
   retries. Deliberate: a 5 s reconnect loop on a misconfigured install would hammer the API.
5. **Backend prompt = preamble + persona.** Cheap (cached prefix) but the persona's speaking-style
   rules are read by a text model. If backend behaviour looks off, give it its own prompt (one-line
   change in `_build_live_service`).
6. **Context ordering on reconnect.** With full duplex the assistant can start before the user's
   final transcript lands; pipecat's realtime-mode handoff defers the user-message write and forces it
   at response end, so the cached history is complete but a user/assistant pair could occasionally be
   swapped. Only affects the `input` history re-seeded on reconnect (max 128 items per API limit,
   `max_context_messages` cap applies).
7. **No graceful `session.close` on teardown** → final `session.closed` usage not received (cumulative
   `session.usage.updated` snapshots are logged instead). Also true for reconnects (pipecat abandons
   the old session on purpose so in-flight delegations don't drain into the new one).
8. **Web search stays a client function tool** (add-on's own Responses call), as the plan says
   "same tool handlers". The Live backend could use the built-in `{"type":"web_search"}` directly —
   cheaper and faster — a phase-4/5 option, not done here.
9. **Half-duplex device.** The Voice PE gates its mic during `replying` (`barge_in:false`), so the
   model's full-duplex listening only matters during the follow-up window; barge-in remains the
   "stop" wake word. Cost-wise Live bills per second of session audio while the connection is up,
   including idle time — check item 13 against the plan's ~$0.05/min estimate.
10. **Device "stop" arms nothing else.** The Realtime kill-window state (`_kill_next_response`,
    dangling-VAD guard) is still updated but has no effect on Live; harmless, kept to avoid
    forking the handler wiring.

## Commits

`22350ca` gpt-live-1: SafeLiveLLMService, factory branch and device hooks ·
`5d3d5dc` tests: no-network smoke test for the gpt-live-1 path ·
`90c660e` 0.17.0-live.2: gpt-live-1 option, live_backend_model, docs ·
`0344ad0` tools: live_probe.py ·
`cc6905c` live: restore usability after reconnect, count pending delegations as busy, guard odd PCM ·
this commit: docs: phase-2 report.
