# Gemini Live provider — phase 1 report

*Branch `gemini-live-port` (fork `myzona/voicepe-realtime`), base `live1-port`, add-on
`openai_realtime_voice_agent` **0.17.0-live.3 → 0.17.0-live.4**. Brief: `docs/GEMINI-LIVE-PHASE1-BRIEF.md`.
Neither `gpt-realtime-2` nor `gpt-live-1` code paths changed behaviour —
`tests/test_pipeline_smoke.py` is byte-for-byte untouched and still pins the `gpt-realtime-2`
payload. Nothing was built or installed on the HA VM, no running add-on was touched, no API key
was used from here (the optional live-smoke step in the brief was not run — see §6).*

## 1. What changed

| Area | Change |
|---|---|
| `pyproject.toml` | `pipecat-ai` extras gained `google` (pulls in `google-genai>=1.68.0`, plus `google-cloud-speech`/`texttospeech`, unused). `poetry.lock` **not** updated — no `poetry` binary in this environment; the Dockerfile installs straight from `pyproject.toml` (`pip3 install /tmp/app_build`, comment: "No poetry.lock is used, matching upstream behaviour"), so the lock going stale is cosmetic, not a build risk. |
| `config.yaml` | New `llm_provider: list(openai\|gemini)` (default `openai`), `gemini_api_key: password?` (default `""`), `gemini_model: str` (default `gemini-3.8-live`), `gemini_voice: str` (default `Charon`), `gemini_thinking_level: list(low\|medium\|high)` (default `low`). `openai_api_key` schema changed from `password` to `password?` (still enforced as required by `main.py` when `llm_provider == openai`) so a Gemini-only install doesn't have to fill in a dummy OpenAI key in the HA UI. Version → `0.17.0-live.4`. |
| `root/run.sh` | Exports `LLM_PROVIDER`, `GEMINI_API_KEY`, `GEMINI_MODEL`, `GEMINI_VOICE`, `GEMINI_THINKING_LEVEL` unconditionally (all have config defaults). `OPENAI_API_KEY` export changed from a hard `bashio::config` + startup `exit 1` to a plain export with no run.sh-level required check (it's optional in the schema now) — `main.py::initialize()` does the "required when openai" check instead, so it can be conditional on `llm_provider`. |
| `app/live_mode.py` (new) | `LiveModeService` — an empty marker mixin. `websocket_handler.build_pipeline`'s `live_mode` check and `Application._preseed_context` now key off `isinstance(service, LiveModeService)` instead of the concrete `SafeLiveLLMService` class, so a second live-style provider needs no changes to that shared code. `SafeLiveLLMService(LiveModeService, OpenAILiveLLMService)` — the only change to `live_service.py`; the mixin's MRO position shifted `SafeLiveLLMService.__mro__[1]`, so `tests/test_pipeline_smoke_live.py`'s one use of that index was fixed to import `OpenAILiveLLMService` directly instead (test behaviour unchanged, still patches the same real method). |
| `app/gemini_live_service.py` (new) | `SafeGeminiLiveLLMService(LiveModeService, GeminiLiveLLMService)` + `openai_tools_to_gemini()` + `model_supports_thinking_level()`. See §2 for the design decisions; the module docstring covers the same ground in the code. |
| `app/main.py` | Provider parsing (`LLM_PROVIDER`, the four `GEMINI_*` options) in `initialize()`, with the `🔮 Gemini Live mode` startup log line (parallel to `🛰️ GPT-Live mode`); `openai_api_key` required only when `llm_provider == openai`, `gemini_api_key` required when `gemini`; `web_search` is disabled with a warning if `llm_provider == gemini` and `openai_api_key` is blank (web_search always calls OpenAI's Responses API — see `web_search_tool.py` — regardless of the conversational provider). `create_openai_service` branches on `getattr(self, "llm_provider", "openai")` first (before the existing `is_live_model(self.model)` check) — the `getattr` default is required because `tests/test_pipeline_smoke.py`'s `configure()` builds an `Application()` and sets attributes individually without calling `initialize()`, so it never sets `llm_provider`, and that test's whole point is pinning `gpt-realtime-2` unchanged. `_build_gemini_service(all_tools)` added next to `_build_live_service`. `_preseed_context` already worked via the `LiveModeService` marker — no separate Gemini-specific branch needed. |
| `tests/test_gemini_live_smoke.py` (new) | 14 tests, no network (see §4). |
| `tests/test_pipeline_smoke_live.py` | One line fixed (see `app/live_mode.py` row above); no assertions changed. |
| Docs | `DOCS.md` §4b, `docs/configuration.md`, translations en/nl, `CHANGELOG.md`, this report, the brief. |

Pipeline topology is unchanged (same 13 processors as the `gpt-realtime-2`/`gpt-live-1` paths,
`SafeGeminiLiveLLMService` in the `SafeRealtimeLLMService` slot, no `RTVIProcessor`,
no `ContextInitializer` — confirmed by `test_pipeline_topology_and_run`).

## 2. How it works (and the decisions behind it)

### Gemini is not a delegation architecture

Unlike GPT-Live-1 (a live voice model that *delegates* every tool call to a separate Responses
backend model), Gemini Live calls the registered tools **itself** — the same shape as
`gpt-realtime-2`. Consequence: `instructions` (+ `memory_instructions()`) reach the Gemini model
unmodified; there is no backend prompt, no `BACKEND_PREAMBLE`/`BACKEND_TOOL_RULES` equivalent, and
`live_instructions()`'s delegation/no-filler rules were **deliberately not reused** — that text is
written for "a backend model holds all the tools", which is false for Gemini and would be actively
misleading in its system prompt. If the `-extended-thinking` filler the probe observed
("let me check") turns out to matter with the real tool set, a Gemini-specific one-line
instruction is a follow-up, not something guessed at here without a live test.

### Session bootstrap differs from GPT-Live-1

pipecat's `GeminiLiveLLMService.setup()` calls `_connect()` unconditionally — the websocket opens
and `session.start`-equivalent config (system instruction + tools) is sent using the
**init-provided** values immediately, before any pipeline context exists. This is unlike
`OpenAILiveLLMService`, which waits for the first `LLMContextFrame` to send anything at all — so,
unlike `SafeLiveLLMService`, `SafeGeminiLiveLLMService` did **not** need a `process_frame(StartFrame)`
override just to get instructions/tools sent.

What it still needs `set_bootstrap_context()`/a `StartFrame` hook for: cached conversation history
from a reconnecting device. pipecat only re-seeds history from an `LLMContextFrame`, and this
pipeline never queues one (no `LLMRunFrame` on connect) — so `SafeGeminiLiveLLMService` is handed
the aggregator pair's context and calls `_handle_context()` itself right after `StartFrame`, exactly
the `SafeLiveLLMService` pattern. `inference_on_context_initialization=False` (passed at
construction in `_build_gemini_service`) stops pipecat's own "seed with the system instruction to
trigger a first response" behaviour for a **brand-new** session (no cached messages) — without it, a
fresh connection would seed a `developer`-role message from the system instruction and immediately
generate a spoken reply to it, i.e. the assistant would greet the room unprompted on every connect.
Traced through pipecat's `_handle_context`/`_create_initial_response` source directly (not just the
docstring) to confirm this before deciding: with the flag off and an empty context, `_create_initial_response`
returns immediately (`if not messages: ...; return`) with no message ever seeded; with cached
messages present, they are seeded via `send_client_content(turn_complete=False)` — history restored,
no reply forced. Verified with a live construction + connect test against the real `google-genai`
types (not just reading the source) — see §4.

### Tools: OpenAI-native dicts → `FunctionSchema`/`ToolsSchema`

The add-on's tool list is built once, in OpenAI's flat
`{"type":"function","name",...,"parameters"}` shape, and handed to whichever provider's service is
being constructed. Gemini's `tools=` kwarg accepts a `ToolsSchema`/`FunctionSchema` list (which its
own adapter turns into `functionDeclarations`) or Gemini-native dicts — **not** OpenAI's shape.
`openai_tools_to_gemini()` converts each OpenAI dict to a `FunctionSchema`, wrapped in a
`ToolsSchema`. Confirmed empirically (not assumed) against the installed `google-genai` 2.24.0 and
pipecat's `GeminiLiveLLMAdapter`:
- `FunctionSchema.to_default_dict()` emits lowercase JSON-Schema types (`"object"`/`"string"`); the
  probe script (raw websocket) had sent uppercase (`"OBJECT"`/`"STRING"`) by hand. Constructing a
  `google.genai.types.Tool`/`LiveConnectConfig(tools=[...])` from the lowercase dicts and dumping it
  back out shows the SDK's own `Type` enum normalizes lowercase to uppercase on serialization — no
  manual case conversion needed.
- All of a `ToolsSchema`'s `standard_tools` land under **one** `{"function_declarations": [...]}`
  dict (not one dict per tool) — confirmed by constructing a two-function `ToolsSchema` and running
  it through the adapter directly.

### `cancel_on_interruption=False` and Gemini 3.x

Both OpenAI service classes force `cancel_on_interruption=False` on every registered tool (so a
mid-call interruption never kills an in-flight HA/web-search request), wrapped through the shared
`app/tool_guard.py::guarded_tool_handler` (speaker gate + turn-liveness). `SafeGeminiLiveLLMService.register_function`
does the same, matching the brief's explicit contract. One consequence, read directly from
pipecat's source: `gemini-3.8-live` is a "Gemini 3.x" model (`_is_gemini_3` checks for `"gemini-3"`
in the model string), and Gemini 3.x does not yet support pipecat's NON_BLOCKING tool-scheduling
hints for that flag — so pipecat logs a one-time **cosmetic** warning
(`"cancel_on_interruption=False is not properly supported by the current Gemini Live model"`) the
first time any async-tool-style result lands, and `push_error()`s it. Left unhandled, that
generic-text `ErrorFrame` would reach `ConnectionRecovery`, match none of its death markers, fall
into the "turn ended on error" branch, and force the device to `idle` mid-tool-call — a real bug.
`SafeGeminiLiveLLMService.push_error()` swallows only that specific message text; every other error
still reaches `ConnectionRecovery` normally. The tool call itself still completes and its result is
still delivered correctly — Gemini just doesn't get the "keep talking while it runs" scheduling
hint, which is a Gemini 3.x platform limitation, not something this add-on can work around.

### Turn frames: service-driven, simpler than GPT-Live-1

pipecat's own `GeminiLiveLLMService` class docstring states explicitly that it emits no
`UserStartedSpeakingFrame`/`UserStoppedSpeakingFrame` at all (confirmed by reading the class, not
just the docstring — grepped for both frame types in the whole file: they only appear in
`process_frame`'s *inbound* handling, never pushed by the service itself). So, like GPT-Live-1:
- The **first** `input_transcription` fragment while the bot is not responding
  (`not self._bot_is_responding`) opens the turn: exactly one `UserStartedSpeakingFrame` downstream.
- The model starting to respond (`_set_bot_is_responding(True)` — wherever it first fires, from
  either audio or output-transcription) closes it: exactly one `UserStoppedSpeakingFrame`.
- A fragment that arrives while the bot **is** responding (a late tail of the question, or echo)
  still reaches the aggregator via pipecat's own untouched transcription handling, but does not
  announce a new turn — simpler than `SafeLiveLLMService`'s `BOT_SPEAKING_GRACE_S` heuristic,
  because Gemini's `_bot_is_responding` is a real state flag, not a silence-based guess.
- A defensive fallback closes a still-open turn on `turn_complete` if the model somehow finished
  without ever responding, so the device can't get stuck in `listening` forever.

The **assistant-side** turn frames (`TTSStartedFrame`/`LLMFullResponseStartFrame` …
`TTSStoppedFrame`/`LLMFullResponseEndFrame`) needed **no changes at all** — pipecat's own
`_handle_msg_model_turn`/`_handle_msg_output_transcription`/`_handle_msg_turn_complete` already
emit exactly the same frame sequence `SafeLiveLLMService` has to emit by hand for OpenAI. This
means `PhaseEmitter`'s `replying`/idle-debounce logic works for Gemini with zero additional code.

### Output audio gate

Ported from `SafeLiveLLMService` (same `OUTPUT_SILENCE_HANGOVER_S = 0.5`, same post-"stop" mute
pattern): pipecat's `_handle_msg_model_turn` is the single method that builds `TTSAudioRawFrame`
from `inline_data`, and it also handles text/thought frames and grounding metadata inline, so there
is no smaller seam to intercept *only* the audio part. `SafeGeminiLiveLLMService._handle_msg_model_turn`
duplicates the method (documented in its docstring, with a re-diff instruction for a pipecat
upgrade) and inserts the silence-gate check immediately before the final `push_frame(TTSAudioRawFrame(...))`.
**Not verified against the real API** whether Gemini in fact streams continuous silence between
utterances the way GPT-Live-1 does — if it doesn't, the gate is a harmless no-op; if it does and the
gate weren't here, the device would never see a gap long enough to leave `replying`. Open question
(§7).

### Session resumption and `goAway`

pipecat's `GeminiLiveLLMService` already stores the resumption handle
(`_handle_msg_resumption_update`, on `session_resumption_update`) and already passes it to every
`_connect()` its own `_reconnect()` triggers — **no override was needed** to preserve resumption
across a reconnect; this was verified directly (constructed a service, called
`_handle_msg_resumption_update` with a synthetic `LiveServerSessionResumptionUpdate`, then called
`_reconnect()` with `_connect` replaced by a capturing stub, and confirmed the stored handle was
passed through — see §4).

`goAway` (the server's advance warning of an imminent forced close) has **no handler anywhere in
pipecat** — grepped the entire installed package for `go_away`/`goAway`/`GoAway`: zero matches
outside the `google-genai` SDK's own type definitions. The whole receive loop
(`_connection_task_handler`) is one method with no smaller seam, so `SafeGeminiLiveLLMService`
duplicates it (documented, with a re-diff instruction) and adds one branch: on `message.go_away`,
log `time_left` and spawn a background task that polls `is_busy()` (quiet-gated, bounded to
`GO_AWAY_MAX_WAIT_S = 20s`) before reconnecting proactively — reusing the same resumption handle
pipecat's own reconnect already knows how to pass along. `time_left` is an opaque duration string
(e.g. `"10s"`) and is only logged, not parsed, to avoid taking on parsing fragility for a value
that's just a soft deadline; if the house never goes quiet in time, the server's own forced close
falls through to pipecat's ordinary connection-error handling.

### Recovery: Gemini already self-heals; ours is the last resort

Read directly from pipecat's source: `GeminiLiveLLMService._handle_connection_error` already
retries the connection up to `MAX_CONSECUTIVE_FAILURES = 3` times, reusing the session resumption
handle, entirely on its own — a structural difference from **both** OpenAI classes, which have zero
built-in reconnect logic (that's the entire reason `ConnectionRecovery` exists). Only once pipecat
gives up does it `push_error()`, with a generic message
(`"Max consecutive failures (3) reached, treating as fatal error"`) that matches none of
`ConnectionRecovery`'s death markers (`"receive loop"`, `"session_expired"`, `"maximum duration"`,
`"live session closed"`) — deliberately, so a merely-transient drop that pipecat is already retrying
never reaches `ConnectionRecovery` and gets treated as a turn-ending error. `SafeGeminiLiveLLMService._handle_connection_error`
adds our own `"gemini live receive loop died: …"` marker **only** on the give-up path, and
`reset_conversation()` (the one public method `ConnectionRecovery` calls) delegates to pipecat's own
`_reconnect()`. This does mean the give-up path pushes two `ErrorFrame`s (pipecat's generic one,
then ours) — `ConnectionRecovery`'s 5 s reconnect cooldown collapses the resulting duplicate
attempt, same as it already does for other double-signal cases (e.g. a send-flood plus a
session-dead marker arriving close together).

`session_expires_at` stays `None` — Gemini's `LiveServerMessage` carries no session-expiry field
anywhere (checked the full message schema) — so `ConnectionRecovery` falls back to its existing
age-based (55 min) proactive refresh, same as it would for a service that never sets the attribute
at all.

### Device "stop"

Gemini Live has no equivalent of OpenAI's `response.cancel` or `session.instructions.append`. The
closest lever — sending real-time audio input to trigger the server's own barge-in detection (the
`interrupted` server signal) — needs audio that resembles speech, which nothing produces on a
spoken "stop" (the device already discards its own mic input at that point, same as both OpenAI
paths). `handle_device_interrupt()` therefore: mutes this utterance's output locally
(`_output_muted_until`, same pattern/constant as `SafeLiveLLMService`) and calls
`broadcast_interruption()` so the **local** pipeline (assistant aggregator, output transport) treats
the reply as over. The remote Gemini session may keep generating (and billing) audio for the muted
utterance — **not verified from here** (§7).

### Quiet context injection (speaker verdicts)

Gemini has no channel equivalent to OpenAI's `session.thinking.append` (context with no reply
triggered). The closest primitive already in pipecat's own code is
`session.send_client_content(turn_complete=False)` — the exact call `_create_initial_response` uses
to seed history *without* triggering inference — reused here for `inject_context()`.

## 3. Options not applicable to Gemini

Same set as GPT-Live-1, for the same reason (full-duplex, server-managed turns): `vad_eagerness`,
the `server_vad` fields, `openai_speed`, `max_output_tokens`, `noise_reduction`,
`transcription_model`/`transcription_language`. `openai_api_key` is still consulted (for
`web_search`, which always calls OpenAI's Responses API regardless of the conversational
provider); if it's blank while `llm_provider: gemini`, `web_search` is disabled with a startup
warning rather than failing the whole add-on over a tool that isn't the reason someone picked
Gemini.

## 4. Verification

- `cd openai_realtime_voice_agent && ../.venv/bin/python -m pytest -q tests` → **40 passed**
  (26 pre-existing + 14 new), Python 3.12.12 / pipecat-ai 1.10.0 / google-genai 2.24.0.
- `PYTHONPATH=. ../.venv/bin/python -m unittest discover -s tests` → `Ran 40 tests … OK`.
- `git diff origin/live1-port -- openai_realtime_voice_agent/tests/test_pipeline_smoke.py` → empty.
- `bash -n root/run.sh` clean; `config.yaml` and both translations parse as YAML.
- New in `tests/test_gemini_live_smoke.py` (no network; only `_connect`/`_connection_task_handler`
  stubbed, real `google-genai` types used throughout so the payload assertions are against the
  actual SDK's config objects, not hand-rolled stand-ins):
  1. `test_provider_selection` — `llm_provider: gemini` → `SafeGeminiLiveLLMService`; default
     (`llm_provider` unset, mirroring `configure()`) → unchanged `SafeRealtimeLLMService`;
     `openai_model: gpt-live-1` → unchanged `SafeLiveLLMService`.
  2. `test_session_start_payload_and_tools` — the real `LiveConnectConfig` built at `_connect()`:
     no `thinking_config` for `gemini-3.8-live`, voice `Charon` under
     `generation_config.speech_config.voice_config.prebuilt_voice_config`, system instruction
     equals the test persona, `tools[0]["function_declarations"]` names match
     `EXPECTED_TOOL_NAMES` (imported from `test_pipeline_smoke_live.py`, same list the OpenAI
     paths use); every registered handler has `cancel_on_interruption=False`.
  3. `test_extended_thinking_model_sends_thinking_level` — `gemini-3.8-live-extended-thinking` →
     `thinking_config.thinking_level == ThinkingLevel.LOW`.
  4. `test_model_supports_thinking_level`, `test_openai_tools_to_gemini_conversion` — unit tests
     for the two standalone helpers.
  5. `test_session_resumption_handle_stored_and_reused_on_reconnect` — a synthetic
     `LiveServerSessionResumptionUpdate` is stored, and `_reconnect()` calls `_connect()` with that
     exact handle.
  6. `test_go_away_triggers_quiet_reconnect_with_handle` — a `go_away` while busy does not
     reconnect immediately; once quiet, the pending task reconnects; a second `go_away` while one
     reconnect task is already pending does not spawn a duplicate.
  7. `test_async_tool_warning_is_suppressed` — the known-benign Gemini 3.x message is swallowed by
     `push_error()` (logged at DEBUG instead), everything else would still go through.
  8. `test_user_turn_frames_are_service_driven`, `test_fragment_while_bot_responding_does_not_open_a_turn`,
     `test_turn_complete_with_no_reply_closes_a_dangling_turn` — the turn-frame contract from §2,
     using real `LiveServerMessage`/`Transcription` objects.
  9. `test_output_audio_gate_drops_inter_utterance_silence` — idle silence dropped, speech +
     hangover silence forwarded as 24 kHz `TTSAudioRawFrame`, post-hangover silence dropped again,
     device "stop" mutes until the mute is lifted.
  10. `test_input_clock_feeds_silence_while_mic_is_gated` — mirrors the GPT-Live-1 phase-4 test:
      idle → nothing; a turn in flight + gated mic → 100 ms of 24 kHz zeros per tick (capped at
      500 ms); device audio flowing → nothing; `note_device_mic_closed()` releases the tail;
      `InputAudioRawFrame`s stamp the clock.
  11. `test_pipeline_topology_and_run` — end-to-end on a fake Voice PE through `serve_connection`:
      the same 13-processor topology with `SafeGeminiLiveLLMService` in the Realtime slot, no
      `RTVIProcessor`, `hello`/`pong` handshake, clean teardown, context cached for session reuse.

### `root/run.sh` verification (review round 1 fix)

`root/run.sh` is shell plumbing between the HA Supervisor's config UI and the Python process; none
of the 40 tests above can see it (they set `Application` attributes directly, bypassing `run.sh`
and `os.environ` entirely). Review round 1 caught two real bugs here that shipped in the phase-1
commit: `OPENAI_API_KEY=$(bashio::config 'openai_api_key')` had been dropped from the Basics block
while the `if [ -z "$OPENAI_API_KEY" ]; then exit 1; fi` check stayed — meaning the add-on would
exit 1 on startup for **every** install, both providers, since the variable was now always empty
— and `LLM_PROVIDER`/`GEMINI_API_KEY`/`GEMINI_MODEL`/`GEMINI_VOICE`/`GEMINI_THINKING_LEVEL` were
read via `bashio::config` but never `export`ed, so `main.py` would never see them and the Gemini
path was unreachable in a real image regardless of the option UI. Both are fixed: the key read is
restored in place, the required-key check is now provider-aware (mirrors `main.py`'s own check,
keeping the exact `"OPENAI_API_KEY is required but not set"` log text people grep for), and all
five are exported.

Proved by running the real `run.sh` (not a rewritten copy) through a minimal `bashio` stub
(`bashio::config KEY` → `$CFG_<KEY>` uppercased, `bashio::config.has_value` checks the same, empty
by default) with `bash -n` first, then swapping the shebang for a plain `#!/bin/bash` + `source`
of the stub and replacing the final `exec python3 -m app.main` with a dump of the relevant
exported variable *names* (values never printed):

```
case A: openai, key set        -> OPENAI_API_KEY=<set>, LLM_PROVIDER=<set>, GEMINI_*=<set/empty>, exit=0
case B: openai, key blank      -> "ERROR: OPENAI_API_KEY is required but not set", exit=1
case C: gemini, gemini key set, openai blank -> LLM_PROVIDER/GEMINI_* exported, exit=0
case D: gemini, gemini key blank             -> "ERROR: gemini_api_key is required when llm_provider is gemini", exit=1
```

All four matched the required behaviour (default/openai install still starts with just an OpenAI
key; a blank required key for the *selected* provider fails loudly with a clear message; the
Gemini-only case no longer needs an OpenAI key to pass validation; every one of the five
provider-related variables reaches the Python process's environment). `bash -n root/run.sh` on the
real file is clean.

## 5. How to flip a device to Gemini (options UI)

1. Set **`llm_provider`** to `gemini`.
2. Set **`gemini_api_key`** (a Google AI Studio key — this is a *different* key from
   `openai_api_key`; keep `openai_api_key` filled in too if you still want `web_search` to work).
3. Leave `gemini_model` (`gemini-3.8-live`), `gemini_voice` (`Charon`) and `gemini_thinking_level`
   (`low`, unused unless you pick an `-extended-thinking` model) at their defaults for the first
   try.
4. Restart the add-on. Startup log should show `🔮 Gemini Live mode: model=gemini-3.8-live, ...`.
5. Everything else (Home Assistant control, timers, memory, enrollment, announce, speaker
   detection) is unchanged — same tool set, same device protocol.

## Deploy steps (Edward does this; not run from here)

1. Copy the add-on directory to `/mnt/data/supervisor/apps/local/<slug>` on HA VM 140
   (`192.168.1.107`).
2. `ha store reload`
3. `ha apps update` (or Settings → Add-ons → the local add-on → Update/Rebuild in the UI, since
   this add-on has no published image and builds locally from the Dockerfile).
4. Flip one device's `llm_provider` to `gemini` per §5 above and work through the regression items
   below before considering it for the office puck full-time.

## 6. What was NOT verified (no live API call was made)

- The optional live-smoke step in the brief (`tools/gemini_live_probe.py` against the real Gemini
  API with the add-on's actual tool schemas) was **not run**. The offline work — tool schema
  conversion, `LiveConnectConfig` construction, resumption/goAway wiring — was validated against
  the installed `google-genai` 2.24.0 SDK's real types (not hand-rolled stand-ins) wherever
  possible (see §4), which is the strongest evidence available without a key from this
  environment. A live probe run is the natural next step before the office puck regression.
- Whether `gemini-3.8-live` actually streams continuous silence between utterances (the premise
  the output gate is built on) — see §7.
- Whether the Gemini 3.x `cancel_on_interruption=False` cosmetic warning (§2) is truly harmless in
  practice under real tool traffic, beyond the unit-level confirmation that `push_error()` swallows
  it.
- The exact wire acceptance of lowercase-then-SDK-uppercased JSON-Schema types for a *nested*
  object parameter (all our real tool schemas were exercised through the adapter and produced the
  expected shape locally, but only Gemini's own server can confirm it accepts them).

## 7. Open questions (per the brief)

1. **Idle WebSocket lifetime.** GPT-Live-1 dropped every ~258 s. Does Gemini Live drop an idle
   connection on a similar cadence, and does `goAway` reliably arrive with enough `time_left` to
   reconnect proactively before that happens? Not measurable without a live session.
2. **Per-second vs. per-turn billing.** GPT-Live-1 bills per second of session audio (including
   input-clock silence, ~10 s/turn). Gemini's Live API pricing model (and whether the input clock's
   silence counts the same way) is not established here.
3. **Does the 258 s (or any) reconnect gap reproduce on Gemini**, and if it does, is the resumption
   handle actually honored server-side the way pipecat's client-side handling assumes (i.e. does the
   restored session genuinely retain context, or does `_handle_session_ready`'s "no re-seed needed"
   branch leave a gap)?
4. **The Gemini voice list.** Only `Charon` (the default, and pipecat's own default) was used here;
   the full set of accepted prebuilt voice names for `gemini-3.8-live` was not enumerated or probed.
5. **Continuous-silence assumption for the output gate** (§2) — if Gemini's real behaviour differs
   from GPT-Live-1's (e.g. no silence between utterances at all, or gaps shorter/longer than
   `OUTPUT_SILENCE_HANGOVER_S`), the device's idle/follow-up timing could be off in either
   direction; only listening to a real session will show it.
6. **Device "stop" while Gemini keeps generating.** Confirmed by design that the *device* stops
   hearing/playing anything after `handle_device_interrupt()`, but whether the *remote* Gemini
   session keeps producing (and billing) audio for the muted utterance is unknown — OpenAI's
   `SafeLiveLLMService` has the same open question in its own report for the analogous case.
7. **`-extended-thinking` filler with the real tool set.** The probe observed a "let me check"
   filler and ~6-10 s latency on `low` thinking; whether that's acceptable, and whether a
   Gemini-specific no-filler instruction line is worth adding, needs a live regression with actual
   household questions, not a guess baked into the prompt now.

## Commits

`8b63360` gemini: llm_provider option, google extra, run.sh plumbing ·
`1329666` gemini: SafeGeminiLiveLLMService + shared live-mode marker mixin ·
`65cf8d7` gemini: wire the provider branch into main.py and websocket_handler ·
`573cc80` tests: no-network smoke tests for the Gemini Live path ·
this commit: docs: gemini live phase-1 report + brief, changelog, configuration docs.
