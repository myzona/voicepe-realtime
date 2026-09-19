# Gemini 3.8 Live provider for the Voice PE add-on — phase 1

Repo: `myzona/voicepe-realtime` (fork of `TristanBrotherton/voicepe-realtime`). Base branch:
`live1-port`. You are in an Orca worktree; your branch must be named **`gemini-live-port`**
(rename with `git branch -m` if Orca gave it another name). Author = Edward (`myzona`) — the
worktree already has `user.name`/`user.email` set; **never add `Co-authored-by`, `Generated with`,
or any AI-attribution trailer** to any commit or PR body.

**Read first**, in this order (read-only, absolute paths — the main checkout is not yours to edit):
1. `/Users/edwardkhytrykh/src/voicepe-realtime/AGENTS.md` — fork brief, production state, house rules.
2. `docs/LIVE1-PORT-PLAN.md`, `docs/LIVE1-PHASE2-REPORT.md`, `docs/LIVE1-PHASE4-REPORT.md` (in your worktree).
3. `openai_realtime_voice_agent/app/live_service.py` (`SafeLiveLLMService`) — the thing you are mirroring.
4. `openai_realtime_voice_agent/tests/test_pipeline_smoke_live.py` — the test pattern you extend.
5. `/Users/edwardkhytrykh/.openclaw/workspace/scripts/gemini-live-probe.py` — the exact Gemini
   setup payload that was verified live on 2026-09-19 (read it; do NOT run it — it needs the key).

## 0. Environment (do this before anything else)

The worktree has no venv. Build one with uv (fast, cached on this Mac):

```bash
cd <worktree>
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python ./openai_realtime_voice_agent pytest
cd openai_realtime_voice_agent && ../.venv/bin/python -m pytest -q tests     # must be 26 passed before you touch anything
```

`.venv/` is already in `.gitignore`. After you add the `google` extra (step 1), re-run the
install so `google.genai` imports. Both `pytest` (local) and `PYTHONPATH=. python -m unittest
discover -s tests` (what CI runs) must pass at the end.

## Why

The office Voice PE runs `gpt-realtime-2` in production. GPT-Live-1 (`SafeLiveLLMService`) was
ported but is parked as too laggy (phase-4 report). A raw-websocket probe of **`gemini-3.8-live`**
on 2026-09-19 gave: `setupComplete` 0.3 s, first audio ~1.0 s, tool call for a device question at
0.8 s with no guessing, spoken answer done at 3.1 s, output `audio/pcm;rate=24000`. That is the
latency we want, so Gemini Live becomes a selectable provider next to the two OpenAI paths.

Also verified by the probe (bake these in, don't rediscover):
- `gemini-3.8-live-extended-thinking` **requires** `generationConfig.thinkingConfig.thinkingLevel`
  — the server closes with 1007 without it. At `low` it speaks a "let me check" filler, tool call
  ~6 s, answer ~10 s. Plain `gemini-3.8-live` must NOT be sent a thinkingConfig.
- The server emits `sessionResumptionUpdate` handles (`resumable: true`) and can send `goAway`.

## What pipecat 1.10.0 already gives you (verified in the installed package — read the source)

`pipecat/services/google/gemini_live/llm.py`, class `GeminiLiveLLMService(LLMService[GeminiLiveLLMAdapter])`:
- `Settings` = `GeminiLiveLLMSettings` with `model`, `voice`, `system_instruction`, `language`,
  `thinking: ThinkingConfig | dict | NotGiven`, `context_window_compression`, `proactivity`, …
  In `_connect()` a dict `thinking` becomes `google.genai.types.ThinkingConfig(**thinking)` and is
  set as `config.thinking_config`; an empty dict/NotGiven sends nothing. The SDK field is
  `thinking_level` (snake_case) — confirm it exists in the installed `google-genai` and that it
  serializes to `thinkingLevel`.
- `__init__(api_key=…, settings=…, tools=…, system_instruction=…, inference_on_context_initialization=…)`.
  `tools=` accepts a `ToolsSchema`, a list of `FunctionSchema`, **or a list of provider-native
  dicts**. Our tool list is the OpenAI-native dict form (`{"type":"function","name",…,"parameters"}`);
  Gemini wants `functionDeclarations`. Convert to `FunctionSchema`/`ToolsSchema` in our
  subclass — do not hand the OpenAI dicts through and hope.
- Session resumption is built in: `_session_resumption_handle` is stored from
  `session_resumption_update`, `_reconnect()` → `_connect(session_resumption_handle=…)`,
  `_handle_session_ready` skips history re-seeding when a handle is present.
- **`goAway` is NOT handled anywhere in pipecat's google services** (grep confirms). The receive
  loop is `_connection_task_handler` (~line 1310). You must add it: on `message.go_away`, log the
  `time_left`, and reconnect proactively with the stored handle *while the house is quiet*
  (coordinate with `is_busy()`; never mid-turn). Prefer the smallest override that keeps the
  parent loop's other branches intact.
- Output audio is pushed at the hard-coded `self._sample_rate = 24000` (`_handle_msg_model_turn`).
  The pipeline already runs at `PIPELINE_SAMPLE_RATE = 24000` (`websocket_handler.py`) and the
  OpenAI paths push 24 kHz too — confirm in `multi_client_transport.py` / `raw_audio_serializer.py`
  that nothing else needs to change, and write down what you found in the report.
- Input: `_send_user_audio` sends frames with `mime_type=f"audio/pcm;rate={frame.sample_rate}"`.
  Frames reach the service at 24 kHz (InputResampler). Fine.
- Extras: `Requires-Dist: google-genai<3,>=1.68.0; extra == "google"` (plus google-cloud-speech /
  texttospeech which we do not need). Add the `google` extra to the `pipecat-ai` line in
  `openai_realtime_voice_agent/pyproject.toml`; update `poetry.lock` **only if you can do so
  reproducibly** (the Dockerfile ignores the lock — see its comment — so a stale lock is a
  cosmetic problem, a wrong lock is not; say in the report which you did).

## The contract: mirror `SafeLiveLLMService`

`app/gemini_live_service.py` — `SafeGeminiLiveLLMService(GeminiLiveLLMService)`. The rest of the
add-on talks to the service through **duck-typed hooks**; every one of these must exist and behave
as in `live_service.py` (read that file's implementations, they document the why):

| Hook | Called from | Meaning |
|---|---|---|
| `set_bootstrap_context(ctx)` | `websocket_handler.build_pipeline` (the `live_mode` branch) | Live-style services start from the aggregator's context instead of a `ContextInitializer` |
| `handle_device_interrupt()` | device `{"type":"interrupt"}` | mute this utterance + ask the model to stop (Gemini: send `activity_end`/text nudge — pick what works; the device already discards audio) |
| `note_device_mic_closed()` | device `{"type":"flush"}` | follow-up window over: end the input-clock tail early |
| `inject_context(text)` | speaker verdicts | quiet context (no reply) |
| `is_busy()` | `ConnectionRecovery._service_busy` | never refresh/reconnect mid-turn |
| `register_function(name, handler, *, cancel_on_interruption=…)` | `Application.create_openai_service` | **must** go through `app/tool_guard.py::guarded_tool_handler` and force `cancel_on_interruption=False`, exactly like both OpenAI classes |
| attributes `speaker_probe`, `male_only_tools`, `turn_liveness`, `session_id`, `session_expires_at` | set by `create_openai_service`, read by tool_guard / ConnectionRecovery | |

`build_pipeline` decides `live_mode = isinstance(openai_service, SafeLiveLLMService)`. Introduce
a small shared marker (a base mixin/protocol, or a `live_mode` attribute) so the Gemini service is
treated as live-mode too **without changing the OpenAI classes' behaviour**.

**Input clock** — the load-bearing piece. The Voice PE firmware mutes its mic during `replying`
and outside wake/follow-up windows, and a Live model only advances while input audio arrives.
`SafeLiveLLMService._input_clock_loop/_input_clock_tick/conversation_active` feed real-time-paced
PCM16 silence (24 kHz) while a turn, tool call, or reply is in flight and no device audio has
arrived for `INPUT_CLOCK_GAP_S`. Port it 1:1 (same constants, same log lines) against Gemini's
`session.send_realtime_input(audio=Blob(...))`. "Conversation active" for Gemini = user turn open
or bot responding (`_bot_is_responding`) or tool calls in flight (`_tool_call_id_to_name` /
function-call bookkeeping) or the activity tail.

**Phase events** — `PhaseEmitter` derives `listening/thinking/replying/idle` from
`UserStartedSpeakingFrame`/`UserStoppedSpeakingFrame` and bot speaking frames; the context
aggregator needs `TranscriptionFrame` (UPSTREAM) + `UserStoppedSpeakingFrame`. pipecat's Gemini
service deliberately does **not** emit UserStarted/Stopped (see the comment in its receive loop),
so you do it, service-driven, like `SafeLiveLLMService._open_turn/_end_turn`: first input
transcription fragment while the bot is not speaking → ONE `UserStartedSpeakingFrame`
(`listening`); model turn begins / `generationComplete` → close the user turn (`TranscriptionFrame`
upstream + ONE `UserStoppedSpeakingFrame`); bot audio → `TTSStartedFrame`/`LLMFullResponseStartFrame`
… `TTSStoppedFrame`/`LLMFullResponseEndFrame` on `turnComplete` (the parent already does part of
this — don't double-emit). No `ProposedUser*`, no interruption broadcast (the device has
`barge_in:false`; a late transcript fragment during the reply must not flip the phase — see the
`bot_is_speaking()` guard). Port the **output silence gate** too (drop continuous inter-utterance
silence after `OUTPUT_SILENCE_HANGOVER_S`, honour the post-"stop" mute) or the device never
reaches idle.

**Reconnect/recovery** — `ConnectionRecovery` (in `websocket_handler.py`) keys on `ErrorFrame`
text: `"receive loop"` (reader died), `"session_expired"` / `"maximum duration"` /
`"live session closed"` (session gone). Gemini's parent handles transport errors with its own
`_reconnect()` (up to `MAX_CONSECUTIVE_FAILURES`) — decide deliberately whether to let the parent
reconnect (with the resumption handle) and only surface an ErrorFrame with one of those markers
when it gives up, and document the choice. `session_expires_at`: Gemini has no `expires_at`;
leave it `None` so `ConnectionRecovery` falls back to the age-based proactive refresh, and make
the refresh path reuse the resumption handle.

**Tools** — same list as the OpenAI paths (`create_openai_service` assembles it: enrollment,
false-wake, timers, memory, web_search function tool, direct `ask_openclaw`/`recall_memory`, HA
MCP tools). Gemini function declarations need JSON-schema types the API accepts; the probe used
`"type": "OBJECT"` / `"STRING"` (uppercase) — check what pipecat's adapter emits for a
`FunctionSchema` and test with the real tool schemas from `test_pipeline_smoke.py`
(`EXPECTED_TOOL_NAMES`). Keep the **ordering rule** from `create_openai_service` (direct
`ask_openclaw` re-registered after MCP handlers).

## Deliverables

1. **Options** (`config.yaml` options + schema, `translations/en.yaml` and `nl.yaml`, `DOCS.md`,
   `docs/configuration.md`, `root/run.sh` env export):
   - `llm_provider: list(openai|gemini)`, default `openai` → existing installs unchanged.
   - `gemini_api_key: password` (default `""`), `gemini_model: str` (default `gemini-3.8-live`),
     `gemini_voice: str` (default `Charon`), `gemini_thinking_level: list(low|medium|high)` (default
     `low`; **only sent when the model name contains `extended-thinking`**).
   - Supervisor validates the whole schema at once: **every existing key stays**, new keys get
     defaults so old option blobs still validate. Follow the `🗣️ Model & voice` grouping/prefix
     convention in translations. `main.py` reads them as `LLM_PROVIDER`, `GEMINI_API_KEY`, …
     `openai_api_key` must remain required only when `llm_provider == openai` (the web_search
     function tool still needs it — decide + document what happens when provider is gemini and
     the OpenAI key is blank; the simplest honest answer is "web_search is disabled with a
     warning").
2. **`app/gemini_live_service.py`** per the contract above. Module docstring in the style of
   `live_service.py`: what the service does, the input clock, turn handling, resumption/goAway.
3. **`app/main.py`**: `_build_gemini_service(all_tools)` next to `_build_live_service`; provider
   branch in `create_openai_service` (and `initialize()` logging like the `🛰️ GPT-Live mode`
   line). System instruction = `self.instructions + memory_instructions()` (same as the OpenAI
   paths — reuse `live_instructions()` if its no-filler rules make sense for Gemini, and say so).
   `_preseed_context` must treat the Gemini service like the Live one. Zero behaviour change when
   `llm_provider == openai` — `test_pipeline_smoke.py` pins that; do not edit its expectations.
4. **Tests** in `openai_realtime_voice_agent/tests/`, `unittest.IsolatedAsyncioTestCase` style
   like the existing smoke tests, no network (stub `google.genai` `aio.live.connect`):
   - provider selection: `LLM_PROVIDER=gemini` → `SafeGeminiLiveLLMService`; default → unchanged
     `SafeRealtimeLLMService`; `gpt-live-1` → unchanged `SafeLiveLLMService`;
   - the `LiveConnectConfig` built for `gemini-3.8-live` has **no** `thinking_config`; for
     `gemini-3.8-live-extended-thinking` it has `thinking_level == <option>`;
   - system instruction, voice, tool declarations (names = `EXPECTED_TOOL_NAMES`) land in the
     connect config;
   - every registered handler has `cancel_on_interruption=False` and is guard-wrapped;
   - resumption: a `session_resumption_update` handle is stored and the next `_connect` is
     called with it; a `go_away` message triggers a reconnect that passes the handle;
   - input clock feeds silence while the device mic is gated and stops when device audio resumes
     (mirror the phase-4 test in `test_pipeline_smoke_live.py`);
   - the pipeline builds and runs end-to-end on the fake device with the Gemini service
     (`build_pipeline` live-mode branch, no `ContextInitializer`, no RTVIProcessor).
   Existing 26 tests stay green, under both pytest and `unittest discover`.
5. **`docs/GEMINI-LIVE-PHASE1-REPORT.md`** (pattern: `docs/LIVE1-PHASE4-REPORT.md`): what was done
   with commit list, exact deploy steps (copy the add-on dir to
   `/mnt/data/supervisor/apps/local/<slug>` on HA VM 140, `ha store reload`, `ha apps update` —
   Edward does this, you only write it), how to flip a device to Gemini in the options UI, what
   you verified vs. did not, and **open questions**: idle WebSocket lifetime (GPT-Live-1 dropped
   every ~258 s — does Gemini, and does `goAway` give warning?), per-second vs per-turn billing,
   whether the 258 s reconnect gap reproduces, the Gemini voice list, and anything in the
   resumption behaviour you could not test offline. Also commit this brief as
   `docs/GEMINI-LIVE-PHASE1-BRIEF.md` (copy it verbatim; the repo keeps briefs next to reports).
6. **Version**: `config.yaml` `version:` → `0.17.0-live.4` and a `CHANGELOG.md` entry (newest
   first, suffixed `(fork)`).

## Optional live smoke (only if the offline work is complete and green)

The Gemini key lives at `~/.openclaw/workspace/.secrets/google-gemini.env` (`GOOGLE_API_KEY=…`).
You may write a **short** `tools/gemini_live_probe.py` (sibling of `tools/live_probe.py`) that
reads the key **in Python from that file**, opens ONE `SafeGeminiLiveLLMService` session with the
add-on's real tool schemas and one text turn, and prints timings — proving our config/tool
conversion is accepted by the real API. Rules: the key never appears in argv, env dumps, logs,
test output, or the report; never commit the `.env`; one or two runs, not a loop. If it fails,
report the server error text (with the key redacted) rather than iterating blindly.

## House rules that are easy to break here

- **Do NOT change `gpt-realtime-2` or `gpt-live-1` behaviour.** If a refactor touches
  `live_service.py`/`main.py` shared code, the existing tests must not need edits.
- Do not deploy, do not touch the HA VM, do not read the add-on's live options/logs.
- Tuned constants in this repo were measured on hardware and the comments say why — copy them,
  do not "improve" them.
- Comments explain *why*; match the surrounding density and emoji-log style. No drive-by
  reformatting. Minimal diffs.
- Docs move with code (`docs/configuration.md` for every new option; `DOCS.md` short version).
- Commit in coherent steps (deps/options → service → wiring → tests → docs/report), messages in
  the repo's style (`gemini: …`, `docs: …`, `tests: …`), **no trailers**.

## Proof before you push

- `cd openai_realtime_voice_agent && ../.venv/bin/python -m pytest -q tests` → all green, paste
  the summary line into the report.
- `PYTHONPATH=. ../.venv/bin/python -m unittest discover -s tests` → also green.
- `git log --format='%an <%ae>%n%b' origin/live1-port..HEAD` shows only Edward and no
  `Co-authored-by`/`Generated` lines.
- `git diff origin/live1-port -- openai_realtime_voice_agent/tests/test_pipeline_smoke.py` is empty.

## Ship

`git push -u origin gemini-live-port`, then
`gh pr create --base live1-port --title "Gemini 3.8 Live provider (phase 1)" --body-file <summary>`
— body: what/why, test summary lines, the open questions list, pointer to the report. **No
attribution footer.** Do not merge, do not enable auto-merge; Edward reviews. Finish by
reporting `worker_done` with the PR URL, the final test counts, and anything you left undone.
