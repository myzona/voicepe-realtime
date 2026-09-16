# GPT-Live-1 port — phase 4 report: latency / quality fixes

*Branch `live1-port` (fork `myzona/voicepe-realtime`), add-on `openai_realtime_voice_agent`
0.17.0-live.2 → **0.17.0-live.3**. Brief: `docs/LIVE1-PHASE4-BRIEF.md`; evidence:
`docs/live1-lag-log-20260916.txt` (office PE, 10:23–10:32 PDT). The `gpt-realtime-*` path is
untouched (pinned by `tests/test_pipeline_smoke.py`). Nothing was built or installed on the HA VM,
no running add-on was touched, no API key was used from here. Ergo was not reachable from the
worktree — decisions are recorded in this document.*

## 1. Root cause of the 17 s stall — the Live model has no clock of its own

**Finding.** The GPT-Live model advances its timeline only while it receives input audio. It
speaks, and "hears" a delegated result, in lockstep with the audio the client appends; when no
`session.input_audio.append` arrives, nothing happens server-side. The `💰 live usage` counter in
the log proves it — it counts session audio, and it moved at exactly the rate the device mic was
open, not at wall-clock rate:

| wall clock | usage | Δusage / Δwall | device mic during the window |
|---|---|---|---|
| 10:23:34.7 → 10:23:49.7 | 70 s → 78 s | 8 s / 15 s | open (follow-up window → listening → thinking), closed from `replying` at 10:23:39.9 |
| 10:23:49.7 → 10:24:04.7 | 78 s → 80 s | **2 s / 15 s** | closed the whole time, except 10:24:01.8–10:24:03.3 (watchdog idle → follow-up mic) |
| 10:24:04.7 → 10:24:19.7 | 80 s → 91 s | 11 s / 15 s | closed while replying, open in the follow-up window until the `flush` at 10:24:18.5 |
| 10:24:19.7 → 10:28:34 | 94 s flat | 0 | closed (idle) — OpenAI then dropped the idle connection at 10:28:39 |

The Voice PE is half-duplex by design (`va_client.cpp` streaming gate): the mic streams during
`listening`/`thinking` and in the follow-up window after `idle`, and is switched **off on
`replying`** — and `thinking` after `replying` does *not* switch it back on. The web-search turn
therefore went:

```
10:23:38.7  user speech ends ("… Pleasant Hill, California")        mic open
10:23:39.3  session.delegation.created                              +0.7 s
10:23:39.9  model vocalises "[hum]" → BotStarted → replying         +1.3 s   → firmware gates the mic
10:23:40.8  backend function_call web_search                        +2.1 s
10:23:42.5  "[hum]" ends → BotStopped → (1.5 s) thinking                       mic stays gated
10:23:46.1  add-on's Responses web_search returns (5.2 s), function_call_output
            + response.create sent — CORRECT and complete                       server clock frozen
10:24:01.1  PhaseEmitter thinking-watchdog forces idle → firmware opens the
            follow-up mic (~10:24:01.8) → audio flows again
10:24:03.3  answer audio starts                                     1.5 s after the mic re-opened
```

The continuation message was fine (`response.item.create` + `response.create`, the same path the
HA MCP tool took), the pending-delegation state machine had nothing batched, the output silence
gate forwards the first speech chunk immediately, and no error event was received: the answer was
simply never generated until audio arrived. The HA-tool turn (10:23:29–32) did not stall because
its result landed ~1 s after the mic closed, while the server still had buffered input to chew on;
the 5 s web search overshot that buffer. In short: **every reply that takes longer than a couple of
seconds after the model has started speaking would stall until the mic re-opens** — the
"[hum]" filler made this hit every tool turn.

**Fix — input clock (`SafeLiveLLMService._input_clock_loop`).** While the conversation is active
and no device audio has arrived for 0.3 s, the service appends real-time-paced silence (24 kHz
PCM16 zeros, ≤ 0.5 s per 100 ms tick, so the session timeline tracks wall clock). "Active" =
a user or assistant transcript turn is open, a function call is running, a delegated response is
pending, or there was activity (device audio, speech, transcript, delegation/response event) in the
last 12 s. Idle rooms are **not** fed (session audio is billed per second); the device's follow-up
`flush` releases the tail immediately, so the cost is ≈ 10 s of silence per turn (~$0.01 at the
plan's $0.05/min). Log lines: `🔇 input clock: device mic gated — feeding silence …` /
`🔇 input clock: stopped after N.Ns of silence (device audio resumed | conversation quiet …)`.

**Why not the firmware?** Keeping the mic open during `replying` (barge-in) is a firmware change
with the echo problems that shelved barge-in before; feeding silence from the add-on is truthful
("the user is silent"), needs no device change and works for every half-duplex client.

**Also fixed on this path**

- pipecat leaves a delegated response that made **no** function call in `_pending_responses`
  forever (`_maybe_continue_response` returns early on `not had_calls`), so `is_busy()` was True
  from the first plain answer on and the proactive session refresh could never run — the session
  would have hit `expired` mid-conversation. Released in `_handle_evt_response`.
- Backend progress (`response.event/*`) now ticks the thinking-watchdog liveness, so a long
  built-in web search with no client tool in flight cannot be force-idled at 15 s.
- `💰 live usage` is logged once per change instead of every 15 s while idle.

**Web search on Live now uses the Responses built-in tool** (`{"type": "web_search"}` in
`delegation.responses.tools`, option `live_builtin_web_search`, default on): the backend
(`gpt-5.4-mini`) searches server-side and answers in the same response — no
function_call → client → second Responses call (gpt-5.5, 5.2 s) → output → `response.create` →
continuation round trip. The add-on's `web_search` handler stays registered (Realtime, and the
fallback): if `session.start` is rejected while the built-in tool is in the payload, the service
flips to the function tool once and restarts the session itself (`⚠️ session.start failed with the
built-in web_search tool — restarting …`), so a deaf device cannot result. Not verified against the
API from here — `tools/live_probe.py --builtin-web-search` does it in seconds (pipecat's
`ResponsesDelegation` docs state hosted tools run server-side).

## 2. Wrong time ("4:06 p.m." at 10:23) — the live model never delegated

The backend never saw the question: the *live* model answered from its own head. The backend's
tool rules can only help once a delegation happens, so both prompts changed:

- **Live model** (`session.instructions` = persona + `LIVE_DELEGATION_RULES`): it "knows no
  current time or date, weather, news, or the state of any device, sensor or timer"; for every such
  question or request — `"what time is it"` named explicitly — it must delegate and relay, never
  estimate; every smart-home action is delegated and confirmed only after the backend reports.
- **Backend model** (`delegation.responses.instructions` = `BACKEND_PREAMBLE` +
  `BACKEND_TOOL_RULES` + persona): "You have no clock … For the current time, date or day ALWAYS
  call `<time tool>` first … never guess, estimate or compute the time yourself"; device state →
  Home Assistant tools (`GetLiveContext`, `Hass*`); weather/news/prices → web search; answer only
  after the tool results. `<time tool>` is the registered tool's real name (`llm__GetDateTime` on
  the office install — `time_tool_name()` finds any `*GetDateTime`), falling back to "the date/time
  tool".
- `tool_choice` stays `auto`: `required` would force a tool call on every backend response,
  including the continuation after a function output (a loop). `reasoning.effort` **is** now sent
  (see §4) — better tool selection than the unconfigured default at lower latency.

Tested (no network): the payload's backend instructions contain the rule and the time-tool name,
the live instructions end with the delegation rules and mention "what time is it".

## 3. "[hum]" filler and phase flapping

**Filler.** pipecat's Live settings model (`OpenAILiveLLMSettings`: `voice` only) and the
`session.start` schema it implements (`SessionConfig`: model, instructions, audio {output.voice,
format}, delegation, input) carry **no** filler/backchannel/thinking-sound knob, and nothing in the
add-on log excerpt hints at one. So it is instructed: `LIVE_DELEGATION_RULES` ends with "While the
backend works, stay silent: do not hum, murmur, make filler sounds or announce that you are
checking. Speak the answer as soon as it arrives, in one short reply." To let the coordinator see
whether an undocumented knob exists, `🟢 Live session … started` now prints every top-level field
the server echoes in `session.started` (minus instructions/input/audio/delegation), and
`live_probe.py` prints `other fields`. Measure: with the input clock in place a filler no longer
causes a stall, so this is now a cosmetics/latency item (the filler costs ~1.3 s of `replying`
before the real answer and one extra mic gate).

**Flapping.** Two mechanisms, both fixed in `SafeLiveLLMService`:

1. `thinking → listening → thinking` (10:23:38.67 → 38.68 → 39.49): pipecat closes a user turn
   after **0.8 s** without a transcript fragment; the pause before ", California" was 0.8 s + 16 ms.
   The user turn gap is now **1.5 s** (`USER_TURN_GAP_S`). The live model decides on its own when
   to answer, so this only delays the `thinking` LED and the context write — never the reply.
2. `replying → listening` on a fragment while the bot speaks (10:23:05–08): a user fragment that
   arrives while the assistant transcript turn is open or within 1 s of the last speech chunk is a
   late tail of the question (or echo). It is still recorded (TranscriptionFrame → context) but
   **not announced**: no `UserStartedSpeakingFrame` (so no `listening`, which would have re-opened
   the device mic into the reply — the firmware turns streaming on for every `listening`), no
   `UserStoppedSpeakingFrame` (no `thinking`), and it does not lift a device-stop mute.
   Log: `🎙️ user transcript while bot speaking — turn not announced`.

PhaseEmitter itself is unchanged (its case C already keeps `replying` on a stop while replying).

## 4. Per-turn latency

Measured from `docs/live1-lag-log-20260916.txt` (user speech end = last transcript fragment,
estimated from the turn-close time − 0.8 s gap):

| turn (Live-1, 0.17.0-live.2) | speech end → delegation | → tool call | tool | result → first audio | speech end → first audio |
|---|---|---|---|---|---|
| "temperature upstairs" (HA MCP) | n/a in excerpt | n/a | n/a | ~1.3 s (result ~10:23:29–30 → 10:23:31.2) | n/a (filler "One second." at 10:23:29.9) |
| "temperature outside in Pleasant Hill, California" (web_search) | 0.7 s | 2.1 s (1.4 s after delegation) | 5.2 s (add-on gpt-5.5 web search) | **17.2 s** (stall; 1.5 s after the mic re-opened) | **24.6 s** (filler "[hum]" at +1.3 s) |
| "what time is it" (no delegation) | — | — | — | — | not in the excerpt |

Realtime comparison numbers are **not** in the excerpt nor in the phase-1 notes (those hold no
timings), so no Realtime column is claimed here. To make the next regression produce the table
by itself, the service now logs one line per reply:

```
⏱️ live turn: user→first audio 3.9s, user→delegation 0.7s, user→tool call 1.6s, tool 0.6s, result→first audio 1.0s
```

Expected after the fixes (derived from the measured stage times): a direct answer ≈ 1.0–1.5 s
(the "[hum]" onset shows how fast the live model starts talking); an HA tool turn ≈ 0.7 s
delegation + ~1 s backend (low effort) + MCP + ~1.3 s relay ≈ **3.5–4.5 s**; a web search
≈ 0.7 s + built-in search 3–6 s + 1.3 s ≈ **5–8 s** (was 9 s without the stall, 24.6 s with it).
The ~1.3 s "result → first audio" relay hop and the ~0.7 s "speech end → delegation" are the
Live architecture's fixed tax versus Realtime's single model.

**Knobs (new, all hidden, `str?`/`bool?` in `config.yaml`, exported by `run.sh` only when set):**

| option | default | sent as | note |
|---|---|---|---|
| `live_backend_reasoning_effort` | `low` | `delegation.responses.reasoning.effort` | pipecat only sends `reasoning` when configured, so 0.17.0-live.2 ran on the server default (medium for gpt-5.x) — seconds of thinking per delegation with 40 tools. `none`/`minimal` are faster still; `default` omits the field. Values pass through with a warning if unknown. |
| `live_backend_verbosity` | unset | `delegation.responses.text.verbosity` (via `Settings.extra`) | not verified against the Live API from here → left off by default; probe first. |
| `live_builtin_web_search` | `true` | `{"type":"web_search"}` in `delegation.responses.tools` | see §1; auto-fallback to the function tool. |
| `live_backend_model` (existing) | `gpt-5.4-mini` | `delegation.responses.model` | `gpt-5.4-nano` is the cheaper/faster candidate; not tried. |

**Recommended defaults:** `gpt-5.4-mini` + `low` + built-in web search (as shipped). If the
"→ tool call" stage still measures > 1.5 s in the `⏱️` lines, try `minimal`, then `none`; if
answers are too wordy, probe `--verbosity low` and set `live_backend_verbosity: low`.
`tools/live_probe.py --reasoning-effort low --builtin-web-search` (and `--verbosity low`) validates
each combination in a few seconds — a rejected field is an `error` event (exit 1), never a deaf
device.

## 5. Diagnostics added (so the next log answers these questions itself)

`📡 live evt <type>` at INFO for every server event type (`session.delegation.created` with id /
target / offset, `response.event/<inner type>` with delegation id — `output_item.done` shows item
type/name/status, `completed` shows status/tokens; `*.delta` at DEBUG), `session.*.appended`,
`session.closed reason=…`, unmodelled events with their fields; output audio summarised every 5 s
(`📡 live audio: N output deltas … (S speech, D silent dropped)`); transcript fragments at DEBUG
except the first of a turn (`(turn opens)`); `🎙️`/`🗣️` turn open/close lines; `📤 function call
output sent` / `📤 response.create sent — backend continues delegation …`; `⏱️ live turn`; the
`🟢 Live session … started` line now includes backend model/reasoning/tool count and the echoed
session fields.

## 6. Verification

- `PYTHONPATH=. python -m pytest tests` → **26 passed** (21 + 5 new), `python -m unittest
  discover -s tests` → `Ran 26 tests … OK`, Python 3.12 / pipecat-ai 1.10.0.
- New/extended in `tests/test_pipeline_smoke_live.py` (no network):
  `session.start` payload now pins the live rules, backend tool rules, `reasoning: {effort: low}`,
  no `text`, built-in web_search + 8 function tools; `test_user_fragment_while_bot_speaking_does_not_flip_phase`;
  `test_input_clock_feeds_silence_while_mic_is_gated` (idle → nothing; tool in flight → 100 ms of
  24 kHz zeros per 100 ms, capped at 500 ms; device audio → nothing; flush releases the tail;
  input frames stamp the clock); `test_delegation_bookkeeping_and_event_log` (no-call delegation
  released, liveness ticked, INFO lines for every event type, deltas at DEBUG, continuation path
  `response.item.create` + `response.create` intact); `test_hosted_web_search_falls_back_on_session_start_rejection`;
  `test_live_options_reach_the_payload` (effort none, verbosity low, function-tool mode, time-tool
  name resolution, option normalisation). The end-to-end topology test also asserts the input clock
  ran in the pipeline and fed nothing while the device streamed. Realtime payload test unchanged.
- `config.yaml` and both translations parse; `bash -n root/run.sh` clean;
  `live_probe.py --help` works and exits 2 without a key.
- Docker: `docker build --platform linux/amd64 -f openai_realtime_voice_agent/Dockerfile
  --build-arg BUILD_FROM=ghcr.io/home-assistant/amd64-base-debian:bookworm -t voicepe-live1-port:amd64-live3
  openai_realtime_voice_agent/` → **exit 0** (639 MB); inside the image (Python 3.11, amd64, tests
  mounted): `python3 -m unittest tests.test_pipeline_smoke_live tests.test_pipeline_smoke` →
  `Ran 16 tests … OK`.

## 7. What the coordinator should re-test (Live-1 on the office PE)

Pre-deploy (key in env): `python3 openai_realtime_voice_agent/tools/live_probe.py --with-tool
--builtin-web-search --reasoning-effort low` → `session.started ✅` with `backend tools` listing
`web_search:` and `backend reasoning: {'effort': 'low'}`. Optional: `--verbosity low`.

1. **Web search turn** ("temperature outside in Pleasant Hill"): answer within ~5–8 s, no
   `thinking-watchdog` warning, log shows `🔇 input clock: … feeding silence` while the mic is
   gated and a `⏱️ live turn` line; `response.event/response.output_item.done item=web_search_call`
   instead of a `web_search called:` function log. If the session start had failed on the hosted
   tool: one `⚠️ session.start failed with the built-in web_search tool` warning then a normal start.
2. **"What time is it"** → `session.delegation.created` + `output_item.done item=function_call
   name=llm__GetDateTime` in the log, correct local time spoken.
3. **HA tool** ("turn the office lamp on"): `⏱️ live turn` ≈ 3.5–4.5 s, lamp changes.
4. **Filler**: does a reply still open with "[hum]" / a spoken filler? Note the `other session
   fields` in the `🟢 Live session` line for any undocumented knob.
5. **Phases**: a long question with a pause mid-sentence stays `listening` (no
   listening↔thinking flap); during a reply the LED never flips back to `listening`.
6. **Cost**: `💰 live usage` per turn — expect ≈ (speech + reply + follow-up window + ~10 s of
   input-clock silence) per turn; usage must stay flat while the room is idle.
7. **Long idle**: after ~4–5 min without audio OpenAI drops the connection (`live receive loop
   ended`, seen at 10:28:39) and ConnectionRecovery reconnects in ~0.2 s — check the next wake
   after such a gap still answers normally (pre-existing behaviour, now visible in the log).
8. Switch to `gpt-realtime-2` once: unchanged behaviour.

## 8. Open risks

1. **Built-in web_search acceptance** is assumed from pipecat's docs, not verified; the one-shot
   fallback covers rejection at `session.start`. A rejection mid-session (unlikely: the tool list is
   fixed at start) would surface as a `⚠️ Live API error` and the backend would answer without search.
2. **Input clock cost**: ≈ 10 s of billed silence per turn; a runaway would show as `💰 live usage`
   climbing while idle — the feed stops 12 s after the last activity and on the device `flush`.
3. **The filler is instruction-controlled only.** If "[hum]" persists, it costs ~1.3 s per tool
   turn but no longer stalls anything.
4. **Reasoning `low` on gpt-5.4-mini** is documented for the gpt-5 series but not probed here;
   `none`/`minimal` even less so — hence the probe flags.
5. **Late `thinking`** by 0.7 s (1.5 s gap) — cosmetic; the wedge watchdog and follow-up logic key
   on `UserStartedSpeaking`, which is unchanged.

## Commits

`77617ab` live: input clock, turn gating, tool rules, built-in web_search, event log ·
`c29678b` 0.17.0-live.3: options, docs, probe flags ·
`536b529` live: release the input clock on follow-up cut-off, tick the thinking watchdog ·
`4621ac6` docs: phase-4 report · `ed4fe99` docs: configuration reference.
