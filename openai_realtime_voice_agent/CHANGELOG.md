# Changelog

All notable changes to this add-on. Newest first.

## 0.17.0-live.4 (fork)

- **Gemini Live provider (`llm_provider: gemini`, phase 1).** A new
  `llm_provider` option (`openai`/`gemini`, default `openai` — existing
  installs unaffected) selects Google's Gemini Live API
  (`app/gemini_live_service.py`, `SafeGeminiLiveLLMService` on pipecat's
  `GeminiLiveLLMService`) instead of the OpenAI paths. Unlike GPT-Live-1,
  Gemini is not a delegation architecture — it calls the registered tools
  itself, so `instructions` reach it unmodified, same as `gpt-realtime-2`.
  New options: `gemini_api_key`, `gemini_model` (default `gemini-3.8-live`),
  `gemini_voice` (default `Charon`), `gemini_thinking_level` (default `low`,
  only sent to a `-extended-thinking` model — a plain model rejects the
  connection if sent one at all).
- **Turn frames, input clock, output gate, ported from GPT-Live-1.** Gemini's
  own service emits no `UserStartedSpeakingFrame`/`UserStoppedSpeakingFrame`
  at all; they are derived here from the first input-transcription fragment
  (open) and the model starting to respond (close), same reasoning as
  GPT-Live-1's turn handling. The input clock (same constants, same log
  lines as `SafeLiveLLMService`) feeds silence while the device mic is gated
  so a tool result is spoken immediately instead of waiting for the mic to
  re-open; the output gate drops inter-utterance silence so the device
  reaches idle.
- **Session resumption + `goAway`.** pipecat's Gemini Live service already
  stores and reuses the session resumption handle on every reconnect it
  triggers itself (unlike either OpenAI class, it also self-heals up to 3
  consecutive connection failures on its own). `goAway` — the server's
  advance warning before a forced close — has no handler anywhere in
  pipecat; this adds one that reconnects proactively once the house is
  quiet, reusing the handle.
- **Tool conversion.** The add-on's OpenAI-native tool dicts are converted to
  pipecat's `FunctionSchema`/`ToolsSchema` (`openai_tools_to_gemini`) so
  Gemini's own adapter builds `functionDeclarations` correctly, instead of
  handing it OpenAI-shaped dicts it would not understand.
- `gpt-realtime-*` and `gpt-live-1` behaviour unchanged (`llm_provider`
  defaults to `openai`, and `test_pipeline_smoke.py` is untouched).
- Review fix (never shipped): `root/run.sh` had dropped the
  `OPENAI_API_KEY=$(bashio::config ...)` read while keeping the required-key
  check, which would have exited every install (both providers) at startup;
  and `LLM_PROVIDER`/`GEMINI_*` were read but never `export`ed, so `main.py`
  would never have seen them. Fixed: the key read is restored, the required
  check is provider-aware (mirrors `main.py`), and all five are exported.
- Review fix (never shipped, found live on HA VM 140): a resumed connect
  that Google closes outright (`1011 Internal error encountered`) used to
  escape `_connection_task_handler` uncaught and leave the service
  permanently dead — pipecat's inner `try/except` only covers the message
  loop, not establishing the connection itself. Now caught, drops the stale
  resumption handle, and retries. Separately, the reconnect seed replayed
  tool call/result pairs as mis-ordered text turns ahead of the user
  question that triggered them (a stale "current time" measurably confused
  the model, and is the likely actual cause of the stall above) — the seed
  now carries only user/assistant text.
- See `docs/GEMINI-LIVE-PHASE1-REPORT.md` for what was verified offline vs.
  not, and the open questions (idle session lifetime, per-second vs.
  per-turn billing, the "stop" behaviour, the Gemini voice list).

## 0.17.0-live.3 (fork)

- **GPT-Live: no more stall after a tool call.** The live model only advances
  while it receives input audio, and the Voice PE stops streaming its mic the
  moment a reply starts — so a tool result that arrived after a filler
  utterance was not spoken until the follow-up window re-opened the mic (17 s
  observed). The add-on now feeds real-time-paced silence to the session while
  a turn, function call or delegated response is in flight and no device audio
  is arriving (`🔇 input clock` log lines); idle time is not fed.
- **GPT-Live: backend tool rules + delegation rules.** The backend model is
  told to always call the date/time tool (`GetDateTime`) for time/date, to use
  Home Assistant tools for device state and web search for current facts, and
  to answer only from tool results; the live model is told it knows no time,
  weather, news or device state itself and must delegate (it answered
  "4:06 p.m." from thin air), and not to hum or fill the pause while the
  backend works.
- **GPT-Live: built-in web search** (`live_builtin_web_search`, default on):
  the backend uses OpenAI's Responses `web_search` tool directly instead of the
  add-on's `web_search` function (one hop and one Responses call fewer). Falls
  back to the function tool once if `session.start` rejects it.
- **GPT-Live: latency knobs** `live_backend_reasoning_effort` (default `low`;
  before, the server default applied) and `live_backend_verbosity` (unset);
  `tools/live_probe.py` gained `--reasoning-effort`, `--verbosity`,
  `--builtin-web-search`.
- **GPT-Live: phase flapping fixed.** A user transcript fragment that arrives
  while the reply is playing (late tail of the question, echo) no longer flips
  the device to `listening`/`thinking` (which re-opened the mic mid-reply); the
  user turn gap is 1.5 s so a mid-sentence pause does not split one question.
- **GPT-Live: diagnostics.** Every Live server event type is logged at INFO
  (audio summarised every 5 s, per-token deltas at DEBUG), one `⏱️ live turn`
  line per reply with the stage latencies, usage logged once per change, and
  delegations that made no function call are released (pipecat kept them
  pending, which pinned the busy flag and blocked the proactive refresh).
- `gpt-realtime-*` behaviour unchanged.

## 0.17.0-live.2 (fork)

- **GPT-Live-1 support (`openai_model: gpt-live-1`, `live_backend_model`).**
  Selecting `gpt-live-1` runs the device on OpenAI's full-duplex GPT-Live API
  (`wss://api.openai.com/v1/live/sessions`) with Responses delegation: the live
  model converses, the backend model (`live_backend_model`, default
  `gpt-5.4-mini`, hidden option) runs the SAME tool set as the Realtime path
  (Home Assistant MCP, ask_openclaw/recall_memory, timers, memory, enrollment,
  web_search, disconnect), so device control is unchanged. `instructions` become
  the live session's instructions (and the backend's, behind a short
  voice-context preamble).
  - Voice: `marin` default; Realtime-only voice names are mapped to `marin`
    with a warning instead of failing the session.
  - Device phases keep working: user turns are derived from the live
    transcript and pushed from the service (like the Realtime path), the
    continuous silent output stream of GPT-Live is gated so the Voice PE
    reaches idle / its follow-up window, and a device "stop" mutes the current
    utterance and asks the model to stop.
  - Connection recovery covers Live session expiry (`session.closed`) and a
    dead reader, and refreshes proactively ahead of the session's `expires_at`.
  - Turn detection, speed, reply-length, noise-reduction and transcription
    options do not apply to GPT-Live and are logged as ignored.
  - `gpt-realtime-*` behaviour is unchanged (pinned by the smoke tests).
  - New: `tools/live_probe.py` opens a Live session with `OPENAI_API_KEY` from
    the environment and prints the `session.started` summary (pre-deploy check).

## 0.17.0-live.1 (fork)

- **pipecat 1.10 migration (prep for GPT-Live-1).** The add-on now runs on
  pipecat-ai 1.10.0 (was 0.0.97). Behaviour on `gpt-realtime-2` is meant to be
  identical; this release is the baseline for the upcoming GPT-Live-1 option
  (see `docs/LIVE1-PORT-PLAN.md`, `docs/LIVE1-PHASE1-REPORT.md`).
  - Session configuration sent to OpenAI (model, instructions, voice/speed,
    turn detection, transcription, tool list) is byte-identical to 0.16.11;
    pipecat's own default model changed to gpt-realtime-2.1 but the add-on
    always sets the model explicitly.
  - Device phase signalling (`listening`/`thinking`/`replying`/`idle`),
    interruption handling and tool-call semantics are kept as before by
    driving them from the realtime service itself rather than pipecat 1.x's
    new aggregator-side turn controller (which would swallow or time out
    turn events on this device's stop/flush paths).
  - Home Assistant MCP tools: pipecat 1.x keeps one persistent MCP session
    instead of a connection per call. The add-on reconnects and retries once
    when that session is dead, so HA restarts no longer break device control
    until the add-on restarts.
  - Assistant transcript lines in the log are no longer duplicated (pipecat
    1.x emits the reply text twice, as LLM text and TTS text).
  - `httpx` is now an explicit dependency (openai 3.x no longer pulls it in).

## 0.16.11 (fork)

- Fixed announcements immediately after a single Voice PE reconnect. The sole
  connected device is now addressable before its first wake/audio activity;
  multi-device instances still require activity or an explicit target when
  more than one idle device is connected.

## 0.16.10 (fork)

- Added an opt-in relay-side output lead buffer for the measured Voice PE
  resampler cold-start defect. It holds the first part of a reply and releases
  it as a burst, giving the device a playout lead before normal streaming.
- The buffer is safe across interruption, connection recovery, short replies,
  and mid-reply pauses, with a bounded watchdog for a stalled source. It is
  disabled by default; our two-device deployment enables 400 ms while the
  existing device playback prebuffer remains 250 ms.

## 0.16.9 (fork)

- Added selectable OpenAI transcription models, including `gpt-live-transcribe`
  and `gpt-transcribe`. Both now receive their required `languages` array when
  a transcription language is configured.

## 0.16.8 (fork)

- **Multiple Voice PE devices on one add-on instance**: every connected device
  now has its own OpenAI session, conversation history, audio pipeline, phase
  updates, speaker-recognition state, and enrollment flow. Devices can talk at
  the same time without interrupting or receiving audio from one another.
- Reconnecting a device replaces only its own stale connection; other active
  devices keep their conversations intact.
- Timer announcements and acknowledgements stay with the device that created
  the timer. Targeted announce requests now return an error when that device
  is offline rather than reporting a false success.

## 0.16.7 (fork)

- **Wedge watchdog**: a half-open OpenAI socket (dies silently during an idle
  gap — no close frame, no error) used to swallow the next request entirely:
  audio streamed out, nothing came back, no reply. Now every wake arms a 12 s
  liveness check; if the server VAD shows no activity, the session reconnects
  in place (~3 s). A silent wake triggers a harmless idle-time reconnect.

## 0.16.6 (fork)

- **Fixed: direct `ask_openclaw` silently rebinding to the HA MCP path.**
  pipecat registers a handler for every MCP tool during session creation,
  which overwrote the native direct-path handler — resurrecting the 60-second
  MCP cap ("it failed" while the task actually succeeded). Native registration
  now happens after MCP registration and wins.
- **Announce endpoint repeat guard**: near-duplicate messages within 10
  minutes are accepted but not spoken (`duplicate_suppressed`), so an agent
  monitoring for a result can't re-announce the same news every poll cycle.

## 0.16.5 (fork)

- **Voice prints now build automatically** when enrollment completes — the
  coach confirms out loud, warns when the enrolled name isn't in
  `speaker_male_name`/`speaker_female_name` (recognition stays inactive until
  it is), and asks for a retry when there wasn't enough clear speech.
  Previously this required a manual `python3 -m app.build_voiceprint` step
  that was easy to miss, leaving enrollments silently ineffective.
- New `sensor.voicepe_<instance>_voice_prints`: enrolled prints, with an
  `active` attribute showing which are enrolled *and* configured.

## 0.16.4 (fork)

- **Cost observability**: every response's exact token usage (from the API's
  `response.done`) is logged with an estimated cost, and a
  `sensor.voicepe_<instance>_openai_cost_today` sensor tracks daily spend in
  Home Assistant. Rates auto-switch for mini models.
- Recommended default applied to our install: `max_output_tokens: 1200` —
  output audio is the dominant per-turn meter ($64/1M tokens, measured); a cap
  bounds runaway monologues without touching normal replies.

## 0.16.3 (fork)

- Documentation overhaul: marketing README, `docs/` (getting started,
  configuration reference, features, agent integration, FAQ); repository
  renamed to `voicepe-realtime` (old URLs redirect). `repository.json` now
  carries this project's identity (was still the upstream fork's).
- `enrollment_phrase` default is now "hey leonard" (matches the shipped
  default wake word); HA UI help text added for all fork options.

## 0.16.2 (fork)

- **Guaranteed report-back on long delegations**: ask_openclaw now sends the
  instance name as `room`; the bridge answers "still working" at 120s instead
  of killing the turn, and delivers the agent's eventual answer to that room's
  announce endpoint itself. Previously a >145s research task was reported as
  a failure by voice while the agent kept working with nowhere to deliver.

## 0.16.1 (fork)

- **`recall_memory` tool** (with `openclaw_url`): instant deterministic search
  of the agent's memory files via the bridge (`{"recall": query}` →
  `{"matches": [...]}`). Registered as the FIRST stop for personal/household
  recall; `ask_openclaw` becomes the deep fallback. Fixes recall being a
  40-80s agent turn that found or missed facts depending on phrasing.

## 0.16.0 (fork)

- **Announce endpoint** (`announce_port` + `announce_token` options): a LAN
  route back to the device for the household's external agent. POST
  `/announce {"message": "..."}` (bearer-authed) speaks the message through
  the device's guarded TTS lane — the same path timers use — so a delegated
  task ("research X") can report back by voice minutes later. Disabled unless
  both options are set; 503 when no device is connected.

## 0.15.1 (fork)

- **Direct OpenClaw escalation** (`openclaw_url` option): `ask_openclaw` now
  calls the bridge endpoint directly instead of going through HA's MCP server,
  whose hardcoded 60-second request timeout killed longer agent turns (deep
  memory recall, contact lookups). Direct calls get ~2.5 minutes. Unset, the
  MCP-script path is used unchanged. The speaker gate applies either way.
- (0.10–0.15.0 entries — speaker voice-prints, timers, enrollment v2, HA
  sensors, false-wake flagging, voice-instructed memory — are in git history.)

## 0.9.0 (fork)

- **Firmware-backed voice enrollment** (pairs with firmware commit 5095ed0+):
  the device enters a true enrollment mode — mic pinned open, wake/stop models
  disarmed, cyan breathing LED, 10-minute hard cap, center button as physical
  escape — while an automated audio coach (gpt-4o-mini-tts prompts, cached,
  pushed down the speaker lane on a fixed schedule) guides 25 varied wake-phrase
  repetitions plus 90 s of natural speech. Mic audio flows ONLY to the recorder
  during enrollment: OpenAI hears nothing, so no VAD commits, no forced
  responses, no cost, no conversation mechanics to fight. New options:
  `enrollment_phrase`, `enrollment_tts_voice`.

## 0.8.0 (fork)

- **Voice enrollment**: say "I want to teach you my voice" — the assistant runs
  a guided recording session (varied wake-phrase repetitions + natural speech)
  via the new `voice_enrollment` tool, capturing the raw device mic stream to
  `/share/voice-enrollment/<person>_<timestamp>.wav` (16 kHz mono, 15-minute
  safety cap, persists across rebuilds). One session yields wake-word training
  positives AND voice-print enrollment audio. Recordings are personal data and
  are not managed by the add-on beyond writing the file.

## 0.7.1 (fork)

- Speaker probe tuned for real device audio (live test found 3-7 voiced frames
  in actual speech vs 100+ on synthetic bench audio): YIN threshold 0.15 → 0.20
  with a moderate-periodicity argmin fallback, energy gate 0.15 → 0.08 of peak
  RMS, minimum voiced frames 12 → 8, capture window 2.5 s → 3.0 s. Synthetic
  bench unchanged (0% wrong on typical voices).
- Debug: when `enable_recording` is on, each probe capture is saved to
  `recordings/probe_*.wav` for offline threshold calibration.

## 0.7.0 (fork)

- **Speaker context v1**: optional voice-type (male/female) detection for a
  two-person household. On every wake the first ~2.5 s of command audio is
  classified by median pitch (pure numpy YIN, in-process, off the event loop;
  benched at 98.6% right / 0% wrong across 11 typical synthetic voices) and the
  verdict is injected into the Realtime session as a system item, so the
  assistant can address the speaker by name ("sir"/"ma'am") and hedge when
  uncertain. New options: `speaker_male_name`, `speaker_female_name` (both
  empty = feature off).
- **Speaker-gated tools**: `male_only_tools` (comma-separated tool names) are
  enforced below the model — gated tools return a polite refusal unless the
  last voice verdict is the male speaker. Fails closed on uncertain/stale
  verdicts. Convenience gating, not biometric auth.

## 0.6.0

> ⚠️ **This update has two parts — please update both:**
> 1. **This add-on** (the update you're installing now).
> 2. **The Voice PE firmware** — open **ESPHome Device Builder** and click **Update** (or **Install**) on your device.
>
> The device and the add-on use one shared protocol; updating only one half can cause odd behaviour.

A reliability and voice-control polish release.

**Stop word**

- **Saying "stop" now usually works on the first try.** The spoken "stop" could
  previously be answered by the assistant a moment later, so you sometimes had to
  repeat it; that follow-on reply is now cancelled, so a single "stop" is
  typically enough.
- **Saying "stop" during a web search returns the device to rest promptly** — the
  light ring no longer keeps showing the "replying" animation for several seconds.
- **Fewer accidental stops** on the assistant's own speech.
- The light ring briefly flashes **red** to confirm your "stop" was registered. *(firmware)*

**Reliability**

- **No more unresponsive sessions.** A silently dropped connection to OpenAI is
  now detected and repaired within seconds, instead of leaving the assistant deaf
  until a restart.
- **The roughly hourly reconnect now happens proactively during a quiet moment**,
  so it practically never interrupts a conversation.
- **Smart-home commands are no longer cancelled** if you keep talking while they run.
- The light can no longer get **stuck on "thinking"**, and long web searches get
  all the time they need.

**No more "answers out of nowhere"**

- The assistant no longer occasionally replies — or repeats its previous answer —
  right after the wake word when you said nothing.
- A sentence that got cut off is no longer answered minutes later on your next wake.

**Settings**

- New **"Wake mic delay"** setting: a short pause after the wake chime before the
  mic opens, so the chime can't be mistaken for speech (default 700 ms).
- The **"Follow-up mic delay"** default is now **700 ms**. Existing installs keep
  their saved value — raise yours if the assistant ever answers right after its
  own reply.

## 0.5.0

A big stable release: everything built and tested on the dev channel over the
past days. **Also update the Voice PE firmware** (v1.1.0 — one click in ESPHome
Builder) to get the full effect of the "stop" improvements; the two halves
work best together.

- **"Stop" now works through the whole reply AND the after-reply listening
  window.** The device detects the word more reliably, and the bridge treats
  it as authoritative: in-flight audio is discarded and an answer OpenAI had
  already started for the stop word itself is cancelled on arrival — no more
  "Okay, I'll be quiet" replies to your "stop".
- **Fixed: an answer could cut off mid-sentence, after which the assistant
  went deaf** until the next reconnect. Harmless protocol races (e.g. your
  sentence being split into two turns by a pause) no longer kill the session.
- **Fixed an audio race that could inject noise/hiss into replies** (firmware,
  paired with this release).
- **Mute behaves properly now** (firmware): the ring goes dark with red
  markers by the microphones, and muting also ends an open listening window
  immediately — both from Home Assistant and with the physical side switch.
- **The LED Ring switch in Home Assistant works again** (firmware): entity off
  = device dark at rest; entity on = the gentle "ready" pulse.
- **Completely reworked Configuration tab**: options grouped logically
  (Basics → Model & voice → Conversation → Web search → Audio →
  Home Assistant → Advanced), every description rewritten in plain practical
  language, and a full Dutch translation included (shown automatically when
  your HA is set to Dutch). Confusing or broken switches were removed; rarely
  needed expert fields stay hidden until you need them.
- **The add-on now has its own icon.**
- Friendlier defaults for new installs: follow-up mic delay 200 ms and
  playback buffer 150 ms. **Existing installs keep their saved values** — if
  yours still say 0, consider setting 200/150 manually (Conversation / Audio
  groups) for fewer ghost triggers and less crackle.

### Heads-up: the firmware stub template was improved

The per-device stub in ESPHome Builder used to reference the firmware in a
form that lets ESPHome **cache the downloaded YAML for a day** — clicking
Update shortly after a release could then silently rebuild yesterday's code.
The stub templates in the firmware repo are fixed; existing users can apply
the same fix once by replacing **only the `packages:` block** in their
device's YAML in ESPHome Builder (everything else — your name, secrets,
`dashboard_import` — stays exactly the same):

```yaml
packages:
  realtime:
    url: https://github.com/TristanBrotherton/voicepe-realtime-firmware
    ref: main
    files: [home-assistant-voice.realtime.yaml]
    refresh: 0s
```

Current templates for reference:
[esphome-builder.dhcp.yaml](https://github.com/TristanBrotherton/voicepe-realtime-firmware/blob/main/esphome-builder.dhcp.yaml) ·
[esphome-builder.static-ip.yaml](https://github.com/TristanBrotherton/voicepe-realtime-firmware/blob/main/esphome-builder.static-ip.yaml)

## 0.4.26

- **Web search is now ON by default**, using **gpt-5.5** (the best-quality search
  model), so the assistant can look things up online — weather, news, facts — out
  of the box. **Existing installs keep their saved setting**: if you had it off,
  switch `enable_web_search` on (and set `web_search_model` to `gpt-5.5`) in the
  add-on Configuration. The cheaper mini/nano models stay available.

## 0.4.25

- **Fix:** the first thing you said in the few seconds right after an automatic
  reconnect (e.g. after the 60-minute session cap) could be ignored
  (`conversation_already_has_active_response`). The reconnected session no longer
  creates a duplicate response, so that turn answers normally.

## 0.4.24

- **Renamed** to **OpenAI Realtime 2 Voice Agent**.
- Rewrote the store/info description and added a full **Documentation** tab
  (install steps, OpenAI key, Home Assistant MCP setup, recommended settings, web
  search, credits). Removed stale text from the original upstream client.
- Default system prompt is now an English, voice-tuned prompt (silent tool calls,
  varied confirmations, language pinning). Your own saved prompt is not changed.
- Default `follow_up_open_delay_ms` and `playback_prebuffer_ms` set to `0` (raise
  them if the device hears its own tail or you hear crackle).

## 0.4.23

- **Fix:** the 60-minute session cap sometimes left the session dead until a
  restart. It now reconnects automatically in all cases (both the keepalive-drop
  and the `session_expired` forms).

## 0.4.22

- **New options:** voice **speed** (0.25–1.5), **max reply length**
  (`max_output_tokens`), and **input noise reduction** (off / near-field /
  far-field). All default to current behaviour.

## 0.4.21

- Model, voice, web-search-model and transcription-model options are now
  **dropdowns** with the known-good values, each with a **custom** entry if you
  need a value not in the list.

## 0.4.20

- **New:** optional **web search**. Turn on `enable_web_search` to let the
  assistant look things up online (weather, news, facts). Uses your OpenAI key;
  off by default. Model configurable via `web_search_model` (default gpt-5.4-mini).

## 0.4.19

- Clarified the MCP option help text for both the built-in HA MCP Server and the
  unofficial ha-mcp add-on.

## 0.4.18

- **Fix:** removed a meaningless filler reply ("I'm ready to continue…") that could
  appear on the first turn of a session.

## 0.4.17

- **Fix:** cap restored conversation history (`max_context_messages`, default 12) to
  bound per-turn token cost and avoid hitting OpenAI's rate limit.

## 0.4.16

- **Fix:** the device no longer gets stuck blinking "thinking" after a turn-ending
  error (e.g. a rate limit) — it returns to idle so you can retry.

## 0.4.14

- **New:** `playback_prebuffer_ms` jitter buffer to reduce occasional crackle at the
  start of replies.

## 0.4.12 – 0.4.13

- **Fix:** "say stop, then immediately ask again → silence". Disabled the broken
  server-side audio truncation that wedged the next turn.

## 0.4.9 – 0.4.11

- **New:** auto-reconnect the OpenAI Realtime session when its connection drops
  (keepalive timeout / 60-minute cap), instead of going dead until a restart.
  Refined so a normal device disconnect doesn't trigger an unnecessary reconnect.

## 0.4.6 – 0.4.8

- **New:** configurable post-reply **follow-up listening window** (answer back
  without re-saying the wake word) + its open-delay, and per-option help text in the
  UI.
- **New:** the assistant's and user's transcripts are logged to the add-on log
  (`🤖 assistant:` / `🗣️ user:`).

## 0.4.0 – 0.4.4

- **Fix:** resample the device's 16 kHz mic to the 24 kHz OpenAI requires (garbled
  speech), and drop empty audio chunks.
- **New:** device **"stop"** interrupt now actually cancels the reply and clears
  buffered audio.

## 0.3.x

- Switched the target to **gpt-realtime-2**, pinned pipecat-ai 0.0.97, and tuned
  turn detection (semantic VAD), phase delivery to the device, and the startup
  sequence to stop double-responses. Made the disconnect tool and transcription
  model configurable.

## Earlier

- Initial pipecat + WebSocket implementation (forked from
  [fjfricke/ha-openai-realtime](https://github.com/fjfricke/ha-openai-realtime)).
