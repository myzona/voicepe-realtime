"""GPT-Live-1 (OpenAI Live API) service for the Voice PE add-on.

`SafeLiveLLMService` is the Live-API twin of `SafeRealtimeLLMService`
(main.py). pipecat's `OpenAILiveLLMService` already speaks the protocol
(`wss://api.openai.com/v1/live/sessions`, `session.start` → `session.started`,
Responses delegation with client-executed function tools); this subclass bends
it to the add-on's device contract, mirroring the Realtime overrides where they
apply:

  * Session bootstrap. pipecat starts the Live session on the first
    `LLMContextFrame` (its examples queue an `LLMRunFrame` on connect). This
    pipeline has no such trigger, so the service is handed the aggregator
    pair's context (`set_bootstrap_context`) and starts the session itself
    right after `StartFrame`. Cached messages from a previous connection are
    already in that context and become the session's `input` history.
  * Tools. The backend model's tools come from the context in pipecat's
    design; here they are the SAME provider-native tool dicts the Realtime path
    sends (HA MCP, ask_openclaw/recall_memory, timers, memory, enrollment,
    web_search, disconnect), injected in `_invocation_params`. Handlers are
    registered explicitly (never pruned by pipecat's tool sync), with
    `cancel_on_interruption=False` and the speaker gate / liveness wrapper.
    With `hosted_web_search=True` the add-on's `web_search` FUNCTION tool is
    swapped for the Responses built-in `{"type": "web_search"}` tool, which the
    backend model runs server-side (no client hop, no second Responses call).
    If `session.start` is rejected on this connection the service falls back
    to the function tool once and restarts the session.
  * Input clock (phase-4 fix for the post-tool stall). The Live model's
    timeline is driven by INPUT audio: it produces output (speech, and the
    "hearing" of a delegated result) only while audio keeps arriving. The
    Voice PE is half-duplex — its firmware stops streaming the mic the moment
    the reply starts (`replying`) and does not resume until `listening` or the
    follow-up window after `idle`. A tool result that lands while the mic is
    gated is therefore not spoken until the mic re-opens (observed 2026-09-16:
    17 s stall until the thinking-watchdog forced idle and the follow-up
    window re-opened the mic). While the conversation is active and no device
    audio is arriving, the service feeds real-time-paced silence to the
    session so the model's clock keeps running. Idle time (no turn in flight)
    is not fed — session audio is billed per second.
  * Turn frames stay SERVICE-DRIVEN (phase-1 decision 1). Live has no
    speech_started/stopped events: pipecat derives user turns from
    `session.input_transcript.delta` fragments and broadcasts
    `ProposedUser*SpeakingFrame` for the aggregator's UserTurnController to
    resolve. That controller is exactly what the Realtime path bypasses, so
    `_open_turn`/`_end_turn` push one `UserStartedSpeakingFrame` / one
    `UserStoppedSpeakingFrame` downstream instead. A user transcript fragment
    that arrives while the bot is speaking (a late tail of the question, or
    echo) is recorded but does NOT announce a turn: `listening` would re-open
    the device mic mid-reply and `thinking` would strand the LED. The user
    turn gap is 1.5 s (pipecat: 0.8 s) so an ordinary mid-sentence pause does
    not split one question into two turns (thinking → listening → thinking).
    No interruption is broadcast: the live model handles being talked over
    itself and a device "stop" is handled explicitly (`handle_device_interrupt`).
  * Output audio gate. The Live model streams output at real-time pace,
    silence included. The Voice PE treats every binary frame as reply audio,
    so a continuous silent stream would keep it in playback forever (no idle,
    no follow-up window, no `stop` re-arm). Silence is dropped after a short
    hangover; the transport still sees enough trailing silence to fire
    BotStoppedSpeaking promptly.
  * Diagnostics. Every server event type is logged at INFO with the add-on's
    timestamps (audio deltas summarised every few seconds, transcript deltas
    at DEBUG after the first of a turn, Responses `*.delta` events at DEBUG),
    plus one `⏱️ live turn` line per reply with the per-stage latencies.
  * Recovery hooks. The receive loop reports its end as an ErrorFrame
    (`live receive loop ...`), an unexpected `session.closed` (expiry, safety,
    upstream loss) reports `live session closed`, and `session_expires_at` is
    exposed so ConnectionRecovery can refresh before `expires_at`.
"""
import asyncio
import base64
import logging
import time
from typing import Iterable, Optional

from pipecat.audio.utils import is_silence
from pipecat.frames.frames import (
    Frame,
    InputAudioRawFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    SpeechOutputAudioRawFrame,
    StartFrame,
    TranscriptionFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.services.openai._constants import OPENAI_SAMPLE_RATE
from pipecat.services.openai.live import events as live_events
from pipecat.services.openai.live.llm import OpenAILiveLLMService
from pipecat.utils.time import time_now_iso8601

from app.live_mode import LiveModeService
from app.tool_guard import guarded_tool_handler

logger = logging.getLogger(__name__)

# Voices the Live API accepts (docs "Managing GPT-Live sessions" → Voice
# options, 2026-09-16: `marin` is the default; the twelve below are listed as
# GPT-Live voices) plus `cedar`, which pipecat's Live service names alongside
# marin. Realtime-only voice names are mapped to the default with a loud
# warning instead of being sent (an invalid voice fails `session.start`, and
# the session never comes up).
LIVE_VOICES = frozenset({
    "marin", "cedar",
    "quartz", "ripple", "vesper", "willow", "stone", "gleam", "meridian",
    "bossa", "tempo", "beacon", "delta", "cinder",
})
REALTIME_ONLY_VOICES = frozenset({
    "alloy", "ash", "ballad", "coral", "echo", "sage", "shimmer", "verse",
})
DEFAULT_LIVE_VOICE = "marin"

# Reasoning efforts the Responses API documents for the gpt-5.x series. Other
# strings pass through with a warning (forward compatibility; the API error is
# loud). Empty/"default" leaves the field unset (server default).
REASONING_EFFORTS = frozenset({"none", "minimal", "low", "medium", "high"})
DEFAULT_BACKEND_REASONING_EFFORT = "low"
TEXT_VERBOSITIES = frozenset({"low", "medium", "high"})

# How long silence keeps flowing to the device after the last speech chunk.
# Must exceed pipecat's BOT_VAD_STOP_SECS (0.35 s) so the output transport
# sees a silence frame late enough to declare the bot stopped.
OUTPUT_SILENCE_HANGOVER_S = 0.5
# Safety cap on the post-"stop" output mute (normally lifted by the next
# assistant/user turn boundary).
OUTPUT_MUTE_MAX_S = 10.0

# Quiet time that ends a USER transcript turn. pipecat uses 0.8 s for both
# roles, which split "temperature outside in Pleasant Hill … California" into
# two turns (thinking → listening → thinking on the LED). The live model
# decides on its own when to answer, so this only delays the `thinking`
# phase and the final TranscriptionFrame — never the reply.
USER_TURN_GAP_S = 1.5
# A user transcript fragment within this long after the last speech chunk
# (or while an assistant transcript turn is open) belongs to the reply in
# progress — a late tail of the question or echo — and must not announce a
# new user turn (the firmware re-opens the mic on `listening`).
BOT_SPEAKING_GRACE_S = 1.0

# Input clock (see module docstring). Device audio arrives in ≤60 ms chunks
# while the mic streams; a gap longer than INPUT_CLOCK_GAP_S means the gate
# closed. Silence is then fed at wall-clock pace, in chunks of at most
# INPUT_CLOCK_MAX_CHUNK_S, for as long as the conversation is active: a turn,
# function call or delegated response is open, or there was activity within
# INPUT_CLOCK_TAIL_S (a delegated result still has to be "heard" and spoken).
INPUT_CLOCK_INTERVAL_S = 0.1
INPUT_CLOCK_GAP_S = 0.3
INPUT_CLOCK_MAX_CHUNK_S = 0.5
INPUT_CLOCK_TAIL_S = 12.0

# Event log rate limits.
AUDIO_LOG_EVERY_S = 5.0

# Responses built-in web search tool (runs in the backend model, server-side).
HOSTED_WEB_SEARCH_TOOL = {"type": "web_search"}
WEB_SEARCH_FUNCTION_NAME = "web_search"

# Fixed preamble for the backend (Responses) model. The persona instructions
# are written for the speaking model; the backend only sees delegated
# requests as text, so it gets the voice-context framing OpenAI recommends
# plus the same persona rules (tool rules, "never guess", language).
BACKEND_PREAMBLE = (
    "You are the backend of a live voice assistant. You receive delegated "
    "requests from the spoken conversation; transcripts can contain mistakes, "
    "unfinished phrases and later corrections, so use the latest context. Use "
    "the available tools to carry out requests; never claim an action "
    "succeeded without a tool result confirming it. Return concise, "
    "conversational plain text (no Markdown, no raw JSON) that the voice "
    "assistant can relay. The assistant's own instructions follow, apply the "
    "same rules.\n\n"
)

# Explicit tool-use rules for the backend model (phase 4: "what time is it"
# was answered from thin air — 4:06 p.m. at 10:23 — with no tool call).
# `{time_tool}` is the date/time tool's actual name when one is registered.
BACKEND_TOOL_RULES = (
    "TOOL RULES: You have no clock, no calendar and no knowledge of the "
    "present moment. For the current time, date or day ALWAYS call {time_tool} "
    "first and answer from its result; never guess, estimate or compute the "
    "time yourself. For the state of a device, sensor, room or person, call the "
    "Home Assistant tools (GetLiveContext, Hass*) instead of assuming. For the "
    "weather, news, prices, scores or anything that may have changed recently, "
    "use web search. To act on a device call the tool and report what the tool "
    "returned. Answer only after the tool results are in; if a tool fails or "
    "returns nothing useful, say so briefly instead of inventing an answer.\n\n"
)
DEFAULT_TIME_TOOL_LABEL = "the date/time tool"

# Rules for the LIVE model (appended to `session.instructions`). It converses;
# it must not answer knowledge/state questions itself and must not fill the
# delegation wait with sounds (observed: every reply opened with "[hum]").
LIVE_DELEGATION_RULES = (
    "DELEGATION: You are the live voice; a backend model holds all the tools and "
    "all the facts. You do not know the current time or date, the weather, the "
    "news, or the state of any device, sensor or timer. For every such question "
    "or request — including \"what time is it\" — delegate to the backend and "
    "relay its answer; never answer from memory and never estimate. Delegate "
    "every smart-home action too, and confirm it only after the backend reports "
    "the result. While the backend works, stay silent: do not hum, murmur, make "
    "filler sounds or announce that you are checking. Speak the answer as soon "
    "as it arrives, in one short reply."
)


def is_live_model(model: str) -> bool:
    """True for the GPT-Live model family (`gpt-live-1` and custom `gpt-live-*` ids)."""
    m = (model or "").strip().lower()
    return m == "gpt-live-1" or m.startswith("gpt-live-")


def resolve_live_voice(voice: str) -> str:
    """Validate/map the configured voice for a Live session.

    Known Live voices pass through. Known Realtime-only voices are mapped to
    `marin` with a warning (they would fail `session.start`). Anything else
    (custom escape hatch) passes through with a warning, so a newly added
    voice can be used before this list is updated; the API error is loud.
    """
    v = (voice or "").strip().lower()
    if not v:
        return DEFAULT_LIVE_VOICE
    if v in LIVE_VOICES:
        return v
    if v in REALTIME_ONLY_VOICES:
        logger.warning(
            f"⚠️ openai_voice '{v}' is a Realtime-only voice; GPT-Live does not accept it — "
            f"using '{DEFAULT_LIVE_VOICE}' (Live voices: {', '.join(sorted(LIVE_VOICES))})"
        )
        return DEFAULT_LIVE_VOICE
    logger.warning(
        f"⚠️ openai_voice '{v}' is not a known GPT-Live voice; sending it anyway "
        f"(session.start fails loudly if the API rejects it)"
    )
    return v


def resolve_reasoning_effort(value: str) -> Optional[str]:
    """Normalise the `live_backend_reasoning_effort` option.

    Returns None for "" / "default" (field omitted → server default). Known
    efforts pass; unknown strings pass through with a warning so a newly
    documented level can be used before this list is updated.
    """
    v = (value or "").strip().lower()
    if not v or v == "default":
        return None
    if v not in REASONING_EFFORTS:
        logger.warning(
            f"⚠️ live_backend_reasoning_effort '{v}' is not a known effort "
            f"({', '.join(sorted(REASONING_EFFORTS))}); sending it anyway"
        )
    return v


def resolve_text_verbosity(value: str) -> Optional[str]:
    """Normalise the `live_backend_verbosity` option (None = not sent)."""
    v = (value or "").strip().lower()
    if not v or v == "default":
        return None
    if v not in TEXT_VERBOSITIES:
        logger.warning(
            f"⚠️ live_backend_verbosity '{v}' is not a known verbosity "
            f"({', '.join(sorted(TEXT_VERBOSITIES))}); sending it anyway"
        )
    return v


def time_tool_name(tool_names: Iterable[str]) -> Optional[str]:
    """The registered date/time tool (HA MCP exposes `GetDateTime`, prefixed per install)."""
    for name in tool_names:
        if name and name.lower().endswith("getdatetime"):
            return name
    return None


def backend_instructions(persona: str, tool_names: Iterable[str] = ()) -> str:
    """Instructions for the backend (Responses) model: preamble + tool rules + persona."""
    tool = time_tool_name(tool_names) or DEFAULT_TIME_TOOL_LABEL
    return BACKEND_PREAMBLE + BACKEND_TOOL_RULES.format(time_tool=tool) + (persona or "")


def live_instructions(persona: str) -> str:
    """`session.instructions` for the live model: persona + delegation/no-filler rules."""
    persona = (persona or "").rstrip()
    return f"{persona}\n\n{LIVE_DELEGATION_RULES}" if persona else LIVE_DELEGATION_RULES


def _fmt_delta(a: Optional[float], b: Optional[float]) -> str:
    if a is None or b is None:
        return "–"
    return f"{b - a:.1f}s"


class SafeLiveLLMService(LiveModeService, OpenAILiveLLMService):
    """OpenAILiveLLMService adapted to the Voice PE add-on (see module docstring).

    Inherits `LiveModeService` (app/live_mode.py) purely as a marker so
    `websocket_handler.build_pipeline` / `Application._preseed_context` can
    detect "a live-style service is in the Realtime slot" without hardcoding
    this class — Gemini Live (app/gemini_live_service.py) shares the same
    marker. No behaviour changes from the mixin itself.
    """

    def __init__(self, *, tools: Optional[list] = None, hosted_web_search: bool = False, **kwargs):
        super().__init__(**kwargs)
        # Provider-native tool dicts ({"type":"function","name",...}) — the
        # exact list the Realtime path puts in session.update.tools.
        self._tool_dicts = list(tools) if tools else []
        self._hosted_web_search = bool(hosted_web_search)
        # Cleared when session.start is rejected while the hosted tool is in
        # the payload; the session is then restarted with the function tool.
        self._hosted_web_search_ok = True
        self._hosted_fallback_task = None
        self._bootstrap_context = None
        self._resetting_conversation = False
        # Wired by Application.create_openai_service (same as the Realtime class).
        self.speaker_probe = None
        self.male_only_tools: set = set()
        self.turn_liveness = None
        # Set from session.started (Unix seconds); ConnectionRecovery refreshes
        # ahead of it.
        self.session_expires_at: Optional[int] = None
        self.session_id: Optional[str] = None
        # Output gate state.
        self._last_speech_mono = 0.0
        self._dropped_silence_frames = 0
        self._output_muted_until = 0.0
        # Graceful close in flight (stop()/EndFrame): a session.closed then is
        # expected, not a death.
        self._closing = False
        # User turn handling (see module docstring).
        self._user_turn.gap_secs = USER_TURN_GAP_S
        self._user_turn_announced = False
        # Input clock.
        self._last_input_audio_mono = 0.0
        self._last_activity_mono = 0.0
        self._input_clock_task = None
        self._input_clock_feeding = False
        self._input_clock_fed_ms = 0
        self._input_clock_total_ms = 0
        # Event log state.
        self._audio_log_mono = 0.0
        self._audio_deltas = 0
        self._audio_speech_deltas = 0
        self._audio_dropped_at_log = 0
        self._last_usage_logged = None
        # Per-turn latency stamps (monotonic).
        self._t_user_last_fragment: Optional[float] = None
        self._t_user_turn_end: Optional[float] = None
        self._t_delegation: Optional[float] = None
        self._t_first_call: Optional[float] = None
        self._t_last_output: Optional[float] = None
        self._t_response_done: Optional[float] = None
        self._t_first_speech: Optional[float] = None

    # ---- bootstrap -----------------------------------------------------------

    def set_bootstrap_context(self, context) -> None:
        """Context the session is started from right after StartFrame."""
        self._bootstrap_context = context

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        if isinstance(frame, InputAudioRawFrame):
            now = time.monotonic()
            self._last_input_audio_mono = now
            self._last_activity_mono = now
        await super().process_frame(frame, direction)
        if isinstance(frame, StartFrame) and self._context is None and self._bootstrap_context is not None:
            # pipecat would wait for an LLMContextFrame (an LLMRunFrame queued
            # by the app). Start from the aggregator pair's context instead:
            # its cached messages become the session's `input` history.
            await self._handle_context(self._bootstrap_context)

    def _invocation_params(self):  # type: ignore[override]
        params = super()._invocation_params()
        if self._tool_dicts and not params["tools"]:
            params["tools"] = self.effective_tools()
        return params

    def hosted_web_search_active(self) -> bool:
        """True when the backend gets the built-in web_search tool instead of the function."""
        return self._hosted_web_search and self._hosted_web_search_ok and any(
            t.get("name") == WEB_SEARCH_FUNCTION_NAME for t in self._tool_dicts
        )

    def effective_tools(self) -> list:
        """The `delegation.responses.tools` list for the current mode."""
        tools = [dict(t) for t in self._tool_dicts]
        if self.hosted_web_search_active():
            tools = [t for t in tools if t.get("name") != WEB_SEARCH_FUNCTION_NAME]
            tools.append(dict(HOSTED_WEB_SEARCH_TOOL))
        return tools

    def _compose_system_instruction(self):  # type: ignore[override]
        """Keep `session.instructions` = the persona (+ appended instructions).

        The base LLMService appends its ASYNC TOOLS guidance whenever a tool is
        registered with cancel_on_interruption=False — i.e. always here. That
        text is written for a cascade LLM that receives late tool results as
        messages; in Live the tools run in the backend model, so it is noise
        in the live model's prompt.
        """
        parts = [self._base_system_instruction] if self._base_system_instruction else []
        parts.extend(getattr(self, "_appended_system_instructions", []) or [])
        composed = "\n\n".join(p for p in parts if p) or None
        self._settings.system_instruction = composed
        self._composed_system_instruction = composed

    # ---- tools ----------------------------------------------------------------

    def register_function(self, function_name, handler, *,
                          cancel_on_interruption=None, timeout_secs=None,
                          cancellable_by_llm=None):  # type: ignore[override]
        """Force cancel_on_interruption=False + speaker gate + liveness (see tool_guard.py)."""
        super().register_function(
            function_name,
            guarded_tool_handler(self, function_name, handler),
            cancel_on_interruption=False,
            timeout_secs=timeout_secs,
            cancellable_by_llm=cancellable_by_llm,
        )

    # ---- turn frames: service-driven, like the Realtime path -------------------

    def bot_is_speaking(self, now: Optional[float] = None) -> bool:
        """True while the reply in progress is still audible (transcript or audio)."""
        now = time.monotonic() if now is None else now
        return bool(self._assistant_turn.open or now - self._last_speech_mono < BOT_SPEAKING_GRACE_S)

    async def _handle_evt_transcript_delta(self, evt):  # type: ignore[override]
        if evt.delta:
            now = time.monotonic()
            self._last_activity_mono = now
            if evt.role == "user" and not self.bot_is_speaking(now):
                if self._t_first_speech is not None:
                    # A new question after the last reply: fresh stamps.
                    self._reset_turn_stamps()
                self._t_user_last_fragment = now
        await super()._handle_evt_transcript_delta(evt)

    async def _open_turn(self, role: str):  # type: ignore[override]
        if role == "user":
            # First transcript fragment of a user turn. One UserStartedSpeaking
            # downstream (PhaseEmitter → `listening`, lifts the device's
            # post-stop mute); no ProposedUser* for the aggregator, no
            # interruption broadcast (full duplex: the model yields itself).
            if self.bot_is_speaking():
                # Late tail of the question (or echo) while the reply plays:
                # `listening` would re-open the device mic into the reply and
                # the following `thinking` would strand the LED. Record only.
                self._user_turn_announced = False
                logger.info("🎙️ user transcript while bot speaking — turn not announced (no listening flip)")
                return
            self._user_turn_announced = True
            self._output_muted_until = 0.0
            logger.info("🎙️ user turn opened (first transcript fragment) → listening")
            await self.push_frame(UserStartedSpeakingFrame())
        else:
            logger.info("🗣️ assistant turn opened (first output transcript fragment)")
            await self.push_frame(LLMFullResponseStartFrame())
            await self.push_frame(TTSStartedFrame())

    async def _end_turn(self, role: str):  # type: ignore[override]
        turn = self._user_turn if role == "user" else self._assistant_turn
        if not turn.open:
            return
        turn.open = False
        text, turn.text = turn.text, ""
        if role == "user":
            self._t_user_turn_end = time.monotonic()
            if text.strip():
                await self.push_frame(
                    TranscriptionFrame(text, "", time_now_iso8601()),
                    FrameDirection.UPSTREAM,
                )
            if self._user_turn_announced:
                self._user_turn_announced = False
                await self.push_frame(UserStoppedSpeakingFrame())
            else:
                logger.info(f"🎙️ unannounced user fragment closed: {text.strip()!r}")
        else:
            # A post-"stop" mute ends with the utterance it silenced.
            self._output_muted_until = 0.0
            logger.info("🗣️ assistant turn closed")
            await self.push_frame(TTSStoppedFrame())
            await self.push_frame(LLMFullResponseEndFrame())

    # ---- output audio gate ----------------------------------------------------

    async def _handle_evt_audio_delta(self, evt: live_events.OutputAudioDeltaEvent):  # type: ignore[override]
        audio = base64.b64decode(evt.delta)
        if not audio:
            return
        now = time.monotonic()
        self._audio_deltas += 1
        self._maybe_log_audio(now)
        if now < self._output_muted_until:
            return  # device "stop": the user does not want to hear this utterance
        if len(audio) % 2 == 0 and is_silence(audio):
            if now - self._last_speech_mono > OUTPUT_SILENCE_HANGOVER_S:
                # Continuous silence between utterances: never forward it, or the
                # device stays in playback and never reaches idle.
                self._dropped_silence_frames += 1
                return
        else:
            self._audio_speech_deltas += 1
            self._last_speech_mono = now
            self._last_activity_mono = now
            if self._t_first_speech is None and self._t_user_last_fragment is not None:
                self._t_first_speech = now
                self._log_turn_latency()
        await self.push_frame(
            SpeechOutputAudioRawFrame(audio=audio, sample_rate=OPENAI_SAMPLE_RATE, num_channels=1)
        )

    def _maybe_log_audio(self, now: float) -> None:
        if now - self._audio_log_mono < AUDIO_LOG_EVERY_S:
            return
        if self._audio_log_mono:
            dropped = self._dropped_silence_frames - self._audio_dropped_at_log
            logger.info(
                f"📡 live audio: {self._audio_deltas} output deltas in the last "
                f"{now - self._audio_log_mono:.0f}s ({self._audio_speech_deltas} speech, "
                f"{dropped} silent dropped)"
            )
        self._audio_log_mono = now
        self._audio_deltas = 0
        self._audio_speech_deltas = 0
        self._audio_dropped_at_log = self._dropped_silence_frames

    # ---- per-turn latency -------------------------------------------------------

    def _reset_turn_stamps(self) -> None:
        self._t_user_last_fragment = None
        self._t_user_turn_end = None
        self._t_delegation = None
        self._t_first_call = None
        self._t_last_output = None
        self._t_response_done = None
        self._t_first_speech = None

    def _log_turn_latency(self) -> None:
        """One line per reply: user speech end (last fragment) → first audio, with stages."""
        u = self._t_user_last_fragment
        parts = [f"user→first audio {_fmt_delta(u, self._t_first_speech)}"]
        if self._t_delegation is not None:
            parts.append(f"user→delegation {_fmt_delta(u, self._t_delegation)}")
        if self._t_first_call is not None:
            parts.append(f"user→tool call {_fmt_delta(u, self._t_first_call)}")
        if self._t_first_call is not None and self._t_last_output is not None:
            parts.append(f"tool {_fmt_delta(self._t_first_call, self._t_last_output)}")
        if self._t_last_output is not None:
            parts.append(f"result→first audio {_fmt_delta(self._t_last_output, self._t_first_speech)}")
        elif self._t_response_done is not None:
            parts.append(f"backend done→first audio {_fmt_delta(self._t_response_done, self._t_first_speech)}")
        logger.info("⏱️ live turn: " + ", ".join(parts))

    # ---- input clock ------------------------------------------------------------

    def conversation_active(self, now: Optional[float] = None) -> bool:
        """True while the model may still have to produce something (feed the clock)."""
        now = time.monotonic() if now is None else now
        return bool(
            self._user_turn.open
            or self._assistant_turn.open
            or self._open_function_calls
            or self._pending_responses
            or now - self._last_activity_mono < INPUT_CLOCK_TAIL_S
        )

    async def _input_clock_tick(self, now: float, clock: float) -> int:
        """Feed silence covering `now - clock` if the device mic is gated. Returns ms fed."""
        if not self._session_started or self._disconnecting:
            return 0
        if now - self._last_input_audio_mono < INPUT_CLOCK_GAP_S:
            return 0  # device audio is flowing: it IS the clock
        if not self.conversation_active(now):
            return 0
        ms = int(round(min(max(now - clock, 0.0), INPUT_CLOCK_MAX_CHUNK_S) * 1000))
        if ms <= 0:
            return 0
        n_samples = OPENAI_SAMPLE_RATE * ms // 1000
        payload = base64.b64encode(b"\x00\x00" * n_samples).decode("utf-8")
        await self.send_client_event(live_events.InputAudioAppendEvent(audio=payload))
        return ms

    async def _input_clock_loop(self):
        clock = time.monotonic()
        try:
            while True:
                await asyncio.sleep(INPUT_CLOCK_INTERVAL_S)
                now = time.monotonic()
                fed = await self._input_clock_tick(now, clock)
                clock = now
                if fed:
                    if not self._input_clock_feeding:
                        self._input_clock_feeding = True
                        self._input_clock_fed_ms = 0
                        logger.info("🔇 input clock: device mic gated — feeding silence so the model keeps running")
                    self._input_clock_fed_ms += fed
                    self._input_clock_total_ms += fed
                elif self._input_clock_feeding:
                    self._input_clock_feeding = False
                    why = "device audio resumed" if now - self._last_input_audio_mono < INPUT_CLOCK_GAP_S else "conversation quiet"
                    logger.info(
                        f"🔇 input clock: stopped after {self._input_clock_fed_ms / 1000:.1f}s of silence "
                        f"({why}; {self._input_clock_total_ms / 1000:.0f}s fed this connection)"
                    )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"⚠️ input clock stopped: {e!r}")

    def _start_input_clock(self) -> None:
        if self._input_clock_task is not None and not self._input_clock_task.done():
            return
        try:
            self._input_clock_task = self.create_task(self._input_clock_loop(), "live-input-clock")
        except Exception as e:  # no task manager (unit tests outside a pipeline)
            logger.debug(f"input clock not started: {e!r}")

    async def _stop_input_clock(self) -> None:
        task, self._input_clock_task = self._input_clock_task, None
        self._input_clock_feeding = False
        if task is not None and not task.done():
            try:
                await self.cancel_task(task)
            except Exception as e:
                logger.debug(f"input clock cancel: {e!r}")

    # ---- device hooks (called from websocket_handler.build_pipeline) --------------

    async def handle_device_interrupt(self) -> None:
        """Device "stop": silence this utterance and tell the model to stop.

        The device already discards incoming audio until the next turn; muting
        here keeps the phase path honest (BotStopped → idle instead of a
        `replying` LED while the model finishes a sentence nobody hears). The
        Live API has no response.cancel for speech, so the model is asked to
        stop via an appended instruction (an appended instruction can interrupt
        speech in progress, per the API docs).
        """
        self._output_muted_until = time.monotonic() + OUTPUT_MUTE_MAX_S
        if not self._session_started:
            return
        try:
            await self.send_client_event(live_events.SessionInstructionsAppendEvent(
                delegation_id=None,
                content=(
                    "The user said stop. Stop speaking immediately, do not finish the "
                    "sentence, and wait silently for the user's next request."
                ),
            ))
            logger.info("🛑 device interrupt → output muted + stop instruction appended")
        except Exception as e:
            logger.info(f"🛑 device interrupt → stop instruction no-op ({e!r})")

    def note_device_mic_closed(self) -> None:
        """Device follow-up window timed out (`flush`): nothing more is coming.

        Ends the input-clock tail early unless a function call / delegated
        response is still in flight (those keep the clock on their own).
        """
        self._last_activity_mono = 0.0
        self._last_input_audio_mono = 0.0

    async def inject_context(self, text: str) -> None:
        """Quiet session context (speaker verdicts): `session.thinking.append`."""
        if not self._session_started or not text:
            return
        await self.send_client_event(
            live_events.SessionThinkingAppendEvent(delegation_id=None, content=text)
        )

    def is_busy(self) -> bool:
        """True while a turn or a delegated function call is in flight (refresh guard)."""
        return bool(
            self._user_turn.open
            or self._assistant_turn.open
            or self._open_function_calls
            or self._pending_responses
            or time.monotonic() - self._last_speech_mono < 2.0
        )

    # ---- server event log ---------------------------------------------------------

    async def _handle_server_event(self, evt):  # type: ignore[override]
        self._log_server_event(evt)
        await super()._handle_server_event(evt)

    def _log_server_event(self, evt) -> None:
        t = evt.type
        if isinstance(evt, live_events.OutputAudioDeltaEvent):
            return  # summarised in _maybe_log_audio
        if isinstance(evt, live_events.TranscriptDeltaEvent):
            turn = self._user_turn if evt.role == "user" else self._assistant_turn
            msg = f"📡 live evt {t} role={evt.role} {evt.start_ms}-{evt.end_ms}ms {evt.delta!r}"
            if turn.open:
                logger.debug(msg)
            else:
                logger.info(msg + " (turn opens)")
            return
        if isinstance(evt, live_events.ResponseEventEnvelope):
            inner = evt.inner_type or "?"
            extra = ""
            if inner == "response.output_item.done":
                item = evt.event.get("item") or {}
                extra = f" item={item.get('type')} name={item.get('name')} status={item.get('status')}"
            elif inner in ("response.completed", "response.incomplete", "response.failed"):
                response = evt.event.get("response") or {}
                usage = response.get("usage") or {}
                extra = f" status={response.get('status')} tokens={usage.get('total_tokens')}"
            msg = f"📡 live evt response.event/{inner} delegation={evt.delegation_id}{extra}"
            if inner.endswith(".delta"):
                logger.debug(msg)
            else:
                logger.info(msg)
            return
        if isinstance(evt, live_events.SessionDelegationCreatedEvent):
            d = evt.delegation
            logger.info(f"📡 live evt {t} id={d.id} target={d.target} response_id={d.response_id} offset={evt.offset_ms}ms")
            return
        if isinstance(evt, live_events.SessionClosedEvent):
            logger.info(f"📡 live evt {t} reason={evt.reason}")
            return
        if isinstance(evt, live_events.ErrorEvent):
            return  # _handle_evt_error logs it with details
        if isinstance(evt, live_events.SessionUsageUpdatedEvent):
            return  # _report_usage (deduplicated)
        if isinstance(evt, live_events.SessionStartedEvent):
            return  # _handle_evt_session_started logs it
        if isinstance(evt, live_events.ContextAppendedEvent):
            logger.info(f"📡 live evt {t} {evt.start_ms}-{evt.end_ms}ms")
            return
        if isinstance(evt, live_events.UnknownServerEvent):
            fields = {k: v for k, v in (evt.model_extra or {}).items() if k not in ("type", "event_id")}
            logger.info(f"📡 live evt {t} (unmodelled) {str(fields)[:200]}")
            return
        logger.info(f"📡 live evt {t}")

    # ---- delegation bookkeeping -------------------------------------------------------

    async def _handle_evt_delegation_created(self, evt):  # type: ignore[override]
        now = time.monotonic()
        self._last_activity_mono = now
        if self._t_delegation is None:
            self._t_delegation = now
        await super()._handle_evt_delegation_created(evt)

    async def _handle_evt_response(self, evt):  # type: ignore[override]
        now = time.monotonic()
        self._last_activity_mono = now
        if self.turn_liveness is not None:
            # Backend progress counts as model activity for PhaseEmitter's
            # thinking watchdog (a built-in web search has no client tool in
            # flight to hold it open).
            self.turn_liveness.last_activity = now
        inner = evt.inner_type
        if inner == "response.output_item.done":
            item = evt.event.get("item") or {}
            if item.get("type") == "function_call" and self._t_first_call is None:
                self._t_first_call = now
        elif inner in ("response.completed", "response.incomplete", "response.failed"):
            self._t_response_done = now
        await super()._handle_evt_response(evt)
        if inner in ("response.completed", "response.incomplete", "response.failed"):
            # pipecat leaves a response that made NO function calls in
            # _pending_responses forever (its continuation check returns early
            # on `not had_calls`), which would keep is_busy() True and block the
            # proactive session refresh for the rest of the connection.
            key = evt.delegation_id or "uncorrelated"
            pending = self._pending_responses.get(key)
            if pending is not None and pending.finished and not pending.call_ids and not pending.had_calls:
                del self._pending_responses[key]

    async def _send_function_call_output(self, call_id: str, output: str):  # type: ignore[override]
        now = time.monotonic()
        self._last_activity_mono = now
        self._t_last_output = now
        logger.info(f"📤 function call output sent for {call_id} ({len(output)} chars)")
        await super()._send_function_call_output(call_id, output)

    async def _maybe_continue_response(self, key: str):  # type: ignore[override]
        pending = self._pending_responses.get(key)
        will_continue = bool(pending and pending.finished and pending.had_calls and not pending.call_ids)
        await super()._maybe_continue_response(key)
        if will_continue:
            logger.info(f"📤 response.create sent — backend continues delegation {key}")

    # ---- lifecycle / recovery ---------------------------------------------------

    async def _handle_evt_session_started(self, evt):  # type: ignore[override]
        self.session_expires_at = evt.session.expires_at
        self.session_id = evt.session.id
        ttl = ""
        if self.session_expires_at:
            ttl = f", expires in {max(0, self.session_expires_at - time.time()) / 60:.0f} min"
        audio = evt.session.audio or {}
        voice = (audio.get("output") or {}).get("voice") if isinstance(audio, dict) else None
        delegation = evt.session.delegation or {}
        responses = delegation.get("responses") if isinstance(delegation, dict) else None
        backend = ""
        if isinstance(responses, dict):
            backend = (
                f", backend={responses.get('model')} reasoning={responses.get('reasoning')} "
                f"tools={len(responses.get('tools') or [])}"
            )
        # Surface every top-level session field the server echoes (minus the
        # long ones) so an undocumented knob (filler/backchannel, formats…)
        # shows up in the add-on log.
        echoed = {
            k: v for k, v in (evt.session.model_dump(exclude_none=True) or {}).items()
            if k not in ("instructions", "input", "delegation", "id", "model", "expires_at", "audio")
        }
        logger.info(
            f"🟢 Live session {self.session_id} started (model={evt.session.model}, "
            f"voice={voice}{ttl}{backend}; other session fields: {echoed})"
        )
        self._last_usage_logged = None
        await super()._handle_evt_session_started(evt)
        self._start_input_clock()

    async def _handle_evt_session_closed(self, evt):  # type: ignore[override]
        await super()._handle_evt_session_closed(evt)
        if self._disconnecting or self._closing or self._resetting_conversation:
            return
        # Expiry / safety / upstream loss: the session is gone even if the
        # socket lingers. ConnectionRecovery keys on "live session closed".
        await self.push_error(
            error_msg=f"live session closed by server (reason={evt.reason}) — session_expired"
        )

    async def _handle_evt_error(self, evt):  # type: ignore[override]
        error = evt.error
        logger.warning(
            f"⚠️ Live API error {error.type or 'error'}/{error.code or 'unknown'}: "
            f"{error.message}{f' (param: {error.param})' if error.param else ''}"
        )
        if (
            not self._session_started_on_connection
            and self.hosted_web_search_active()
            and self._hosted_fallback_task is None
        ):
            # session.start rejected while the built-in web_search tool was in
            # the payload: retry once with the add-on's function tool (its
            # handler is registered either way). Not from the receive task —
            # the restart cancels it.
            self._hosted_web_search_ok = False
            logger.warning(
                "⚠️ session.start failed with the built-in web_search tool — restarting the "
                "session with the add-on's web_search function tool instead"
            )
            self._hosted_fallback_task = self.create_task(
                self._restart_without_hosted_tools(), "live-hosted-fallback"
            )
            return
        await super()._handle_evt_error(evt)

    async def _restart_without_hosted_tools(self):
        try:
            await self.reset_conversation()
        except Exception as e:
            await self.push_error(error_msg=f"Session startup failed (fallback restart): {e!r}")

    async def _report_usage(self, usage):  # type: ignore[override]
        # Phase-4 cost check needs this in the add-on log at INFO — once per
        # change, not every 15 s while the room is quiet.
        if usage.seconds is None:
            return
        seconds = round(usage.seconds)
        if seconds == self._last_usage_logged:
            return
        self._last_usage_logged = seconds
        logger.info(f"💰 live usage: {seconds}s of session audio (cumulative)")

    async def stop(self, frame):  # type: ignore[override]
        self._closing = True
        await self._stop_input_clock()
        await super().stop(frame)

    async def _disconnect(self):  # type: ignore[override]
        await self._stop_input_clock()
        await super()._disconnect()

    async def reset_conversation(self):  # type: ignore[override]
        """Reconnect in place (ConnectionRecovery / wedge watchdog).

        pipecat's version already does the right thing for Live: close open
        turns, drop the socket, reconnect and re-send `session.start` from the
        current context (so the conversation so far is seeded as history).
        The flag just tells the receive-loop wrapper that the old reader's
        end is expected.
        """
        self._resetting_conversation = True
        try:
            await super().reset_conversation()
        finally:
            self._resetting_conversation = False
        self.session_expires_at = None
        # pipecat marks the processor unusable on a permanent error (the
        # connection-death ErrorFrames that triggered this reconnect). The
        # worker's default CONTINUE policy keeps routing frames regardless,
        # but the flag should reflect the fresh connection.
        await self.set_usable(True)

    async def _receive_task_handler(self):  # type: ignore[override]
        """Surface reader death as an ErrorFrame ConnectionRecovery acts on.

        Same rationale as SafeRealtimeLLMService: pipecat's loop can end
        without any ErrorFrame (clean server close), which would leave the
        session deaf until the next utterance hits the dead socket.
        """
        try:
            await super()._receive_task_handler()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            await self.push_error(error_msg=f"live receive loop died: {e!r}")
            return
        if self._resetting_conversation or self._disconnecting or self._closing:
            return
        await self.push_error(error_msg="live receive loop ended — connection closed")
