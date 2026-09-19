"""Gemini Live provider for the Voice PE add-on.

`SafeGeminiLiveLLMService` is the Gemini-Live twin of `SafeLiveLLMService`
(app/live_service.py, GPT-Live-1); it bends pipecat's `GeminiLiveLLMService`
(`google.genai` `bidiGenerateContent`, `models/{model}` on
`wss://generativelanguage.googleapis.com`) to the add-on's device contract.
Unlike GPT-Live-1, Gemini Live is NOT a delegation architecture: there is no
separate backend model — Gemini calls the registered tools itself, the same
way `gpt-realtime-2` does, so `instructions` reach it unmodified (+ the
household memory notes, same as every other path). `live_instructions()`
(live_service.py) was deliberately NOT reused here: its delegation/no-filler
rules are written for "a backend model holds all the tools", which does not
describe Gemini and would be actively misleading in its prompt. If the
"let me check" filler the probe observed on `-extended-thinking` models turns
out to matter in practice, a Gemini-specific one-line instruction is a
follow-up, not a guess baked in now — see the phase-1 report's open
questions.

Differences from pipecat's stock `GeminiLiveLLMService` that this class adds,
mirroring `SafeLiveLLMService` where the same problem applies to a full-duplex
model behind a half-duplex device:

  * Session bootstrap. pipecat's Gemini Live service opens its websocket
    unconditionally in `setup()` (before any context exists), using the
    init-provided `system_instruction`/`tools` — unlike GPT-Live-1, so no
    override is needed just to get instructions/tools sent. But cached
    conversation history (a reconnecting device) still needs to reach the
    session as seeded context, and pipecat only does that from an
    `LLMContextFrame`, which this pipeline never queues (no `LLMRunFrame` on
    connect). So, like `SafeLiveLLMService`, this class is handed the
    aggregator pair's context (`set_bootstrap_context`) and calls
    `_handle_context()` itself right after `StartFrame`.
    `inference_on_context_initialization=False` (passed at construction, see
    `main.py::_build_gemini_service`) stops pipecat's own "seed with system
    instruction to trigger a first response" behaviour, which would otherwise
    make a brand-new session greet the room unprompted on connect — the same
    problem `Application._preseed_context` solves for the Realtime path.
  * Tools. The add-on's tool dicts are OpenAI-native
    (`{"type":"function","name",...,"parameters"}`); Gemini's `tools=` kwarg
    wants a `ToolsSchema`/`FunctionSchema` list (or Gemini-native dicts) so
    its own adapter can build `functionDeclarations`. `openai_tools_to_gemini`
    converts them; `FunctionSchema.to_default_dict()` emits lowercase
    JSON-Schema types ("object"/"string") and the installed `google-genai`
    SDK normalizes those to Gemini's uppercase wire format on serialization
    (verified empirically against the installed `google-genai` package,
    2026-09-19) — no manual case conversion needed. Handlers are registered
    through the same `guarded_tool_handler` (speaker gate + turn-liveness)
    both OpenAI classes use, with `cancel_on_interruption=False` forced to
    match their contract exactly. gemini-3.8-live does not support pipecat's
    NON_BLOCKING tool-scheduling hints for that flag (Gemini 3.x limitation,
    not this add-on's), so pipecat logs a one-time cosmetic warning the first
    time an async-tool-style result lands — `push_error` below swallows only
    that specific message so it can't reach `ConnectionRecovery` as a
    generic error and force a live tool call's phase to `idle`.
  * Turn frames are SERVICE-DRIVEN, same reason as GPT-Live-1: Gemini Live
    emits no `UserStartedSpeakingFrame`/`UserStoppedSpeakingFrame` at all
    (pipecat's own class docstring says so explicitly — it expects a local
    VAD instead, which this half-duplex device does not have). The FIRST
    input-transcription fragment while the bot is not responding opens the
    turn (one `UserStartedSpeakingFrame`); the model starting to respond
    (`_set_bot_is_responding(True)`, wherever it first fires — audio or
    output-transcription) closes it (one `UserStoppedSpeakingFrame`). A
    fragment that arrives while the bot IS responding (a late tail of the
    question, or echo) still reaches the context (pipecat's own transcription
    handling is untouched) but does not announce a turn — same reasoning as
    `SafeLiveLLMService._open_turn`'s `bot_is_speaking()` guard, simpler here
    because Gemini's own `_bot_is_responding` flag is authoritative rather
    than a silence heuristic. The base class's assistant-turn frames
    (`TTSStartedFrame`/`LLMFullResponseStartFrame` … `TTSStoppedFrame`/
    `LLMFullResponseEndFrame`) already work unmodified — pipecat's Gemini
    service happens to emit exactly the sequence `SafeLiveLLMService` has to
    emit by hand for OpenAI — so this class does not touch them.
  * Input clock (same root cause as GPT-Live-1's phase-4 fix, ported 1:1:
    same constants, same log lines). A full-duplex model's timeline only
    advances while it receives input audio; the Voice PE is half-duplex and
    gates its mic during a reply. While the conversation is active and no
    device audio has arrived for `INPUT_CLOCK_GAP_S`, real-time-paced silence
    (PCM16 zeros at the service's own sample rate) is fed via
    `session.send_realtime_input(audio=Blob(...))` so a tool result lands
    while the model can still "hear" and speak it. Idle time is not fed.
  * Output audio gate. If Gemini streams silence between utterances the way
    GPT-Live-1 does (not verified from here — see the phase-1 report), the
    Voice PE would never see a gap long enough to leave `replying`. Silence is
    dropped after a short hangover, same as `SafeLiveLLMService`'s gate; a
    harmless no-op if Gemini in fact sends none.
  * `goAway`. pipecat's Gemini Live service has NO handler for the server's
    `goAway` warning (grepped the installed package — confirmed absent) and
    no smaller seam than the whole receive loop to add one, so
    `_connection_task_handler` is duplicated with one added branch (see that
    method's docstring for the re-diff instruction on a pipecat upgrade).
    On `goAway`, this class waits (bounded) for a quiet moment
    (`is_busy()`) and reconnects proactively, reusing the stored session
    resumption handle — the same handle pipecat's own `_reconnect()` already
    passes to `_connect()` on every reconnect it triggers itself, so no
    override was needed just to preserve resumption.
  * Recovery. pipecat's Gemini Live service, UNLIKE both OpenAI classes,
    already retries its own connection up to `MAX_CONSECUTIVE_FAILURES`
    times with the session resumption handle, entirely on its own. It only
    calls `push_error()` once it gives up, with a message that does not
    match any of `ConnectionRecovery`'s death markers — deliberately: most
    transient drops should self-heal without `ConnectionRecovery` ever
    seeing them. `_handle_connection_error` below adds our own "receive
    loop died" marker ONLY on that give-up path, so `ConnectionRecovery`
    still has a `reset_conversation()` to fall back on as a last resort,
    exactly like both OpenAI classes.
  * `session_expires_at` stays `None` (Gemini's `LiveServerMessage` carries no
    session-expiry field), so `ConnectionRecovery` falls back to its
    age-based (55 min) proactive refresh — see the phase-1 report's open
    questions on whether/when Gemini actually drops an idle connection.
"""
import asyncio
import logging
import time
from typing import Optional

from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.audio.utils import is_silence
from pipecat.frames.frames import (
    InputAudioRawFrame,
    LLMFullResponseStartFrame,
    LLMTextFrame,
    LLMThoughtEndFrame,
    LLMThoughtStartFrame,
    LLMThoughtTextFrame,
    StartFrame,
    TTSAudioRawFrame,
    TTSStartedFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.services.google.gemini_live.llm import GeminiLiveLLMService
from pipecat.utils.types import assert_given

from google.genai.types import Blob, Content, Part

from app.live_mode import LiveModeService
from app.tool_guard import guarded_tool_handler

logger = logging.getLogger(__name__)

# How long silence keeps flowing to the device after the last speech chunk.
# Must exceed pipecat's BOT_VAD_STOP_SECS (0.35 s) so the output transport
# sees a silence frame late enough to declare the bot stopped. Same value as
# SafeLiveLLMService (live_service.py) — same transport, same requirement.
OUTPUT_SILENCE_HANGOVER_S = 0.5
# Safety cap on the post-"stop" output mute.
OUTPUT_MUTE_MAX_S = 10.0

# Input clock (see module docstring). Ported 1:1 from SafeLiveLLMService.
INPUT_CLOCK_INTERVAL_S = 0.1
INPUT_CLOCK_GAP_S = 0.3
INPUT_CLOCK_MAX_CHUNK_S = 0.5
INPUT_CLOCK_TAIL_S = 12.0

# How long to wait, quiet-polling at 1 Hz, for is_busy() to clear before
# reconnecting proactively on a goAway. Gemini's `time_left` is an opaque
# duration string (e.g. "10s"), not parsed here (not worth the fragility for
# a value that's only ever used as a soft deadline) — if the house never goes
# quiet in time, the server's own forced close falls through to
# `_handle_connection_error` / pipecat's built-in retry, same as any other
# connection drop.
GO_AWAY_MAX_WAIT_S = 20.0


def model_supports_thinking_level(model: str) -> bool:
    """True for Gemini's "-extended-thinking" model variants.

    Verified live 2026-09-19 (gemini-live-probe.py): an extended-thinking
    model REJECTS the connection (close 1007) without
    `generationConfig.thinkingConfig.thinkingLevel`; a plain model must NOT
    be sent one at all.
    """
    return "extended-thinking" in (model or "")


def openai_tools_to_gemini(tool_dicts: list) -> ToolsSchema:
    """Convert the add-on's OpenAI-native tool dicts to a Gemini `ToolsSchema`.

    Every tool the add-on registers (HA MCP, ask_openclaw/recall_memory,
    timers, memory, enrollment, web_search, disconnect) is built once, in
    OpenAI's flat `{"type":"function","name",...,"parameters"}` shape, and
    reused for every provider (`main.py::create_openai_service`). Handing
    that shape straight to Gemini's `tools=` kwarg would not raise — pipecat
    passes an unrecognized list through as "provider-native" — but it is the
    WRONG provider's native shape (Gemini wants
    `{"function_declarations": [...]}`), so tools would silently not work.
    `FunctionSchema`/`ToolsSchema` is pipecat's own standard form; its Gemini
    adapter (`GeminiLiveLLMAdapter.to_provider_tools_format`) builds the
    `functionDeclarations` list from it.
    """
    functions = [
        FunctionSchema(
            name=t.get("name", ""),
            description=t.get("description", "") or "",
            properties=(t.get("parameters") or {}).get("properties", {}) or {},
            required=(t.get("parameters") or {}).get("required", []) or [],
        )
        for t in tool_dicts
    ]
    return ToolsSchema(standard_tools=functions)


class SafeGeminiLiveLLMService(LiveModeService, GeminiLiveLLMService):
    """GeminiLiveLLMService adapted to the Voice PE add-on (see module docstring)."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._bootstrap_context = None
        self._closing = False
        # Wired by Application.create_openai_service (same as both OpenAI classes).
        self.speaker_probe = None
        self.male_only_tools: set = set()
        self.turn_liveness = None
        # Gemini has no session-id/expiry event; these stay None. session_id is
        # exposed for parity/logging; session_expires_at=None makes
        # ConnectionRecovery fall back to its age-based refresh.
        self.session_id: Optional[str] = None
        self.session_expires_at: Optional[int] = None
        # User-turn state (service-driven; see module docstring).
        self._user_turn_open = False
        # Output gate state.
        self._last_speech_mono = 0.0
        self._dropped_silence_frames = 0
        self._output_muted_until = 0.0
        # Input clock.
        self._last_input_audio_mono = 0.0
        self._last_activity_mono = 0.0
        self._input_clock_task = None
        self._input_clock_feeding = False
        self._input_clock_fed_ms = 0
        self._input_clock_total_ms = 0
        # goAway: reconnect proactively at most once per warning.
        self._go_away_task = None

    # ---- bootstrap -----------------------------------------------------------

    def set_bootstrap_context(self, context) -> None:
        """Context the session seeds itself from right after StartFrame."""
        self._bootstrap_context = context

    async def process_frame(self, frame, direction):  # type: ignore[override]
        if isinstance(frame, InputAudioRawFrame):
            now = time.monotonic()
            self._last_input_audio_mono = now
            self._last_activity_mono = now
        await super().process_frame(frame, direction)
        if isinstance(frame, StartFrame) and self._context is None and self._bootstrap_context is not None:
            # pipecat's _connect() already ran (in setup(), before StartFrame)
            # with the init-provided system_instruction/tools; this seeds
            # cached history (a reconnecting device) as the session's context
            # without triggering a spontaneous reply (see
            # inference_on_context_initialization=False in
            # main.py::_build_gemini_service).
            await self._handle_context(self._bootstrap_context)

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

    async def push_error(self, error_msg, exception=None, fatal=False,
                          category=None, force_treat_as_permanent=False):  # type: ignore[override]
        """Swallow pipecat's benign "cancel_on_interruption=False is not
        properly supported" warning (see module docstring) so it can't reach
        ConnectionRecovery as a generic ErrorFrame and force a live tool
        call's phase to idle."""
        if error_msg and "is not properly supported by the current Gemini Live model" in error_msg:
            logger.debug(f"suppressing benign Gemini async-tool-scheduling warning: {error_msg}")
            return
        await super().push_error(
            error_msg, exception=exception, fatal=fatal, category=category,
            force_treat_as_permanent=force_treat_as_permanent,
        )

    # ---- turn frames: service-driven (Gemini emits none itself) ---------------

    async def _handle_msg_input_transcription(self, message):  # type: ignore[override]
        sc = message.server_content
        text = sc.input_transcription.text if sc and sc.input_transcription else None
        if text and not self._user_turn_open and not self._bot_is_responding:
            self._user_turn_open = True
            logger.info("🎙️ user turn opened (first transcript fragment) → listening")
            await self.push_frame(UserStartedSpeakingFrame())
        await super()._handle_msg_input_transcription(message)

    async def _set_bot_is_responding(self, responding: bool) -> None:  # type: ignore[override]
        became_responding = responding and not self._bot_is_responding
        await super()._set_bot_is_responding(responding)
        if became_responding and self._user_turn_open:
            self._user_turn_open = False
            logger.info("🗣️ user turn closed (model turn begins) → thinking/replying")
            await self.push_frame(UserStoppedSpeakingFrame())

    async def _handle_msg_turn_complete(self, message):  # type: ignore[override]
        if self._user_turn_open:
            # Defensive: the model finished its turn without ever responding
            # (should be rare), so nothing closed the user turn above — do it
            # here instead of leaving the device stuck in `listening`.
            self._user_turn_open = False
            logger.info("🎙️ user turn closed (turn complete with no reply)")
            await self.push_frame(UserStoppedSpeakingFrame())
        await super()._handle_msg_turn_complete(message)

    # ---- output audio gate (duplicates pipecat's _handle_msg_model_turn; see
    # module docstring — no smaller seam exists to intercept only the audio
    # part without also handling text/thought frames) -------------------------

    async def _handle_msg_model_turn(self, msg):  # type: ignore[override]
        assert msg.server_content is not None and msg.server_content.model_turn is not None
        parts = msg.server_content.model_turn.parts
        if not parts:
            return
        part = parts[0]

        await self.stop_ttfb_metrics()

        text = part.text
        if text:
            if not self._bot_is_responding:
                await self._set_bot_is_responding(True)
                await self.push_frame(LLMFullResponseStartFrame())
            if part.thought:
                await self.push_frame(LLMThoughtStartFrame())
                await self.push_frame(LLMThoughtTextFrame(text))
                await self.push_frame(LLMThoughtEndFrame())
            else:
                self._bot_text_buffer += text
                self._search_result_buffer += text
                await self.push_frame(LLMTextFrame(text=text))

        if msg.server_content and msg.server_content.grounding_metadata:
            self._accumulated_grounding_metadata = msg.server_content.grounding_metadata

        inline_data = part.inline_data
        if not inline_data:
            return

        expected_mime_type = f"audio/pcm;rate={self._sample_rate}"
        if inline_data.mime_type == expected_mime_type:
            pass
        elif inline_data.mime_type == "audio/pcm":
            if not hasattr(self, "_sample_rate_warning_logged"):
                logger.warning(
                    f"Sample rate not provided in mime type '{inline_data.mime_type}', "
                    f"assuming rate of {self._sample_rate}"
                )
                self._sample_rate_warning_logged = True
        else:
            logger.warning(f"Unrecognized server_content format {inline_data.mime_type}")
            return

        audio = inline_data.data
        if not audio:
            return

        now = time.monotonic()
        if now < self._output_muted_until:
            return  # device "stop": the user does not want to hear this utterance
        if len(audio) % 2 == 0 and is_silence(audio):
            if now - self._last_speech_mono > OUTPUT_SILENCE_HANGOVER_S:
                # Continuous silence between utterances (if Gemini streams
                # any, mirroring GPT-Live-1's behaviour — not verified from
                # here): never forward it, or the device stays in playback
                # and never reaches idle.
                self._dropped_silence_frames += 1
                return
        else:
            self._last_speech_mono = now
            self._last_activity_mono = now

        if not self._bot_is_responding:
            await self._set_bot_is_responding(True)
            await self.push_frame(TTSStartedFrame())
            await self.push_frame(LLMFullResponseStartFrame())

        self._bot_audio_buffer.extend(audio)
        await self.push_frame(
            TTSAudioRawFrame(audio=audio, sample_rate=self._sample_rate, num_channels=1)
        )

    # ---- device hooks (called from websocket_handler.build_pipeline) --------------

    async def handle_device_interrupt(self) -> None:
        """Device "stop": mute this utterance locally and let the local
        pipeline treat the reply as over.

        Gemini Live has no equivalent of OpenAI's `response.cancel` or
        `session.instructions.append("stop speaking")` — the closest lever,
        sending real-time input to trigger the server's own barge-in
        detection, needs audio that resembles speech, which nothing here
        produces on a spoken "stop". The device already discards incoming
        audio on its own "stop", so muting locally costs nothing; the remote
        session may keep generating (and billing) audio for this utterance —
        not verified from here, see the phase-1 report's open questions.
        """
        self._output_muted_until = time.monotonic() + OUTPUT_MUTE_MAX_S
        try:
            await self.broadcast_interruption()
        except Exception as e:
            logger.info(f"🛑 device interrupt → broadcast_interruption no-op ({e!r})")
        logger.info("🛑 device interrupt → output muted locally")

    def note_device_mic_closed(self) -> None:
        """Device follow-up window timed out (`flush`): end the input-clock tail early."""
        self._last_activity_mono = 0.0
        self._last_input_audio_mono = 0.0

    async def inject_context(self, text: str) -> None:
        """Quiet session context (speaker verdicts).

        Gemini has no dedicated silent-context channel like OpenAI's
        `session.thinking.append`. The closest analog already in this file is
        `send_client_content(turn_complete=False)` — the exact call pipecat's
        own `_create_initial_response` uses to seed history WITHOUT
        triggering inference — so it is reused here for the same "add to
        context, don't reply" effect.
        """
        if not self._session or not self._ready_for_realtime_input or not text:
            return
        try:
            await self._session.send_client_content(
                turns=[Content(role="user", parts=[Part(text=text)])],
                turn_complete=False,
            )
        except Exception as e:
            logger.info(f"quiet context injection no-op ({e!r})")

    def is_busy(self) -> bool:
        """True while a turn or a tool call is in flight (refresh guard)."""
        return bool(
            self._user_turn_open
            or self._bot_is_responding
            or (self.turn_liveness is not None and self.turn_liveness.in_flight > 0)
            or time.monotonic() - self._last_speech_mono < 2.0
        )

    # ---- input clock ------------------------------------------------------------

    def conversation_active(self, now: Optional[float] = None) -> bool:
        """True while the model may still have to produce something (feed the clock)."""
        now = time.monotonic() if now is None else now
        return bool(
            self._user_turn_open
            or self._bot_is_responding
            or (self.turn_liveness is not None and self.turn_liveness.in_flight > 0)
            or now - self._last_activity_mono < INPUT_CLOCK_TAIL_S
        )

    async def _input_clock_tick(self, now: float, clock: float) -> int:
        """Feed silence covering `now - clock` if the device mic is gated. Returns ms fed."""
        if not self._session or not self._ready_for_realtime_input or self._disconnecting:
            return 0
        if now - self._last_input_audio_mono < INPUT_CLOCK_GAP_S:
            return 0  # device audio is flowing: it IS the clock
        if not self.conversation_active(now):
            return 0
        ms = int(round(min(max(now - clock, 0.0), INPUT_CLOCK_MAX_CHUNK_S) * 1000))
        if ms <= 0:
            return 0
        n_samples = self._sample_rate * ms // 1000
        silence = b"\x00\x00" * n_samples
        try:
            await self._session.send_realtime_input(
                audio=Blob(data=silence, mime_type=f"audio/pcm;rate={self._sample_rate}")
            )
        except Exception as e:
            logger.debug(f"input clock send failed: {e!r}")
            return 0
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
            self._input_clock_task = self.create_task(self._input_clock_loop(), "gemini-input-clock")
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

    async def _handle_session_ready(self, session):  # type: ignore[override]
        await super()._handle_session_ready(session)
        self._start_input_clock()

    # ---- goAway (see module docstring) -------------------------------------------

    async def _connection_task_handler(self, config):  # type: ignore[override]
        """Identical to pipecat's `GeminiLiveLLMService._connection_task_handler`
        (pipecat-ai 1.10.0, `pipecat/services/google/gemini_live/llm.py`) with
        ONE addition: a `message.go_away` branch. pipecat has no handler for
        this message anywhere (confirmed by grepping the installed package)
        and the whole receive loop is a single method with no smaller seam to
        hook, so this duplicates it rather than patching one branch. Re-diff
        against the installed pipecat-ai source on any version bump — see
        pyproject.toml's pipecat-ai pin comment for the same rule applied to
        the rest of this add-on.
        """
        model = assert_given(self._settings.model)
        if model is None:
            raise ValueError("Gemini Live model must be specified")
        async with self._client.aio.live.connect(model=model, config=config) as session:
            logger.info("Connected to Gemini service")
            self._connection_start_time = time.time()
            await self._handle_session_ready(session)

            while True:
                try:
                    turn = session.receive()
                    async for message in turn:
                        self._check_and_reset_failure_counter()

                        sc = message.server_content
                        if sc and sc.interrupted:
                            logger.debug("Gemini VAD: interrupted signal received")
                            await self.broadcast_interruption()
                        if sc and sc.model_turn:
                            await self._handle_msg_model_turn(message)
                        if sc and sc.input_transcription:
                            await self._handle_msg_input_transcription(message)
                        if sc and sc.output_transcription:
                            await self._handle_msg_output_transcription(message)
                        if (
                            sc
                            and sc.grounding_metadata
                            and not sc.model_turn
                            and not sc.output_transcription
                        ):
                            await self._handle_msg_grounding_metadata(message)
                        if sc and sc.turn_complete:
                            if not message.usage_metadata:
                                logger.warning("Received turn_complete without usage_metadata")
                            await self._handle_msg_turn_complete(message)
                            if message.usage_metadata:
                                await self._handle_msg_usage_metadata(message)
                        if message.tool_call:
                            await self._handle_msg_tool_call(message)
                        if message.session_resumption_update:
                            self._handle_msg_resumption_update(message)
                        if message.go_away:
                            self._handle_go_away(message.go_away)
                except Exception as e:
                    if not self._disconnecting:
                        should_reconnect = await self._handle_connection_error(e)
                        if should_reconnect:
                            await self._reconnect()
                            return  # Exit this connection handler, _reconnect will start a new one
                    break

    def _handle_go_away(self, go_away) -> None:
        """Server warns it will close the connection soon (session max
        duration, load shedding, ...). Reconnect proactively (reusing the
        session resumption handle) once the house is quiet, so the forced
        close never lands mid-turn; if it lands anyway before that,
        `_handle_connection_error`'s own retry (or, past
        MAX_CONSECUTIVE_FAILURES, ConnectionRecovery) takes over."""
        if self._go_away_task is not None and not self._go_away_task.done():
            return
        logger.warning(f"⚠️ Gemini goAway: server will close this connection soon (time_left={go_away.time_left})")
        self._go_away_task = self.create_task(self._reconnect_before_go_away(), "gemini-go-away-reconnect")

    async def _reconnect_before_go_away(self) -> None:
        deadline = time.monotonic() + GO_AWAY_MAX_WAIT_S
        try:
            while time.monotonic() < deadline and self.is_busy():
                await asyncio.sleep(1.0)
            if self._disconnecting or self._closing:
                return
            logger.info("🔄 Gemini goAway: reconnecting now (reusing the session resumption handle)")
            await self._reconnect()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"⚠️ goAway proactive reconnect failed: {e!r}")

    # ---- lifecycle / recovery ---------------------------------------------------

    async def _handle_connection_error(self, error) -> bool:  # type: ignore[override]
        """pipecat's own retry (up to MAX_CONSECUTIVE_FAILURES, reusing the
        session resumption handle) runs first and self-heals most drops
        silently. Only when it gives up do we add our own "receive loop
        died" marker, so ConnectionRecovery (websocket_handler.py) has a
        reset_conversation() to fall back on as a last resort — same
        contract as both OpenAI service classes. This does mean the give-up
        path pushes two ErrorFrames (pipecat's generic one, then ours);
        ConnectionRecovery's reconnect cooldown collapses the duplicate.
        """
        should_reconnect = await super()._handle_connection_error(error)
        if not should_reconnect:
            await self.push_error(error_msg=f"gemini live receive loop died: {error!r}")
        return should_reconnect

    async def reset_conversation(self):  # type: ignore[override]
        """ConnectionRecovery's one PUBLIC reconnect entrypoint. pipecat's
        own `_reconnect()` already does the right thing for Gemini: drop the
        socket and reconnect with the stored session resumption handle so
        the server restores the conversation itself."""
        await self._reconnect()

    async def stop(self, frame):  # type: ignore[override]
        self._closing = True
        await self._stop_input_clock()
        await super().stop(frame)

    async def _disconnect(self):  # type: ignore[override]
        await self._stop_input_clock()
        task, self._go_away_task = self._go_away_task, None
        if task is not None and not task.done() and task is not asyncio.current_task():
            task.cancel()
        await super()._disconnect()
