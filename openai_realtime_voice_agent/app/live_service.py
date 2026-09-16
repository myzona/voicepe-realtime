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
  * Turn frames stay SERVICE-DRIVEN (phase-1 decision 1). Live has no
    speech_started/stopped events: pipecat derives user turns from
    `session.input_transcript.delta` fragments (0.8 s gap) and broadcasts
    `ProposedUser*SpeakingFrame` for the aggregator's UserTurnController to
    resolve. That controller is exactly what the Realtime path bypasses (it
    swallows a second start while a turn is open and force-stops after 5 s
    without transcript activity), so `_open_turn`/`_end_turn` push one
    `UserStartedSpeakingFrame` / one `UserStoppedSpeakingFrame` downstream
    instead, which is what PhaseEmitter and the device need. No interruption
    is broadcast: the live model handles being talked over itself and a
    device "stop" is handled explicitly (`handle_device_interrupt`).
  * Output audio gate. The Live model streams output at real-time pace,
    silence included. The Voice PE treats every binary frame as reply audio,
    so a continuous silent stream would keep it in playback forever (no idle,
    no follow-up window, no `stop` re-arm). Silence is dropped after a short
    hangover; the transport still sees enough trailing silence to fire
    BotStoppedSpeaking promptly.
  * Recovery hooks. The receive loop reports its end as an ErrorFrame
    (`live receive loop ...`), an unexpected `session.closed` (expiry, safety,
    upstream loss) reports `live session closed`, and `session_expires_at` is
    exposed so ConnectionRecovery can refresh before `expires_at`.
"""
import asyncio
import base64
import logging
import time
from typing import Optional

from pipecat.audio.utils import is_silence
from pipecat.frames.frames import (
    Frame,
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

# How long silence keeps flowing to the device after the last speech chunk.
# Must exceed pipecat's BOT_VAD_STOP_SECS (0.35 s) so the output transport
# sees a silence frame late enough to declare the bot stopped.
OUTPUT_SILENCE_HANGOVER_S = 0.5
# Safety cap on the post-"stop" output mute (normally lifted by the next
# assistant/user turn boundary).
OUTPUT_MUTE_MAX_S = 10.0

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


class SafeLiveLLMService(OpenAILiveLLMService):
    """OpenAILiveLLMService adapted to the Voice PE add-on (see module docstring)."""

    def __init__(self, *, tools: Optional[list] = None, **kwargs):
        super().__init__(**kwargs)
        # Provider-native tool dicts ({"type":"function","name",...}) — the
        # exact list the Realtime path puts in session.update.tools.
        self._tool_dicts = list(tools) if tools else []
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

    # ---- bootstrap -----------------------------------------------------------

    def set_bootstrap_context(self, context) -> None:
        """Context the session is started from right after StartFrame."""
        self._bootstrap_context = context

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, StartFrame) and self._context is None and self._bootstrap_context is not None:
            # pipecat would wait for an LLMContextFrame (an LLMRunFrame queued
            # by the app). Start from the aggregator pair's context instead:
            # its cached messages become the session's `input` history.
            await self._handle_context(self._bootstrap_context)

    def _invocation_params(self):  # type: ignore[override]
        params = super()._invocation_params()
        if self._tool_dicts and not params["tools"]:
            params["tools"] = [dict(t) for t in self._tool_dicts]
        return params

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

    async def _open_turn(self, role: str):  # type: ignore[override]
        if role == "user":
            # First transcript fragment of a user turn. One UserStartedSpeaking
            # downstream (PhaseEmitter → `listening`, lifts the device's
            # post-stop mute); no ProposedUser* for the aggregator, no
            # interruption broadcast (full duplex: the model yields itself).
            self._output_muted_until = 0.0
            await self.push_frame(UserStartedSpeakingFrame())
        else:
            await self.push_frame(LLMFullResponseStartFrame())
            await self.push_frame(TTSStartedFrame())

    async def _end_turn(self, role: str):  # type: ignore[override]
        turn = self._user_turn if role == "user" else self._assistant_turn
        if not turn.open:
            return
        turn.open = False
        text, turn.text = turn.text, ""
        if role == "user":
            if text.strip():
                await self.push_frame(
                    TranscriptionFrame(text, "", time_now_iso8601()),
                    FrameDirection.UPSTREAM,
                )
            await self.push_frame(UserStoppedSpeakingFrame())
        else:
            # A post-"stop" mute ends with the utterance it silenced.
            self._output_muted_until = 0.0
            await self.push_frame(TTSStoppedFrame())
            await self.push_frame(LLMFullResponseEndFrame())

    # ---- output audio gate ----------------------------------------------------

    async def _handle_evt_audio_delta(self, evt: live_events.OutputAudioDeltaEvent):  # type: ignore[override]
        audio = base64.b64decode(evt.delta)
        if not audio:
            return
        now = time.monotonic()
        if now < self._output_muted_until:
            return  # device "stop": the user does not want to hear this utterance
        if is_silence(audio):
            if now - self._last_speech_mono > OUTPUT_SILENCE_HANGOVER_S:
                # Continuous silence between utterances: never forward it, or the
                # device stays in playback and never reaches idle.
                self._dropped_silence_frames += 1
                return
        else:
            self._last_speech_mono = now
        await self.push_frame(
            SpeechOutputAudioRawFrame(audio=audio, sample_rate=OPENAI_SAMPLE_RATE, num_channels=1)
        )

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
            or time.monotonic() - self._last_speech_mono < 2.0
        )

    # ---- lifecycle / recovery ---------------------------------------------------

    async def _handle_evt_session_started(self, evt):  # type: ignore[override]
        self.session_expires_at = evt.session.expires_at
        self.session_id = evt.session.id
        ttl = ""
        if self.session_expires_at:
            ttl = f", expires in {max(0, self.session_expires_at - time.time()) / 60:.0f} min"
        audio = evt.session.audio or {}
        voice = (audio.get("output") or {}).get("voice") if isinstance(audio, dict) else None
        logger.info(
            f"🟢 Live session {self.session_id} started (model={evt.session.model}, "
            f"voice={voice}{ttl})"
        )
        await super()._handle_evt_session_started(evt)

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
        await super()._handle_evt_error(evt)

    async def _report_usage(self, usage):  # type: ignore[override]
        # Phase-4 cost check needs this in the add-on log at INFO.
        if usage.seconds is not None:
            logger.info(f"💰 live usage: {usage.seconds:.0f}s of session audio (cumulative)")

    async def stop(self, frame):  # type: ignore[override]
        self._closing = True
        await super().stop(frame)

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
