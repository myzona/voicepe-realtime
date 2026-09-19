"""Gemini Live path smoke test — no network.

Companion to test_pipeline_smoke.py (gpt-realtime-2 baseline) and
test_pipeline_smoke_live.py (gpt-live-1). With `llm_provider: gemini` selected,
Application.create_openai_service builds a SafeGeminiLiveLLMService (pipecat
GeminiLiveLLMService). Only the Gemini websocket connect
(`_connection_task_handler`) is stubbed; everything else is the production
path.

Pinned here:

  * provider selection: llm_provider=gemini -> SafeGeminiLiveLLMService;
    default/openai -> SafeRealtimeLLMService unchanged; openai_model
    gpt-live-1 -> SafeLiveLLMService unchanged;
  * the LiveConnectConfig built at _connect(): no thinking_config for
    gemini-3.8-live, thinking_level set for a "-extended-thinking" model;
    system instruction, voice and the tool declarations (function_declarations
    covering EXPECTED_TOOL_NAMES) land in it;
  * every registered handler has cancel_on_interruption=False and is
    guard-wrapped (speaker gate + turn-liveness);
  * session resumption: a session_resumption_update handle is stored and the
    next _connect() is called with it; a go_away message triggers a
    quiet-gated reconnect that also passes the handle;
  * turn frames are service-driven: the first input-transcription fragment
    while the bot is not responding is ONE UserStartedSpeakingFrame, the
    model starting to respond is ONE UserStoppedSpeakingFrame, and a
    fragment while the bot IS responding announces neither;
  * the output audio gate drops inter-utterance silence (after a hangover)
    and honours the post-"stop" mute;
  * the input clock feeds silence while the device mic is gated and stops
    when device audio resumes;
  * pipeline topology (no RTVIProcessor, no ContextInitializer) and an
    end-to-end run on a fake device;
  * selecting llm_provider=openai (default) is entirely unaffected.
"""
import asyncio
import json
import os
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from google.genai import types as genai_types

from pipecat.frames.frames import (
    TranscriptionFrame,
    TTSAudioRawFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.frame_processor import FrameDirection

from app.device_registry import DeviceConnection
from app.gemini_live_service import (
    GO_AWAY_MAX_WAIT_S,
    INPUT_CLOCK_GAP_S,
    INPUT_CLOCK_TAIL_S,
    OUTPUT_SILENCE_HANGOVER_S,
    SafeGeminiLiveLLMService,
    model_supports_thinking_level,
    openai_tools_to_gemini,
    strip_tool_messages,
)
from app.live_service import SafeLiveLLMService
from app.main import Application, SafeRealtimeLLMService
from app.phase_emitter import TurnLiveness
from app.raw_audio_serializer import RawAudioSerializer
from app.websocket_handler import WebSocketHandler
from tests.test_pipeline_smoke import PCM_20MS_16K, N_PCM_FRAMES, FakeWebSocket, configure
from tests.test_pipeline_smoke_live import EXPECTED_TOOL_NAMES, configure_live


def _run_tasks_on_plain_asyncio(service) -> None:
    """Bypass pipecat's TaskManager (only wired up inside a running pipeline)
    for tests that call service internals directly. Coroutines passed to
    create_task still run for real, just as a plain asyncio Task."""
    service.create_task = lambda coro, name=None, context=None: asyncio.get_event_loop().create_task(coro)


def configure_gemini(app: Application) -> None:
    """Mirror Application.initialize() defaults for llm_provider: gemini."""
    configure(app)
    app.llm_provider = "gemini"
    app.gemini_api_key = "fake-gemini-key"
    app.gemini_model = "gemini-3.8-live"
    app.gemini_voice = "Charon"
    app.gemini_thinking_level = "low"


def _model_turn_audio(audio: bytes) -> genai_types.LiveServerMessage:
    return genai_types.LiveServerMessage(
        server_content=genai_types.LiveServerContent(
            model_turn=genai_types.Content(
                parts=[genai_types.Part(inline_data=genai_types.Blob(
                    data=audio, mime_type="audio/pcm;rate=24000",
                ))]
            )
        )
    )


def _input_transcription(text: str) -> genai_types.LiveServerMessage:
    return genai_types.LiveServerMessage(
        server_content=genai_types.LiveServerContent(
            input_transcription=genai_types.Transcription(text=text)
        )
    )


class TestGeminiLivePipelineSmoke(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        os.environ.pop("OPENCLAW_URL", None)
        os.environ.pop("PIPECAT_ALLOWED_ORIGINS", None)
        self.app = Application()
        configure_gemini(self.app)
        self.handler = WebSocketHandler(
            session_manager=self.app.session_manager, follow_up_ms=8000, output_lead_buffer_ms=400
        )
        self.app.websocket_handler = self.handler
        self._connect_patch = patch.object(SafeGeminiLiveLLMService, "_connect", AsyncMock())
        self._connect_patch.start()

    async def asyncTearDown(self):
        self._connect_patch.stop()

    async def _make_connection(self, websocket):
        serializer = RawAudioSerializer("office")
        connection = DeviceConnection(device_id="office", websocket=websocket, serializer=serializer)
        connection.transport = self.handler.create_transport(websocket, serializer)
        connection.turn_liveness = TurnLiveness()
        connection.openai_service = await self.app.create_openai_service(connection)
        return connection

    # ---- provider selection --------------------------------------------------

    async def test_provider_selection(self):
        gemini_connection = await self._make_connection(FakeWebSocket([]))
        self.assertIsInstance(gemini_connection.openai_service, SafeGeminiLiveLLMService)

        realtime_patch = patch.object(SafeRealtimeLLMService, "_connect", AsyncMock())
        realtime_patch.start()
        try:
            configure(self.app)  # default: openai, gpt-realtime-2
            self.app.llm_provider = "openai"  # configure() doesn't touch this fork-specific field
            connection = await self._make_connection(FakeWebSocket([]))
            self.assertIsInstance(connection.openai_service, SafeRealtimeLLMService)
        finally:
            realtime_patch.stop()

        live_patch = patch.object(SafeLiveLLMService, "_connect", AsyncMock())
        live_patch.start()
        try:
            configure_live(self.app)  # openai_model gpt-live-1
            connection = await self._make_connection(FakeWebSocket([]))
            self.assertIsInstance(connection.openai_service, SafeLiveLLMService)
        finally:
            live_patch.stop()

    # ---- connect config -------------------------------------------------------

    async def test_session_start_payload_and_tools(self):
        self._connect_patch.stop()
        try:
            connection = await self._make_connection(FakeWebSocket([]))
            service = connection.openai_service
            _run_tasks_on_plain_asyncio(service)
            captured = {}

            async def fake_handler(config):
                captured["config"] = config

            service._connection_task_handler = fake_handler
            await service._connect()
            await asyncio.sleep(0.05)

            cfg = captured["config"]
            self.assertIsNone(cfg.thinking_config)
            voice_config = cfg.generation_config.speech_config.voice_config
            self.assertEqual(voice_config.prebuilt_voice_config.voice_name, "Charon")
            self.assertEqual(cfg.system_instruction, "You are the test assistant.")
            self.assertEqual(len(cfg.tools), 1)
            names = [d["name"] for d in cfg.tools[0]["function_declarations"]]
            self.assertEqual(sorted(names), sorted(EXPECTED_TOOL_NAMES))

            # Every handler registered, all with cancel_on_interruption=False.
            self.assertEqual(sorted(service._functions), sorted(EXPECTED_TOOL_NAMES))
            for name, item in service._functions.items():
                self.assertFalse(item.cancel_on_interruption, name)
        finally:
            self._connect_patch.start()

    async def test_extended_thinking_model_sends_thinking_level(self):
        self.app.gemini_model = "gemini-3.8-live-extended-thinking"
        self._connect_patch.stop()
        try:
            connection = await self._make_connection(FakeWebSocket([]))
            service = connection.openai_service
            _run_tasks_on_plain_asyncio(service)
            captured = {}

            async def fake_handler(config):
                captured["config"] = config

            service._connection_task_handler = fake_handler
            await service._connect()
            await asyncio.sleep(0.05)

            cfg = captured["config"]
            self.assertIsNotNone(cfg.thinking_config)
            self.assertEqual(cfg.thinking_config.thinking_level, genai_types.ThinkingLevel.LOW)
        finally:
            self._connect_patch.start()

    def test_model_supports_thinking_level(self):
        self.assertFalse(model_supports_thinking_level("gemini-3.8-live"))
        self.assertTrue(model_supports_thinking_level("gemini-3.8-live-extended-thinking"))
        self.assertFalse(model_supports_thinking_level(""))

    def test_openai_tools_to_gemini_conversion(self):
        schema = openai_tools_to_gemini([
            {
                "type": "function", "name": "web_search", "description": "search the web",
                "parameters": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]},
            },
        ])
        names = [f.name for f in schema.standard_tools]
        self.assertEqual(names, ["web_search"])

    # ---- session resumption / goAway -------------------------------------------

    async def test_session_resumption_handle_stored_and_reused_on_reconnect(self):
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        msg = genai_types.LiveServerMessage(
            session_resumption_update=genai_types.LiveServerSessionResumptionUpdate(
                resumable=True, new_handle="handle-123",
            )
        )
        service._handle_msg_resumption_update(msg)
        self.assertEqual(service._session_resumption_handle, "handle-123")

        service._disconnect = AsyncMock()
        reconnected = {}

        async def fake_connect(session_resumption_handle=None):
            reconnected["handle"] = session_resumption_handle

        service._connect = fake_connect
        await service._reconnect()
        self.assertEqual(reconnected["handle"], "handle-123")

    async def test_go_away_triggers_quiet_reconnect_with_handle(self):
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        _run_tasks_on_plain_asyncio(service)
        service._session_resumption_handle = "handle-456"
        reconnected = []
        service._reconnect = AsyncMock(side_effect=lambda: reconnected.append(True))

        # Busy: the reconnect must wait, not fire immediately.
        service._user_turn_open = True
        service._handle_go_away(genai_types.LiveServerGoAway(time_left="10s"))
        await asyncio.sleep(0.05)
        self.assertEqual(reconnected, [])

        # Quiet: the pending task reconnects.
        service._user_turn_open = False
        await service._go_away_task
        self.assertEqual(reconnected, [True])

        # A second goAway while the first task is still running does not
        # spawn a duplicate.
        service._go_away_task = asyncio.get_event_loop().create_task(asyncio.sleep(10))
        before = service._go_away_task
        service._handle_go_away(genai_types.LiveServerGoAway(time_left="5s"))
        self.assertIs(service._go_away_task, before)
        before.cancel()

    async def test_async_tool_warning_is_suppressed(self):
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        with self.assertLogs("app.gemini_live_service", level="DEBUG") as logs:
            await service.push_error(
                error_msg=(
                    "cancel_on_interruption=False is not properly supported by "
                    "the current Gemini Live model."
                )
            )
        self.assertIn("suppressing benign Gemini async-tool-scheduling warning", "\n".join(logs.output))

    # ---- turn frames: service-driven -------------------------------------------

    async def test_user_turn_frames_are_service_driven(self):
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        pushed = []

        async def push_frame(frame, direction=FrameDirection.DOWNSTREAM):
            pushed.append((frame, direction))

        service.push_frame = push_frame
        service.broadcast_frame = AsyncMock()
        service.broadcast_interruption = AsyncMock()

        # Sentence-terminated fragments so the base class's transcription
        # buffer drains immediately and never schedules its own timeout task
        # (which needs a real pipecat TaskManager this bare service doesn't have).
        await service._handle_msg_input_transcription(_input_transcription("turn on the lamp."))
        await service._handle_msg_input_transcription(_input_transcription("Thanks."))
        starts = [f for f, d in pushed if isinstance(f, UserStartedSpeakingFrame)]
        self.assertEqual(len(starts), 1)
        self.assertTrue(service._user_turn_open)
        # The transcript still reaches the aggregator (base class behaviour,
        # untouched): one TranscriptionFrame upstream per complete sentence.
        transcripts = [f for f, d in pushed if isinstance(f, TranscriptionFrame)]
        self.assertEqual([t.text for t in transcripts], ["turn on the lamp.", "Thanks."])

        # The model starting to respond closes the turn: one stop.
        await service._set_bot_is_responding(True)
        self.assertFalse(service._user_turn_open)
        stops = [f for f, d in pushed if isinstance(f, UserStoppedSpeakingFrame)]
        self.assertEqual(len(stops), 1)
        service.broadcast_frame.assert_not_awaited()
        for frame, _ in pushed:
            self.assertNotIn("Proposed", type(frame).__name__)

    async def test_fragment_while_bot_responding_does_not_open_a_turn(self):
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        pushed = []

        async def push_frame(frame, direction=FrameDirection.DOWNSTREAM):
            pushed.append((frame, direction))

        service.push_frame = push_frame
        service._bot_is_responding = True

        await service._handle_msg_input_transcription(_input_transcription("late tail."))
        self.assertFalse(service._user_turn_open)
        self.assertFalse(any(isinstance(f, UserStartedSpeakingFrame) for f, _ in pushed))
        # Still recorded upstream, per base class behaviour.
        self.assertEqual(
            [f.text for f, _ in pushed if isinstance(f, TranscriptionFrame)], ["late tail."]
        )

    async def test_turn_complete_with_no_reply_closes_a_dangling_turn(self):
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        pushed = []

        async def push_frame(frame, direction=FrameDirection.DOWNSTREAM):
            pushed.append((frame, direction))

        service.push_frame = push_frame
        service._user_turn_open = True
        msg = genai_types.LiveServerMessage(
            server_content=genai_types.LiveServerContent(turn_complete=True),
            usage_metadata=genai_types.UsageMetadata(),
        )
        await service._handle_msg_turn_complete(msg)
        self.assertFalse(service._user_turn_open)
        self.assertEqual(
            len([f for f, _ in pushed if isinstance(f, UserStoppedSpeakingFrame)]), 1
        )

    # ---- output audio gate ------------------------------------------------------

    async def test_output_audio_gate_drops_inter_utterance_silence(self):
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        pushed = []

        async def push_frame(frame, direction=FrameDirection.DOWNSTREAM):
            pushed.append(frame)

        service.push_frame = push_frame
        silence = b"\x00\x00" * 480
        speech = b"\x00\x10" * 480

        # Idle stream: silence is dropped.
        await service._handle_msg_model_turn(_model_turn_audio(silence))
        self.assertEqual(pushed, [])

        # Speech passes, and silence within the hangover passes.
        await service._handle_msg_model_turn(_model_turn_audio(speech))
        await service._handle_msg_model_turn(_model_turn_audio(silence))
        audio_frames = [f for f in pushed if isinstance(f, TTSAudioRawFrame)]
        self.assertEqual(len(audio_frames), 2)
        self.assertEqual(audio_frames[0].sample_rate, 24000)

        # After the hangover, silence is dropped again.
        service._last_speech_mono = time.monotonic() - OUTPUT_SILENCE_HANGOVER_S - 1
        await service._handle_msg_model_turn(_model_turn_audio(silence))
        self.assertEqual(len([f for f in pushed if isinstance(f, TTSAudioRawFrame)]), 2)

        # Device "stop": muted until the next turn boundary.
        await service.handle_device_interrupt()
        await service._handle_msg_model_turn(_model_turn_audio(speech))
        self.assertEqual(len([f for f in pushed if isinstance(f, TTSAudioRawFrame)]), 2)
        service._output_muted_until = 0.0
        await service._handle_msg_model_turn(_model_turn_audio(speech))
        self.assertEqual(len([f for f in pushed if isinstance(f, TTSAudioRawFrame)]), 3)

    # ---- input clock ------------------------------------------------------------

    async def test_input_clock_feeds_silence_while_mic_is_gated(self):
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        service._session = AsyncMock()
        service._ready_for_realtime_input = True
        now = time.monotonic()

        # Idle connection: no activity, nothing in flight -> no silence (billing).
        service._last_activity_mono = now - INPUT_CLOCK_TAIL_S - 1
        service._last_input_audio_mono = now - 10
        self.assertEqual(await service._input_clock_tick(now, now - 0.1), 0)
        service._session.send_realtime_input.assert_not_awaited()

        # A turn is open and the device mic is gated: feed the elapsed time.
        service._user_turn_open = True
        fed = await service._input_clock_tick(now, now - 0.1)
        self.assertEqual(fed, 100)
        blob = service._session.send_realtime_input.await_args.kwargs["audio"]
        self.assertEqual(len(blob.data), 24000 * 2 * 100 // 1000)
        self.assertEqual(blob.data.strip(b"\x00"), b"")

        # Device audio flowing -> the device IS the clock; nothing is fed.
        service._last_input_audio_mono = now - INPUT_CLOCK_GAP_S / 2
        self.assertEqual(await service._input_clock_tick(now, now - 0.1), 0)

        # Follow-up window closed (device flush): the tail ends at once.
        service._last_input_audio_mono = now - 1
        service._user_turn_open = False
        service._last_activity_mono = now - 1
        self.assertEqual(await service._input_clock_tick(now, now - 0.1), 100)
        service.note_device_mic_closed()
        self.assertEqual(await service._input_clock_tick(now, now - 0.1), 0)

        # Session not ready -> nothing.
        service._ready_for_realtime_input = False
        service._user_turn_open = True
        self.assertEqual(await service._input_clock_tick(now, now - 0.1), 0)

        # Input frames stamp the clock source.
        service._ready_for_realtime_input = True
        before = service._last_input_audio_mono
        with patch("app.gemini_live_service.GeminiLiveLLMService.process_frame", AsyncMock()):
            from pipecat.frames.frames import InputAudioRawFrame
            await service.process_frame(
                InputAudioRawFrame(audio=b"\x00\x00" * 480, sample_rate=24000, num_channels=1),
                FrameDirection.DOWNSTREAM,
            )
        self.assertGreater(service._last_input_audio_mono, before)

    # ---- pipeline topology / end-to-end ------------------------------------------

    async def test_pipeline_topology_and_run(self):
        inbound = [
            {"type": "websocket.receive", "text": json.dumps({"type": "start"})},
            {"type": "websocket.receive", "text": json.dumps({"type": "wake"})},
        ] + [{"type": "websocket.receive", "bytes": PCM_20MS_16K} for _ in range(N_PCM_FRAMES)] + [
            {"type": "websocket.receive", "text": json.dumps({"type": "ping"})},
        ]
        websocket = FakeWebSocket(inbound)
        built = {}

        async def factory(connection):
            service = await self.app.create_openai_service(connection)
            service._connection_task_handler = AsyncMock()
            built["service"] = service
            return service

        real_build = self.handler.build_pipeline

        def build(connection, activity_callback=None):
            result = real_build(connection, activity_callback)
            built["pipeline"], built["runner"], built["worker"] = result
            built["connection"] = connection
            return result

        self.handler.openai_service_factory = factory
        self.handler.build_pipeline = build
        self._connect_patch.stop()

        def on_client_disconnected(connection):
            self.app.session_manager.handle_client_disconnect(
                connection.device_id, connection.openai_service
            )

        try:
            await asyncio.wait_for(
                self.handler.serve_connection(websocket, on_client_disconnected=on_client_disconnected),
                timeout=15,
            )
        finally:
            self._connect_patch.start()

        pipeline, worker, service = built["pipeline"], built["worker"], built["service"]
        names = [
            type(p).__name__
            for p in pipeline.processors
            if type(p).__name__ not in ("PipelineSource", "PipelineSink")
        ]
        self.assertEqual(
            names,
            [
                "MixedFastAPIWebsocketInputTransport",
                "ConnectionRecovery",
                "InputResampler",
                "SessionActivityTracker",
                "LLMUserAggregator",
                "TranscriptLogger",
                "SafeGeminiLiveLLMService",
                "TranscriptLogger",
                "RealtimeAssistantAggregator",
                "SessionActivityTracker",
                "PhaseEmitter",
                "OutputLeadBuffer",
                "FastAPIWebsocketOutputTransport",
            ],
        )
        texts = [json.loads(data) for kind, data in websocket.sent if kind == "text"]
        self.assertEqual(texts[0]["type"], "hello")
        self.assertIn({"type": "pong"}, texts)
        self.assertTrue(worker.has_finished())
        self.assertEqual(self.handler.devices.ids(), [])
        connection = built["connection"]
        self.assertIsNone(connection.recovery)
        self.assertIsNone(connection.openai_service)
        self.assertIn("office", self.app.session_manager.context_caches)

    # ---- review round 2: reconnect-seed ordering + connect-time failures --------

    def test_strip_tool_messages_drops_only_plumbing(self):
        context = LLMContext(messages=[
            {
                "role": "assistant",
                "tool_calls": [{"id": "call_1", "type": "function",
                                "function": {"name": "llm__GetDateTime", "arguments": "{}"}}],
            },
            {"role": "tool", "tool_call_id": "call_1", "content": "12:41:19"},
            {"role": "user", "content": "What time is it right now?"},
            {"role": "assistant", "content": "It is twelve forty-one PM."},
            {"role": "developer", "content": '{"type": "async_tool", "status": "finished"}'},
        ])
        stripped = strip_tool_messages(context)
        self.assertEqual(
            stripped.get_messages(),
            [
                {"role": "user", "content": "What time is it right now?"},
                {"role": "assistant", "content": "It is twelve forty-one PM."},
            ],
        )
        # The original context is untouched.
        self.assertEqual(len(context.get_messages()), 5)

    async def test_create_initial_response_strips_tool_call_messages_from_the_seed(self):
        """Review round 2 (live HA VM test): a reconnect seed replayed tool
        calls/results as mis-ordered text pseudo-messages ahead of the user
        question that triggered them, which measurably confused the model.
        Only user/assistant text should reach Gemini's seed; the live
        context itself must be untouched afterward."""
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        service._context = LLMContext(messages=[
            {
                "role": "assistant",
                "tool_calls": [{"id": "call_1", "type": "function",
                                "function": {"name": "llm__GetDateTime", "arguments": "{}"}}],
            },
            {"role": "tool", "tool_call_id": "call_1", "content": "12:41:19"},
            {"role": "user", "content": "What time is it right now?"},
            {"role": "assistant", "content": "It is twelve forty-one PM."},
        ])
        service._session = AsyncMock()
        seen = {}

        async def fake_send_client_content(turns, turn_complete):
            seen["turns"] = turns
            seen["turn_complete"] = turn_complete

        service._session.send_client_content = fake_send_client_content
        await service._create_initial_response()

        roles_texts = [(c.role, c.parts[0].text) for c in seen["turns"]]
        self.assertEqual(
            roles_texts,
            [("user", "What time is it right now?"), ("model", "It is twelve forty-one PM.")],
        )
        # The live context (tool-call bookkeeping, future caching) is untouched.
        self.assertEqual(len(service._context.get_messages()), 4)
        self.assertTrue(service._ready_for_realtime_input)

    async def test_handle_connect_failure_drops_stale_resumption_handle_and_retries(self):
        """Review round 2 (live HA VM test): a resumed connect can fail
        outright (observed: Google 1011 'Internal error encountered'), and
        that must not leave the service permanently dead."""
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        service._session_resumption_handle = "stale-handle"
        reconnected = {}

        async def fake_connect(session_resumption_handle=None):
            reconnected["handle"] = session_resumption_handle

        service._connect = fake_connect
        await service._handle_connect_failure(RuntimeError("1011 Internal error encountered"))

        self.assertIsNone(service._session_resumption_handle)
        self.assertIn("handle", reconnected)
        self.assertIsNone(reconnected["handle"])

    async def test_handle_connect_failure_without_a_handle_still_retries(self):
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        service._session_resumption_handle = None
        reconnected = []

        async def fake_connect(session_resumption_handle=None):
            reconnected.append(session_resumption_handle)

        service._connect = fake_connect
        await service._handle_connect_failure(RuntimeError("some other connect error"))
        self.assertEqual(reconnected, [None])

    async def test_handle_connect_failure_gives_up_after_max_consecutive_failures(self):
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        service._consecutive_failures = 2  # one more reaches MAX_CONSECUTIVE_FAILURES (3)
        service._connect = AsyncMock()
        errors = []
        service.push_error = AsyncMock(side_effect=lambda error_msg=None, **kw: errors.append(error_msg))
        await service._handle_connect_failure(RuntimeError("1011 Internal error encountered"))
        service._connect.assert_not_awaited()
        self.assertTrue(any("gemini live receive loop died" in (e or "") for e in errors), errors)

    async def test_connection_task_handler_connect_failure_triggers_fallback(self):
        """The failure happens establishing `async with ... connect(...)`
        itself — before pipecat's own inner message-loop try/except would
        ever run — so this proves the new OUTER try/except actually catches
        it instead of letting it escape uncaught (the bug as observed live:
        `_connection_task_handler unexpected exception`, service dead)."""
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service

        class _FailingConnectCM:
            async def __aenter__(self):
                raise RuntimeError("1011 Internal error encountered")

            async def __aexit__(self, *exc_info):
                return False

        class _FakeLive:
            def connect(self, model, config):
                return _FailingConnectCM()

        class _FakeAio:
            live = _FakeLive()

        class _FakeClient:
            aio = _FakeAio()

        service._client = _FakeClient()
        handled = {}

        async def fake_handle_connect_failure(error):
            handled["error"] = error

        service._handle_connect_failure = fake_handle_connect_failure
        await service._connection_task_handler(config=genai_types.LiveConnectConfig())
        self.assertIn("1011", str(handled["error"]))


if __name__ == "__main__":
    unittest.main()
