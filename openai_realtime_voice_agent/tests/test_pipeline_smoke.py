"""Construct and run the real per-device pipeline on pipecat 1.x — no network.

This is the pipecat-1.10 migration smoke test. It builds a SafeRealtimeLLMService
through Application.create_openai_service (real tool registration, real
SessionProperties), builds the real pipeline through
WebSocketHandler.build_pipeline (real serializer, mixed transport, aggregator
pair, PhaseEmitter, OutputLeadBuffer ...), then RUNS it on a fake starlette
WebSocket that delivers a control frame, two PCM frames and a disconnect. The
only thing stubbed is the OpenAI WebSocket connect (so nothing leaves the box);
everything else is the production code path.

It also pins the things the migration must keep identical to pipecat 0.0.97:

  * the session.update payload OpenAI would receive (model, instructions,
    voice/speed, semantic_vad, tool list),
  * the pipeline topology (no RTVIProcessor injected by pipecat 1.x),
  * every tool registered with cancel_on_interruption=False,
  * speech_started/stopped emitting UserStarted/StoppedSpeakingFrame plus an
    interruption from the service itself.
"""
import asyncio
import base64
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from starlette.websockets import WebSocketState

from pipecat.frames.frames import UserStartedSpeakingFrame, UserStoppedSpeakingFrame

from app.device_registry import DeviceConnection
from app.main import Application, SafeRealtimeLLMService
from app.phase_emitter import TurnLiveness
from app.raw_audio_serializer import RawAudioSerializer
from app.session_manager import SessionManager
from app.timers import TimerRegistry
from app.websocket_handler import WebSocketHandler

PCM_20MS_16K = b"\x01\x00" * 320  # 20 ms of 16 kHz mono PCM16
# The soxr streaming resampler primes on the first ~4 x 20 ms chunks (same on
# pipecat 0.0.97), so send enough for output to appear.
N_PCM_FRAMES = 10


class FakeURL:
    query = "device_id=office"


class FakeWebSocket:
    """Minimal starlette WebSocket stand-in for the FastAPI transport."""

    url = FakeURL()
    client = None

    def __init__(self, inbound):
        self._inbound = list(inbound)
        self.sent = []
        self.client_state = WebSocketState.CONNECTED
        self.application_state = WebSocketState.CONNECTED
        self.headers = {}
        self.closed = False

    async def accept(self):
        return None

    async def receive(self):
        if not self._inbound:
            # Hold the socket open a moment so the pipeline has started before
            # the disconnect ends it, like a real device would.
            await asyncio.sleep(0.2)
            return {"type": "websocket.disconnect"}
        await asyncio.sleep(0.02)
        return self._inbound.pop(0)

    async def send_bytes(self, data):
        self.sent.append(("bytes", data))

    async def send_text(self, data):
        self.sent.append(("text", data))

    async def close(self, *_args, **_kwargs):
        self.closed = True
        self.client_state = WebSocketState.DISCONNECTED
        self.application_state = WebSocketState.DISCONNECTED


def configure(app: Application) -> None:
    """Mirror Application.initialize() for the fields create_openai_service reads."""
    app.openai_api_key = "sk-test-not-a-real-key"
    app.vad_threshold = 0.5
    app.vad_prefix_padding_ms = 300
    app.vad_silence_duration_ms = 800
    app.turn_detection_type = "semantic_vad"
    app.vad_eagerness = "low"
    app.interrupt_response = False
    app.semantic_vad_create_response = True
    app.enable_disconnect_tool = False
    app.transcription_language = "en"
    app.transcription_model = "gpt-4o-transcribe"
    app.instructions = "You are the test assistant."
    app.model = "gpt-realtime-2"
    app.voice = "marin"
    app.openai_speed = 1.0
    app.max_output_tokens = None
    app.noise_reduction = ""
    app.mcp_tool_allowlist = []
    app.mcp_client = None
    app.mcp_service = None
    app.enable_web_search = True
    app.web_search_model = "gpt-5.5"
    app.session_manager = SessionManager(reuse_timeout=300, max_restored_messages=12)
    app.timer_registry = TimerRegistry()
    app.enrollment_conductor = None
    app.speaker_male_name = ""
    app.speaker_female_name = ""
    app.male_only_tools = set()


class TestPipelineSmoke(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        os.environ.pop("OPENCLAW_URL", None)
        os.environ.pop("PIPECAT_ALLOWED_ORIGINS", None)
        self.app = Application()
        configure(self.app)
        self.handler = WebSocketHandler(
            session_manager=self.app.session_manager, follow_up_ms=8000, output_lead_buffer_ms=400
        )
        self.app.websocket_handler = self.handler
        # No network: the realtime service connects in setup(); make that a no-op.
        self._connect_patch = patch.object(SafeRealtimeLLMService, "_connect", AsyncMock())
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

    async def test_service_settings_and_session_update_payload(self):
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service

        # Model is a connection-level parameter: it must be OUR model in the URL,
        # not pipecat 1.x's new default (gpt-realtime-2.1).
        self.assertTrue(service.base_url.endswith("?model=gpt-realtime-2"), service.base_url)
        self.assertEqual(service._settings.model, "gpt-realtime-2")

        # Capture what session.update would put on the wire.
        sent = []

        async def capture(payload):
            sent.append(payload)

        service._ws_send = capture
        await service._send_session_update()
        self.assertEqual(len(sent), 1)
        payload = sent[0]
        self.assertEqual(payload["type"], "session.update")
        session = payload["session"]
        self.assertEqual(session["instructions"], "You are the test assistant.")
        self.assertEqual(session["audio"]["output"], {"voice": "marin", "speed": 1.0})
        turn = session["audio"]["input"]["turn_detection"]
        self.assertEqual(turn["type"], "semantic_vad")
        self.assertEqual(turn["eagerness"], "low")
        self.assertIs(turn["create_response"], True)
        self.assertIs(turn["interrupt_response"], False)
        # (The GPT-transcribe `languages` rewrite in realtime_payload.py keys
        # on the legacy top-level input_audio_transcription field, which this
        # GA-shaped payload does not have — unchanged from 0.0.97.)
        transcription = session["audio"]["input"]["transcription"]
        self.assertEqual(transcription["model"], "gpt-4o-transcribe")
        self.assertEqual(transcription["language"], "en")
        # No pipecat-1.x-only fields leaked in for the classic model.
        self.assertNotIn("reasoning", session)
        # Tools reach the wire as provider-native dicts, unchanged.
        names = [t["name"] for t in session["tools"]]
        self.assertIn("web_search", names)
        self.assertIn("voice_enrollment", names)
        self.assertIn("mark_false_wake", names)
        self.assertIn("set_timer", names)
        self.assertIn("remember", names)
        self.assertNotIn("disconnect_client", names)
        for tool in session["tools"]:
            self.assertEqual(tool["type"], "function")
            self.assertEqual(set(tool), {"type", "name", "description", "parameters"})

        # Every registered handler keeps cancel_on_interruption=False.
        self.assertTrue(service._functions)
        for name, item in service._functions.items():
            self.assertFalse(item.cancel_on_interruption, name)
        for name in ("web_search", "set_timer", "remember", "voice_enrollment"):
            self.assertTrue(service.has_function(name), name)

        # Pre-seeded context: no spontaneous greeting on connect.
        self.assertIsNotNone(service._context)
        self.assertFalse(service._llm_needs_conversation_setup)

    async def test_speech_events_emit_turn_frames_like_0_0_97(self):
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        service.push_frame = AsyncMock()
        service.broadcast_interruption = AsyncMock()
        service.start_ttfb_metrics = AsyncMock()
        service.start_processing_metrics = AsyncMock()

        await service._handle_evt_speech_started(object())
        service.broadcast_interruption.assert_awaited_once()
        self.assertIsInstance(service.push_frame.await_args.args[0], UserStartedSpeakingFrame)

        await service._handle_evt_speech_stopped(object())
        self.assertIsInstance(service.push_frame.await_args.args[0], UserStoppedSpeakingFrame)
        # No pipecat-1.x proposal frames: the aggregator must not get a say.
        for call in service.push_frame.await_args_list:
            self.assertNotIn("Proposed", type(call.args[0]).__name__)

    async def test_pipeline_topology_and_run(self):
        """Serve one fake device end to end through WebSocketHandler.serve_connection."""
        inbound = [
            {"type": "websocket.receive", "text": json.dumps({"type": "start"})},
            {"type": "websocket.receive", "text": json.dumps({"type": "wake"})},
        ] + [{"type": "websocket.receive", "bytes": PCM_20MS_16K} for _ in range(N_PCM_FRAMES)] + [
            {"type": "websocket.receive", "text": json.dumps({"type": "ping"})},
        ]
        websocket = FakeWebSocket(inbound)

        # Capture the client events the pipeline would send to OpenAI, and
        # keep handles on what serve_connection builds.
        events = []
        built = {}

        async def capture(payload):
            events.append(payload)

        async def factory(connection):
            service = await self.app.create_openai_service(connection)
            service._ws_send = capture
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

        # Production entry point: accept, build, run until the device goes
        # away (the transport's on_client_disconnected cancels the worker —
        # same mechanism as on pipecat 0.0.97), then tear everything down.
        def on_client_disconnected(connection):
            # What Application.build_web_app wires in production.
            self.app.session_manager.handle_client_disconnect(
                connection.device_id, connection.openai_service
            )

        await asyncio.wait_for(
            self.handler.serve_connection(websocket, on_client_disconnected=on_client_disconnected),
            timeout=15,
        )

        pipeline, worker = built["pipeline"], built["worker"]
        # Topology: exactly the 0.0.97 processor chain, no RTVIProcessor.
        # (pipecat wraps the chain in its own PipelineSource/PipelineSink.)
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
                "SafeRealtimeLLMService",
                "TranscriptLogger",
                "RealtimeAssistantAggregator",
                "SessionActivityTracker",
                "PhaseEmitter",
                "OutputLeadBuffer",
                "FastAPIWebsocketOutputTransport",
            ],
        )

        # The device got its hello and a pong for its ping (both TEXT frames).
        texts = [json.loads(data) for kind, data in websocket.sent if kind == "text"]
        self.assertEqual(texts[0]["type"], "hello")
        self.assertEqual(texts[0]["follow_up_ms"], 8000)
        self.assertIn({"type": "pong"}, texts)
        # The connect-time input clear went out, and the 16 kHz PCM frames were
        # resampled to 24 kHz and forwarded as audio appends.
        types = [e["type"] for e in events]
        self.assertIn("input_audio_buffer.clear", types)
        appends = [e for e in events if e["type"] == "input_audio_buffer.append"]
        self.assertTrue(appends, types)
        total = sum(len(base64.b64decode(e["audio"])) for e in appends)
        # N x 20 ms at 24 kHz mono PCM16 = N x 960 bytes; the streaming
        # resampler holds the tail back until more audio arrives.
        self.assertGreater(total, N_PCM_FRAMES * 960 // 2, total)
        self.assertLessEqual(total, N_PCM_FRAMES * 960, total)
        # No reply audio reached the device (no OpenAI). The only binary the
        # output transport may write is pipecat's end-of-pipeline silence tail
        # (audio_out_end_silence_secs, unchanged from 0.0.97).
        for kind, data in websocket.sent:
            if kind == "bytes":
                self.assertEqual(data.strip(b"\x00"), b"", "non-silence audio reached the device")
        # Clean teardown: worker finished, device deregistered, per-connection
        # background tasks released, context cached for session reuse.
        self.assertTrue(worker.has_finished())
        self.assertEqual(self.handler.devices.ids(), [])
        connection = built["connection"]
        self.assertIsNone(connection.recovery)
        self.assertIsNone(connection.phase_emitter)
        self.assertIsNone(connection.openai_service)
        self.assertNotIn("office", self.app.session_manager.current_services)
        self.assertIn("office", self.app.session_manager.context_caches)


if __name__ == "__main__":
    unittest.main()
