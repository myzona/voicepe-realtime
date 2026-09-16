"""GPT-Live-1 path smoke test — no network.

Companion to test_pipeline_smoke.py (the gpt-realtime-2 baseline). With
`openai_model: gpt-live-1` selected, Application.create_openai_service builds a
SafeLiveLLMService (pipecat OpenAILiveLLMService + ResponsesDelegation). Only the
OpenAI WebSocket connect is stubbed; everything else is the production path.

Pinned here:

  * the `session.start` payload: model gpt-live-1, delegation.type responses,
    delegation.responses.model = live_backend_model, backend instructions and
    the SAME provider-native tool list the Realtime path sends, session
    instructions = persona (+ memory notes, no pipecat async-tool boilerplate),
    audio.output.voice;
  * every tool handler registered with cancel_on_interruption=False;
  * turn frames are service-driven: a user transcript turn opens/closes as ONE
    UserStartedSpeakingFrame / ONE UserStoppedSpeakingFrame downstream, no
    ProposedUser* broadcast, no interruption broadcast;
  * the output audio gate drops inter-utterance silence (after a hangover);
  * voice validation (Realtime-only names → marin; Live names pass);
  * ConnectionRecovery reacts to the Live reader/session-death messages;
  * pipeline topology (no RTVIProcessor) and an end-to-end run on a fake device:
    session.start is the first client event, mic PCM flows as
    session.input_audio.append once the session has started;
  * selecting gpt-realtime-2 still yields the phase-1 session.update payload.
"""
import asyncio
import base64
import json
import os
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipecat.frames.frames import (
    ErrorFrame,
    SpeechOutputAudioRawFrame,
    TranscriptionFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.frame_processor import FrameDirection
from pipecat.services.openai.live import events as live_events

from app.device_registry import DeviceConnection
from app.live_service import (
    BACKEND_PREAMBLE,
    OUTPUT_SILENCE_HANGOVER_S,
    SafeLiveLLMService,
    is_live_model,
    resolve_live_voice,
)
from app.main import Application, SafeRealtimeLLMService
from app.phase_emitter import TurnLiveness
from app.raw_audio_serializer import RawAudioSerializer
from app.websocket_handler import ConnectionRecovery, WebSocketHandler
from tests.test_pipeline_smoke import PCM_20MS_16K, N_PCM_FRAMES, FakeWebSocket, configure

EXPECTED_TOOL_NAMES = [
    "web_search", "voice_enrollment", "mark_false_wake",
    "set_timer", "cancel_timer", "list_timers",
    "remember", "forget", "list_memories",
]


def configure_live(app: Application) -> None:
    configure(app)
    app.model = "gpt-live-1"
    app.voice = "marin"
    app.live_backend_model = "gpt-5.4-mini"


class TestLivePipelineSmoke(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        os.environ.pop("OPENCLAW_URL", None)
        os.environ.pop("PIPECAT_ALLOWED_ORIGINS", None)
        self.app = Application()
        configure_live(self.app)
        self.handler = WebSocketHandler(
            session_manager=self.app.session_manager, follow_up_ms=8000, output_lead_buffer_ms=400
        )
        self.app.websocket_handler = self.handler
        self._connect_patch = patch.object(SafeLiveLLMService, "_connect", AsyncMock())
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

    async def test_session_start_payload_and_tools(self):
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        self.assertIsInstance(service, SafeLiveLLMService)
        self.assertEqual(service.base_url, "wss://api.openai.com/v1/live/sessions")
        # No Realtime pre-seed: the session starts from the pipeline's context.
        self.assertIsNone(service._context)

        sent = []

        async def capture(payload):
            sent.append(payload)

        service._ws_send = capture
        service.set_bootstrap_context(LLMContext())
        # What process_frame(StartFrame) does in the pipeline.
        await service._handle_context(service._bootstrap_context)

        self.assertEqual(len(sent), 1)
        payload = sent[0]
        self.assertEqual(payload["type"], "session.start")
        session = payload["session"]
        self.assertEqual(session["model"], "gpt-live-1")
        # Persona + memory notes only — pipecat's ASYNC TOOLS guidance stays out.
        self.assertTrue(session["instructions"].startswith("You are the test assistant."))
        self.assertNotIn("ASYNC TOOLS", session["instructions"])
        self.assertEqual(session["audio"], {"output": {"voice": "marin"}})
        self.assertNotIn("input", session)  # empty history is omitted
        delegation = session["delegation"]
        self.assertEqual(delegation["type"], "responses")
        responses = delegation["responses"]
        self.assertEqual(responses["model"], "gpt-5.4-mini")
        self.assertTrue(responses["instructions"].startswith(BACKEND_PREAMBLE))
        self.assertIn("You are the test assistant.", responses["instructions"])
        self.assertNotIn("tool_choice", responses)
        # The flat `delegation.model` form is rejected by the API; never send it.
        self.assertNotIn("model", delegation)
        # Tools: same provider-native dicts as the Realtime path, unchanged.
        names = [t["name"] for t in responses["tools"]]
        self.assertEqual(names, EXPECTED_TOOL_NAMES)
        for tool in responses["tools"]:
            self.assertEqual(tool["type"], "function")
            self.assertEqual(set(tool), {"type", "name", "description", "parameters"})
            self.assertNotIn("strict", tool)

        # Every handler registered, all with cancel_on_interruption=False.
        self.assertEqual(sorted(service._functions), sorted(EXPECTED_TOOL_NAMES))
        for name, item in service._functions.items():
            self.assertFalse(item.cancel_on_interruption, name)

        # A second context frame must not restart the session.
        await service._handle_context(LLMContext())
        self.assertEqual(len(sent), 1)

    async def test_session_start_seeds_cached_history(self):
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        sent = []

        async def capture(payload):
            sent.append(payload)

        service._ws_send = capture
        context = LLMContext(messages=[
            {"role": "user", "content": "turn the lamp on"},
            {"role": "assistant", "content": "The lamp is on."},
        ])
        service.set_bootstrap_context(context)
        await service._handle_context(context)
        history = sent[0]["session"]["input"]
        self.assertEqual([m["role"] for m in history], ["user", "assistant"])
        self.assertEqual(history[0]["content"], [{"type": "input_text", "text": "turn the lamp on"}])
        self.assertEqual(history[1]["content"], [{"type": "output_text", "text": "The lamp is on."}])

    async def test_user_turn_frames_are_service_driven(self):
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        pushed = []

        async def push_frame(frame, direction=FrameDirection.DOWNSTREAM):
            pushed.append((frame, direction))

        service.push_frame = push_frame
        service.broadcast_frame = AsyncMock()
        service.broadcast_interruption = AsyncMock()
        # The 0.8 s gap timer needs the pipeline's task manager; the turn is
        # closed explicitly below instead.
        service._restart_turn_timer = AsyncMock()

        delta = live_events.TranscriptDeltaEvent(
            type="session.input_transcript.delta", delta="turn on", start_ms=0, end_ms=200
        )
        await service._handle_evt_transcript_delta(delta)
        delta2 = live_events.TranscriptDeltaEvent(
            type="session.input_transcript.delta", delta=" the lamp", start_ms=200, end_ms=400
        )
        await service._handle_evt_transcript_delta(delta2)
        # Exactly one UserStartedSpeakingFrame downstream for the whole turn.
        starts = [f for f, d in pushed if isinstance(f, UserStartedSpeakingFrame)]
        self.assertEqual(len(starts), 1)
        self.assertEqual([d for f, d in pushed if isinstance(f, UserStartedSpeakingFrame)],
                         [FrameDirection.DOWNSTREAM])
        self.assertTrue(service._user_turn.open)

        # Turn end (normally the 0.8 s gap timer): transcript upstream + one stop.
        await service._close_turn("user")
        self.assertFalse(service._user_turn.open)
        transcripts = [(f, d) for f, d in pushed if isinstance(f, TranscriptionFrame)]
        self.assertEqual(len(transcripts), 1)
        self.assertEqual(transcripts[0][0].text, "turn on the lamp")
        self.assertEqual(transcripts[0][1], FrameDirection.UPSTREAM)
        stops = [f for f, d in pushed if isinstance(f, UserStoppedSpeakingFrame)]
        self.assertEqual(len(stops), 1)
        # Nothing for the aggregator-side UserTurnController, no interruption.
        service.broadcast_frame.assert_not_awaited()
        service.broadcast_interruption.assert_not_awaited()
        for frame, _ in pushed:
            self.assertNotIn("Proposed", type(frame).__name__)

    async def test_output_audio_gate_drops_inter_utterance_silence(self):
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        pushed = []

        async def push_frame(frame, direction=FrameDirection.DOWNSTREAM):
            pushed.append(frame)

        service.push_frame = push_frame
        silence = base64.b64encode(b"\x00\x00" * 480).decode()
        speech = base64.b64encode(b"\x00\x10" * 480).decode()

        def evt(delta):
            return live_events.OutputAudioDeltaEvent(type="session.output_audio.delta", delta=delta)

        # Idle stream: silence is dropped.
        await service._handle_evt_audio_delta(evt(silence))
        self.assertEqual(pushed, [])
        # Speech passes, and silence within the hangover passes (lets the
        # transport's 0.35 s bot-stopped detector fire).
        await service._handle_evt_audio_delta(evt(speech))
        await service._handle_evt_audio_delta(evt(silence))
        self.assertEqual(len(pushed), 2)
        self.assertTrue(all(isinstance(f, SpeechOutputAudioRawFrame) for f in pushed))
        self.assertEqual(pushed[0].sample_rate, 24000)
        # After the hangover, silence is dropped again.
        service._last_speech_mono = time.monotonic() - OUTPUT_SILENCE_HANGOVER_S - 1
        await service._handle_evt_audio_delta(evt(silence))
        self.assertEqual(len(pushed), 2)
        # Device "stop": this utterance is muted until the next turn boundary.
        service._session_started = True
        sent = []

        async def capture(payload):
            sent.append(payload)

        service._ws_send = capture
        await service.handle_device_interrupt()
        await service._handle_evt_audio_delta(evt(speech))
        self.assertEqual(len(pushed), 2)
        self.assertEqual(sent[0]["type"], "session.instructions.append")
        self.assertIsNone(sent[0]["delegation_id"])
        await service._open_turn("user")  # next user turn lifts the mute
        await service._handle_evt_audio_delta(evt(speech))
        self.assertEqual(len(pushed), 4)

    def test_voice_and_model_resolution(self):
        self.assertTrue(is_live_model("gpt-live-1"))
        self.assertTrue(is_live_model("gpt-live-1.5"))
        self.assertFalse(is_live_model("gpt-realtime-2"))
        self.assertFalse(is_live_model("gpt-realtime-mini"))
        self.assertEqual(resolve_live_voice("marin"), "marin")
        self.assertEqual(resolve_live_voice("Cedar"), "cedar")
        self.assertEqual(resolve_live_voice("vesper"), "vesper")
        self.assertEqual(resolve_live_voice(""), "marin")
        for realtime_only in ("alloy", "ash", "ballad", "coral", "echo", "sage", "shimmer", "verse"):
            self.assertEqual(resolve_live_voice(realtime_only), "marin", realtime_only)
        # Custom/unknown names pass through (expert escape hatch; API error is loud).
        self.assertEqual(resolve_live_voice("brandnew"), "brandnew")

    async def test_connection_recovery_reacts_to_live_death_messages(self):
        class FakeService:
            def __init__(self):
                self.resets = 0
                self.session_expires_at = None

            def is_busy(self):
                return False

            async def reset_conversation(self):
                self.resets += 1

        for message in (
            "live receive loop ended — connection closed",
            "live receive loop died: ConnectionClosedError(...)",
            "live session closed by server (reason=expired) — session_expired",
        ):
            service = FakeService()
            recovery = ConnectionRecovery(service)
            recovery.push_frame = AsyncMock()
            await recovery.process_frame(ErrorFrame(message), FrameDirection.UPSTREAM)
            self.assertIsNotNone(recovery._recover_task, message)
            await recovery._recover_task
            self.assertEqual(service.resets, 1, message)
            await recovery.close()

        # A tool failure is NOT a reconnect trigger.
        service = FakeService()
        recovery = ConnectionRecovery(service)
        recovery.push_frame = AsyncMock()
        await recovery.process_frame(ErrorFrame("Delegated response failed: boom"), FrameDirection.UPSTREAM)
        self.assertIsNone(recovery._recover_task)
        await recovery.close()

    async def test_pipeline_topology_and_run(self):
        inbound = [
            {"type": "websocket.receive", "text": json.dumps({"type": "start"})},
            {"type": "websocket.receive", "text": json.dumps({"type": "wake"})},
        ] + [{"type": "websocket.receive", "bytes": PCM_20MS_16K} for _ in range(N_PCM_FRAMES)] + [
            {"type": "websocket.receive", "text": json.dumps({"type": "ping"})},
        ]
        websocket = FakeWebSocket(inbound)
        events = []
        built = {}

        async def factory(connection):
            service = await self.app.create_openai_service(connection)

            async def capture(payload):
                events.append(payload)
                if payload["type"] == "session.start":
                    # The server's answer, so audio starts flowing.
                    await service._handle_server_event(live_events.parse_server_event(json.dumps({
                        "type": "session.started",
                        "session": {"id": "sess_test", "expires_at": int(time.time()) + 3600,
                                    "model": "gpt-live-1",
                                    "audio": {"output": {"voice": "marin"}}},
                    })))

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

        def on_client_disconnected(connection):
            self.app.session_manager.handle_client_disconnect(
                connection.device_id, connection.openai_service
            )

        await asyncio.wait_for(
            self.handler.serve_connection(websocket, on_client_disconnected=on_client_disconnected),
            timeout=15,
        )

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
                "SafeLiveLLMService",
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

        types = [e["type"] for e in events]
        # session.start is the first message on the socket; Realtime-only
        # events never appear.
        self.assertEqual(types[0], "session.start")
        self.assertNotIn("input_audio_buffer.clear", types)
        self.assertNotIn("session.update", types)
        self.assertEqual(service.session_id, "sess_test")
        self.assertIsNotNone(service.session_expires_at)
        appends = [e for e in events if e["type"] == "session.input_audio.append"]
        self.assertTrue(appends, types)
        total = sum(len(base64.b64decode(e["audio"])) for e in appends)
        self.assertGreater(total, N_PCM_FRAMES * 960 // 2, total)
        self.assertLessEqual(total, N_PCM_FRAMES * 960, total)
        for kind, data in websocket.sent:
            if kind == "bytes":
                self.assertEqual(data.strip(b"\x00"), b"", "non-silence audio reached the device")
        self.assertTrue(worker.has_finished())
        self.assertEqual(self.handler.devices.ids(), [])
        connection = built["connection"]
        self.assertIsNone(connection.recovery)
        self.assertIsNone(connection.openai_service)
        self.assertIn("office", self.app.session_manager.context_caches)

    async def test_realtime_selection_still_produces_phase1_payload(self):
        """gpt-realtime-2 is untouched by the Live addition."""
        self._connect_patch.stop()
        realtime_patch = patch.object(SafeRealtimeLLMService, "_connect", AsyncMock())
        realtime_patch.start()
        try:
            configure(self.app)  # model gpt-realtime-2
            connection = await self._make_connection(FakeWebSocket([]))
            service = connection.openai_service
            self.assertIsInstance(service, SafeRealtimeLLMService)
            sent = []

            async def capture(payload):
                sent.append(payload)

            service._ws_send = capture
            await service._send_session_update()
            payload = sent[0]
            self.assertEqual(payload["type"], "session.update")
            session = payload["session"]
            self.assertEqual(set(session), {"type", "model", "instructions", "audio", "tools"})
            self.assertEqual(session["type"], "realtime")
            self.assertEqual(session["model"], "gpt-realtime-2")
            self.assertEqual(session["instructions"], "You are the test assistant.")
            self.assertEqual(session["audio"], {
                "input": {
                    "turn_detection": {
                        "type": "semantic_vad", "eagerness": "low",
                        "create_response": True, "interrupt_response": False,
                    },
                    "transcription": {"model": "gpt-4o-transcribe", "language": "en"},
                },
                "output": {"voice": "marin", "speed": 1.0},
            })
            self.assertEqual([t["name"] for t in session["tools"]], EXPECTED_TOOL_NAMES)
            self.assertNotIn("delegation", session)
            self.assertIsNotNone(service._context)  # Realtime pre-seed still applies
        finally:
            realtime_patch.stop()
            self._connect_patch.start()


if __name__ == "__main__":
    unittest.main()
