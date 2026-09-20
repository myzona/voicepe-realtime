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
  * selecting gpt-realtime-2 still yields the phase-1 session.update payload;
  * phase 4: the backend gets the tool rules + reasoning effort, the live model
    the delegation/no-filler rules, web_search is the Responses built-in tool
    (with a one-shot fallback), the input clock feeds silence while the device
    mic is gated, a user fragment during the reply does not flip the phase,
    delegations without function calls are released, server events are logged.
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
    InputAudioRawFrame,
    SpeechOutputAudioRawFrame,
    TranscriptionFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.frame_processor import FrameDirection
from pipecat.services.openai.live.llm import OpenAILiveLLMService
from pipecat.services.openai.live import events as live_events

from app.device_registry import DeviceConnection
from app.live_service import (
    BACKEND_PREAMBLE,
    BACKEND_TOOL_RULES,
    HOSTED_WEB_SEARCH_TOOL,
    INPUT_CLOCK_GAP_S,
    INPUT_CLOCK_TAIL_S,
    LIVE_DELEGATION_RULES,
    OUTPUT_SILENCE_HANGOVER_S,
    USER_TURN_GAP_S,
    SafeLiveLLMService,
    backend_instructions,
    is_live_model,
    live_instructions,
    resolve_live_voice,
    resolve_reasoning_effort,
    resolve_text_verbosity,
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


# The backend's tool list when the built-in web_search replaces the function.
EXPECTED_BACKEND_TOOL_NAMES = [n for n in EXPECTED_TOOL_NAMES if n != "web_search"]


def configure_live(app: Application) -> None:
    """Mirror Application.initialize() defaults for the gpt-live-1 path."""
    configure(app)
    app.model = "gpt-live-1"
    app.voice = "marin"
    app.live_backend_model = "gpt-5.4-mini"
    app.live_backend_reasoning_effort = "low"
    app.live_backend_verbosity = None
    app.live_builtin_web_search = True


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
        # Persona + memory notes + the delegation/no-filler rules — pipecat's
        # ASYNC TOOLS guidance stays out.
        self.assertTrue(session["instructions"].startswith("You are the test assistant."))
        self.assertTrue(session["instructions"].endswith(LIVE_DELEGATION_RULES))
        self.assertIn("what time is it", session["instructions"])
        self.assertIn("do not hum", session["instructions"])
        self.assertNotIn("ASYNC TOOLS", session["instructions"])
        self.assertEqual(session["audio"], {"output": {"voice": "marin"}})
        self.assertNotIn("input", session)  # empty history is omitted
        delegation = session["delegation"]
        self.assertEqual(delegation["type"], "responses")
        responses = delegation["responses"]
        self.assertEqual(responses["model"], "gpt-5.4-mini")
        # Backend prompt: preamble, explicit tool rules (time → time tool,
        # never guess), then the persona.
        self.assertTrue(responses["instructions"].startswith(BACKEND_PREAMBLE))
        self.assertIn("ALWAYS call the date/time tool first", responses["instructions"])
        self.assertIn("never guess, estimate or compute the time", responses["instructions"])
        self.assertIn("You are the test assistant.", responses["instructions"])
        self.assertNotIn("tool_choice", responses)
        # Latency knobs: reasoning effort low by default, verbosity not sent.
        self.assertEqual(responses["reasoning"], {"effort": "low"})
        self.assertNotIn("text", responses)
        self.assertNotIn("max_output_tokens", responses)
        # The flat `delegation.model` form is rejected by the API; never send it.
        self.assertNotIn("model", delegation)
        # Tools: same provider-native dicts as the Realtime path, except that
        # web_search is the Responses built-in tool (runs in the backend).
        functions = [t for t in responses["tools"] if t["type"] == "function"]
        self.assertEqual([t["name"] for t in functions], EXPECTED_BACKEND_TOOL_NAMES)
        for tool in functions:
            self.assertEqual(set(tool), {"type", "name", "description", "parameters"})
            self.assertNotIn("strict", tool)
        hosted = [t for t in responses["tools"] if t["type"] != "function"]
        self.assertEqual(hosted, [HOSTED_WEB_SEARCH_TOOL])
        self.assertTrue(service.hosted_web_search_active())

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

        # Turn end (normally the gap timer): transcript upstream + one stop.
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
        # A mid-sentence pause must not split the question: the user gap is
        # 1.5 s (pipecat's 0.8 s produced thinking → listening → thinking).
        self.assertEqual(service._user_turn.gap_secs, USER_TURN_GAP_S)
        self.assertGreaterEqual(USER_TURN_GAP_S, 1.5)

    async def test_user_fragment_while_bot_speaking_does_not_flip_phase(self):
        """A late tail of the question during the reply is recorded, not announced."""
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        pushed = []

        async def push_frame(frame, direction=FrameDirection.DOWNSTREAM):
            pushed.append((frame, direction))

        service.push_frame = push_frame
        service._restart_turn_timer = AsyncMock()
        # Bot is speaking: assistant transcript turn open + fresh output audio.
        service._assistant_turn.open = True
        service._last_speech_mono = time.monotonic()
        service._output_muted_until = time.monotonic() + 5  # a device stop is in force
        delta = live_events.TranscriptDeltaEvent(
            type="session.input_transcript.delta", delta=", California", start_ms=0, end_ms=200
        )
        await service._handle_evt_transcript_delta(delta)
        self.assertTrue(service._user_turn.open)
        self.assertFalse(any(isinstance(f, UserStartedSpeakingFrame) for f, _ in pushed))
        # The stop-mute is NOT lifted by an unannounced fragment.
        self.assertGreater(service._output_muted_until, time.monotonic())
        await service._close_turn("user")
        # Transcript still reaches the context; no UserStoppedSpeaking → no
        # `thinking` flip either.
        self.assertEqual([f.text for f, _ in pushed if isinstance(f, TranscriptionFrame)], [", California"])
        self.assertFalse(any(isinstance(f, UserStoppedSpeakingFrame) for f, _ in pushed))

        # Once the bot is quiet, the next fragment is a real turn again.
        service._assistant_turn.open = False
        service._last_speech_mono = time.monotonic() - 5
        pushed.clear()
        await service._handle_evt_transcript_delta(live_events.TranscriptDeltaEvent(
            type="session.input_transcript.delta", delta="what time is it", start_ms=0, end_ms=200
        ))
        self.assertEqual(len([f for f, _ in pushed if isinstance(f, UserStartedSpeakingFrame)]), 1)
        self.assertEqual(service._output_muted_until, 0.0)
        await service._close_turn("user")
        self.assertEqual(len([f for f, _ in pushed if isinstance(f, UserStoppedSpeakingFrame)]), 1)

    async def test_input_clock_feeds_silence_while_mic_is_gated(self):
        """The Live model only runs while input audio arrives (root cause of the
        17 s post-tool stall): with the device mic gated during a reply, the
        service feeds real-time-paced silence for as long as the conversation
        is active, and nothing while the room is idle."""
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        sent = []

        async def capture(payload):
            sent.append(payload)

        service._ws_send = capture
        service._session_started = True
        now = time.monotonic()

        # Idle connection: no activity, nothing in flight → no silence (billing).
        service._last_activity_mono = now - INPUT_CLOCK_TAIL_S - 1
        service._last_input_audio_mono = now - 10
        self.assertEqual(await service._input_clock_tick(now, now - 0.1), 0)
        self.assertEqual(sent, [])

        # A tool is in flight and the device mic is gated: feed the elapsed time.
        service._open_function_calls["call_1"] = "item_1"
        fed = await service._input_clock_tick(now, now - 0.1)
        self.assertEqual(fed, 100)
        self.assertEqual(sent[-1]["type"], "session.input_audio.append")
        audio = base64.b64decode(sent[-1]["audio"])
        self.assertEqual(len(audio), 24000 * 2 * 100 // 1000)  # 100 ms of 24 kHz PCM16
        self.assertEqual(audio.strip(b"\x00"), b"")
        # Never more than one max chunk per tick, even after a long stall.
        self.assertEqual(await service._input_clock_tick(now, now - 3.0), 500)
        service._open_function_calls.clear()

        # A delegated result just landed (activity): keep the clock running so
        # the model can "hear" it and speak.
        service._last_activity_mono = now - 1
        self.assertEqual(await service._input_clock_tick(now, now - 0.1), 100)
        # Device audio flowing → the device IS the clock; nothing is fed.
        service._last_input_audio_mono = now - INPUT_CLOCK_GAP_S / 2
        self.assertEqual(await service._input_clock_tick(now, now - 0.1), 0)
        # Follow-up window closed (device flush): the tail ends at once.
        service._last_input_audio_mono = now - 1
        service._last_activity_mono = now - 1
        self.assertEqual(await service._input_clock_tick(now, now - 0.1), 100)
        service.note_device_mic_closed()
        self.assertEqual(await service._input_clock_tick(now, now - 0.1), 0)
        # …unless a function call is still running.
        service._open_function_calls["call_2"] = "item_2"
        self.assertEqual(await service._input_clock_tick(now, now - 0.1), 100)
        service._open_function_calls.clear()
        # Session not started / disconnecting → nothing.
        service._last_input_audio_mono = now - 10
        service._session_started = False
        self.assertEqual(await service._input_clock_tick(now, now - 0.1), 0)
        # Input frames stamp the clock source.
        service._session_started = True
        before = service._last_input_audio_mono
        # Patch the real base class's process_frame directly rather than by
        # __mro__ index — SafeLiveLLMService also inherits LiveModeService
        # (app/live_mode.py, a marker mixin the Gemini Live service shares),
        # which shifts the MRO index without changing which class this test
        # means to patch.
        with patch.object(OpenAILiveLLMService, "process_frame", AsyncMock()):
            await service.process_frame(
                InputAudioRawFrame(audio=b"\x00\x00" * 480, sample_rate=24000, num_channels=1),
                FrameDirection.DOWNSTREAM,
            )
        self.assertGreater(service._last_input_audio_mono, before)

    async def test_delegation_bookkeeping_and_event_log(self):
        """response.completed without function calls must not leave is_busy() stuck,
        and every server event type reaches the log at INFO."""
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        service._session_started = True
        service._ws_send = AsyncMock()

        def envelope(inner, delegation_id="item_1", **fields):
            return live_events.parse_server_event(json.dumps({
                "type": "response.event", "delegation_id": delegation_id,
                "event": {"type": inner, **fields},
            }))

        service.turn_liveness = TurnLiveness()
        with self.assertLogs("app.live_service", level="INFO") as logs:
            await service._handle_server_event(live_events.parse_server_event(json.dumps({
                "type": "session.delegation.created", "offset_ms": 1000,
                "delegation": {"id": "item_1", "type": "delegation", "target": "responses"},
            })))
            await service._handle_server_event(envelope("response.created"))
            self.assertTrue(service.is_busy())
            await service._handle_server_event(envelope("response.output_text.delta", delta="It"))
            await service._handle_server_event(envelope(
                "response.completed", response={"status": "completed", "usage": {"total_tokens": 12}}
            ))
            await service._handle_server_event(live_events.parse_server_event(json.dumps({
                "type": "session.commentary.appended", "start_ms": 1000, "end_ms": 1200,
            })))
            await service._handle_server_event(live_events.parse_server_event(json.dumps({
                "type": "session.something.new", "detail": 1,
            })))
        # No function call → the pending entry is released (pipecat leaks it).
        self.assertEqual(service._pending_responses, {})
        # Backend progress ticked the thinking watchdog's liveness signal.
        self.assertGreater(service.turn_liveness.last_activity, 0.0)
        service._last_speech_mono = 0.0
        self.assertFalse(service.is_busy())
        text = "\n".join(logs.output)
        self.assertIn("session.delegation.created id=item_1 target=responses", text)
        self.assertIn("response.event/response.created delegation=item_1", text)
        self.assertIn("response.event/response.completed delegation=item_1 status=completed tokens=12", text)
        self.assertNotIn("output_text.delta", text)  # per-token deltas stay at DEBUG
        self.assertIn("session.commentary.appended 1000-1200ms", text)
        self.assertIn("session.something.new (unmodelled)", text)

        # A response WITH a function call keeps the pipecat continuation path:
        # output → response.item.create + response.create, logged at INFO.
        service.run_function_calls = AsyncMock()
        await service._handle_server_event(envelope("response.created", delegation_id="item_2"))
        await service._handle_server_event(envelope(
            "response.output_item.done", delegation_id="item_2",
            item={"type": "function_call", "status": "completed", "call_id": "call_9",
                  "name": "web_search", "arguments": json.dumps({"query": "weather"})},
        ))
        self.assertIn("call_9", service._open_function_calls)
        await service._handle_server_event(envelope(
            "response.completed", delegation_id="item_2", response={"status": "completed"}
        ))
        self.assertTrue(service.is_busy())
        with self.assertLogs("app.live_service", level="INFO") as logs:
            await service._send_function_call_output("call_9", "64 degrees")
        types = [c.args[0]["type"] for c in service._ws_send.await_args_list]
        self.assertEqual(types[-2:], ["response.item.create", "response.create"])
        self.assertEqual(service._pending_responses, {})
        self.assertIn("response.create sent — backend continues delegation item_2", "\n".join(logs.output))

    async def test_hosted_web_search_falls_back_on_session_start_rejection(self):
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        self.assertTrue(service.hosted_web_search_active())
        restarted = []

        async def fake_reset():
            restarted.append(True)

        service.reset_conversation = fake_reset
        created = []

        def create_task(coro, name=None):
            task = asyncio.get_event_loop().create_task(coro)
            created.append(task)
            return task

        service.create_task = create_task
        service.push_error = AsyncMock()
        await service._handle_server_event(live_events.parse_server_event(json.dumps({
            "type": "error",
            "error": {"type": "invalid_request_error", "code": "invalid_value",
                      "message": "Unsupported tool type", "param": "session.delegation.responses.tools"},
        })))
        await asyncio.gather(*created)
        self.assertEqual(restarted, [True])
        self.assertFalse(service.hosted_web_search_active())
        names = [t.get("name") for t in service.effective_tools()]
        self.assertIn("web_search", names)
        self.assertNotIn(HOSTED_WEB_SEARCH_TOOL, service.effective_tools())
        service.push_error.assert_not_awaited()
        # A second startup error is a real failure → pipecat's permanent error.
        await service._handle_server_event(live_events.parse_server_event(json.dumps({
            "type": "error", "error": {"type": "invalid_request_error", "message": "nope"},
        })))
        service.push_error.assert_awaited()

    async def test_live_options_reach_the_payload(self):
        """Verbosity + reasoning knobs and the function-tool web_search mode."""
        self.app.live_backend_reasoning_effort = "none"
        self.app.live_backend_verbosity = "low"
        self.app.live_builtin_web_search = False
        connection = await self._make_connection(FakeWebSocket([]))
        service = connection.openai_service
        sent = []

        async def capture(payload):
            sent.append(payload)

        service._ws_send = capture
        service.set_bootstrap_context(LLMContext())
        await service._handle_context(service._bootstrap_context)
        responses = sent[0]["session"]["delegation"]["responses"]
        self.assertEqual(responses["reasoning"], {"effort": "none"})
        self.assertEqual(responses["text"], {"verbosity": "low"})
        self.assertEqual([t["name"] for t in responses["tools"]], EXPECTED_TOOL_NAMES)
        self.assertTrue(all(t["type"] == "function" for t in responses["tools"]))
        self.assertFalse(service.hosted_web_search_active())
        # The time tool's real name lands in the backend rules when registered.
        self.assertIn("llm__GetDateTime", backend_instructions("persona", ["HassTurnOn", "llm__GetDateTime"]))
        self.assertTrue(backend_instructions("p", []).startswith(BACKEND_PREAMBLE + BACKEND_TOOL_RULES.format(time_tool="the date/time tool")))
        self.assertEqual(live_instructions("persona"), "persona\n\n" + LIVE_DELEGATION_RULES)
        self.assertEqual(resolve_reasoning_effort(""), None)
        self.assertEqual(resolve_reasoning_effort("Default"), None)
        self.assertEqual(resolve_reasoning_effort("MINIMAL"), "minimal")
        self.assertEqual(resolve_reasoning_effort("xhigh"), "xhigh")  # pass-through, warned
        self.assertEqual(resolve_text_verbosity(""), None)
        self.assertEqual(resolve_text_verbosity("Low"), "low")

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
        # The input clock ran inside the pipeline (task manager) and was
        # stopped with the connection; the device streamed the whole time, so
        # it fed nothing.
        self.assertIsNone(service._input_clock_task)
        self.assertEqual(service._input_clock_total_ms, 0)
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
