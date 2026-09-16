"""Two devices must stay connected at once.

This is the acceptance test for the multi-device work. It runs the real
WebSocketHandler.serve_connection over a real uvicorn server with two real
WebSocket clients, stubbing only the OpenAI service and the pipeline runner —
the connection lifecycle, device registry, framing and routing are genuine.

Under the old single-client WebsocketServerTransport the second connect closed
the first socket, so the `both connected` and `still alive` assertions below
are precisely what used to fail.
"""
import asyncio
import contextlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import websockets
import uvicorn

from app.main import Application
from app.websocket_handler import WebSocketHandler

PORT = 18770


class FakeOpenAIService:
    """Stands in for SafeRealtimeLLMService; records which device made it."""

    _instances = []

    def __init__(self, device_id):
        self.device_id = device_id
        FakeOpenAIService._instances.append(self)
        self.disconnected = False

    def event_handler(self, _name):
        def decorator(fn):
            return fn
        return decorator

    async def send_client_event(self, _evt):
        return None

    async def disconnect(self):
        self.disconnected = True


async def build_server():
    handler = WebSocketHandler(host="127.0.0.1", port=PORT, follow_up_ms=1234)

    async def factory(connection):
        return FakeOpenAIService(connection.device_id)

    handler.openai_service_factory = factory

    # Replace the pipeline with a stub. The stub still runs the transport's
    # real read loop — pull frames off the mixed client and hand them to the
    # serializer, exactly as transport.input() does — so control frames and
    # the binary/text multiplexing are genuinely exercised. Only the OpenAI
    # audio processing is skipped.
    def fake_build(connection, activity_callback=None):
        class Runner:
            # pipecat 1.x WorkerRunner shape: add_workers() then run().
            async def add_workers(self, *_workers):
                return None

            async def run(self):
                async for message in connection.transport.client.receive():
                    await connection.serializer.deserialize(message)

        class Task:
            async def cancel(self):
                return None

        return object(), Runner(), Task()

    handler.build_pipeline = fake_build

    app = Application()
    app.websocket_handler = handler
    app.session_manager = None
    web_app = app.build_web_app()

    config = uvicorn.Config(web_app, host="127.0.0.1", port=PORT, log_level="error", lifespan="off")
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None
    task = asyncio.create_task(server.serve())
    for _ in range(100):
        if server.started:
            break
        await asyncio.sleep(0.05)
    return handler, server, task


async def main():
    handler, server, server_task = await build_server()
    try:
        url = f"ws://127.0.0.1:{PORT}/?device_id="
        async with websockets.connect(url + "kitchen") as a:
            hello_a = json.loads(await asyncio.wait_for(a.recv(), 5))
            assert hello_a["type"] == "hello", hello_a
            assert hello_a["follow_up_ms"] == 1234
            print(f"kitchen  -> hello received: follow_up_ms={hello_a['follow_up_ms']}")

            async with websockets.connect(url + "office") as b:
                hello_b = json.loads(await asyncio.wait_for(b.recv(), 5))
                assert hello_b["type"] == "hello"
                print("office   -> hello received while kitchen is still connected")

                # THE bug: with the old transport, opening `b` closed `a`.
                await asyncio.sleep(0.4)
                assert handler.devices.ids() == ["kitchen", "office"], handler.devices.ids()
                print(f"registry -> both connected: {handler.devices.ids()}")

                # The first socket must still be alive and usable.
                await a.send(json.dumps({"type": "ping"}))
                pong = json.loads(await asyncio.wait_for(a.recv(), 5))
                assert pong == {"type": "pong"}, pong
                print("kitchen  -> still alive after office connected (ping/pong works)")

                await b.send(json.dumps({"type": "ping"}))
                assert json.loads(await asyncio.wait_for(b.recv(), 5)) == {"type": "pong"}
                print("office   -> ping/pong works")

                # Independent sessions, not one shared session.
                ids = sorted(s.device_id for s in FakeOpenAIService._instances)
                assert ids == ["kitchen", "office"], ids
                print(f"sessions -> one OpenAI session per device: {ids}")

                # Phases must be unicast, not broadcast.
                await handler.devices.get("kitchen").send_phase("listening")
                msg = json.loads(await asyncio.wait_for(a.recv(), 5))
                assert msg == {"type": "phase", "value": "listening"}, msg
                with contextlib.suppress(asyncio.TimeoutError):
                    leaked = await asyncio.wait_for(b.recv(), 0.5)
                    raise AssertionError(f"phase leaked to office: {leaked}")
                print("phases   -> kitchen's phase did NOT reach office")

                # Targeting: explicit id wins; unknown id refuses to guess.
                assert handler.resolve_device("office").device_id == "office"
                assert handler.resolve_device("bedroom") is None
                print("targeting-> explicit id resolves; unknown id returns None")

            await asyncio.sleep(0.5)
            assert handler.devices.ids() == ["kitchen"], handler.devices.ids()
            print(f"cleanup  -> office removed on disconnect: {handler.devices.ids()}")

        # Unflashed firmware sends no device_id and must still work: the id
        # falls back to the client IP, which is distinct per device, so two
        # legacy devices still get separate identities and never evict each
        # other. This is the configuration a stock device actually runs.
        async with websockets.connect(f"ws://127.0.0.1:{PORT}/") as legacy:
            assert json.loads(await asyncio.wait_for(legacy.recv(), 5))["type"] == "hello"
            await asyncio.sleep(0.3)
            ids = handler.devices.ids()
            assert ids == ["127.0.0.1"], ids
            print(f"legacy   -> no device_id falls back to client IP: {ids}")
            await legacy.send(json.dumps({"type": "ping"}))
            assert json.loads(await asyncio.wait_for(legacy.recv(), 5)) == {"type": "pong"}
            print("legacy   -> ping/pong works without a device_id")
        await asyncio.sleep(0.5)

        async with websockets.connect(
            f"ws://127.0.0.1:{PORT}/voice/pe?device_id=hall"
        ) as non_root:
            assert json.loads(await asyncio.wait_for(non_root.recv(), 5))["type"] == "hello"
            await asyncio.sleep(0.3)
            assert handler.devices.ids() == ["hall"], handler.devices.ids()
            print("routing  -> non-root websocket paths stay accepted")
        await asyncio.sleep(0.5)

        await asyncio.sleep(0.5)
        assert handler.devices.ids() == [], handler.devices.ids()
        print("cleanup  -> kitchen removed on disconnect")
        assert all(s.disconnected for s in FakeOpenAIService._instances), "sessions leaked"
        print("cleanup  -> both OpenAI sessions torn down")

        print("\nALL ASSERTIONS PASSED — two devices coexist")
    finally:
        server.should_exit = True
        with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError):
            await asyncio.wait_for(server_task, 5)


asyncio.run(main())
