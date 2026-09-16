"""Per-connection FastAPI transport for binary audio and text control frames."""

import asyncio
import typing

from loguru import logger
from pipecat.frames.frames import EndFrame
from starlette.websockets import WebSocket, WebSocketState

from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketClient,
    FastAPIWebsocketInputTransport,
    FastAPIWebsocketOutputTransport,
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)


class MixedFastAPIWebsocketClient(FastAPIWebsocketClient):
    """A FastAPI WebSocket client that reads and writes both frame types.

    `receive()` yields whichever payload each frame actually carried, and
    `send()` dispatches on the Python type of the outgoing data rather than on
    a mode fixed at construction time.

    pipecat 1.x's stock client already carries both payload types (the
    `is_binary` mode is gone). This subclass is kept for two things the stock
    one still lacks: a send lock (audio and JSON control frames are written
    from different tasks) and a receive loop that treats starlette's
    post-disconnect RuntimeError as a clean end-of-stream.
    """

    def __init__(self, websocket: WebSocket, callbacks, ws_close_timeout: float | None = None):
        """Initialize the client.

        Args:
            websocket: The FastAPI/starlette WebSocket connection.
            callbacks: The transport's FastAPIWebsocketCallbacks.
            ws_close_timeout: How long disconnect() waits for the close
                handshake; None keeps pipecat's default.
        """
        if ws_close_timeout is None:
            super().__init__(websocket, callbacks)
        else:
            super().__init__(websocket, callbacks, ws_close_timeout=ws_close_timeout)
        # The pipeline's output transport writes audio bytes while the phase
        # emitter writes JSON text, from different tasks, to the same socket.
        # starlette does not serialize concurrent sends, so interleaved frames
        # could corrupt the stream; this lock makes each send atomic.
        self._send_lock = asyncio.Lock()

    def receive(self) -> typing.AsyncIterator[bytes | str]:
        """Yield every inbound frame, binary or text.

        Returns:
            An async iterator over raw payloads, ending on disconnect.
        """
        return self._iter_messages()

    async def _iter_messages(self) -> typing.AsyncIterator[bytes | str]:
        while True:
            try:
                message = await self._websocket.receive()
            except asyncio.CancelledError:
                raise
            except RuntimeError:
                # starlette raises RuntimeError if receive() is called once the
                # socket has already gone away. Treat it as end-of-stream so the
                # input transport runs its normal disconnect path.
                break

            if message.get("type") == "websocket.disconnect":
                break

            # A frame carries exactly one of these. Binary is checked first:
            # it is the audio path and by far the hotter of the two.
            payload = message.get("bytes")
            if payload is None:
                payload = message.get("text")
            if payload is not None:
                yield payload

    async def send(self, data: str | bytes):
        """Send one frame, choosing binary or text from the payload type.

        Args:
            data: Audio bytes or a JSON control string.
        """
        try:
            if self._can_send():
                async with self._send_lock:
                    if isinstance(data, (bytes, bytearray, memoryview)):
                        await self._websocket.send_bytes(data)
                    else:
                        await self._websocket.send_text(data)
        except Exception as e:
            logger.error(
                f"{self} exception sending data: {e.__class__.__name__} ({e}), "
                f"application_state: {self._websocket.application_state}"
            )
            # Mirrors the base class: a send failure on an already-dead socket
            # means the transport should start closing rather than retry.
            if (
                self._websocket.application_state == WebSocketState.DISCONNECTED
                and not self.is_closing
            ):
                logger.warning("Closing already disconnected websocket!")
                self._closing = True


class MixedFastAPIWebsocketInputTransport(FastAPIWebsocketInputTransport):
    """End the pipeline when this connection's receive loop ends."""

    async def _receive_messages(self):
        await super()._receive_messages()
        await self.push_frame(EndFrame())


class MixedFastAPIWebsocketTransport(FastAPIWebsocketTransport):
    """A per-connection transport whose client speaks both frame types.

    Construct one of these per connected device. Everything else — event
    handlers, `input()`, `output()` — behaves exactly like the pipecat base.
    """

    def __init__(
        self,
        websocket: WebSocket,
        params: FastAPIWebsocketParams,
        input_name: typing.Optional[str] = None,
        output_name: typing.Optional[str] = None,
    ):
        """Initialize the transport.

        Args:
            websocket: The FastAPI/starlette WebSocket connection.
            params: Transport configuration, including the serializer.
            input_name: Optional name for the input processor.
            output_name: Optional name for the output processor.
        """
        super().__init__(websocket, params, input_name=input_name, output_name=output_name)

        # The base constructor already built a stock client and handed it to
        # the input/output processors, so both have to be rebuilt around the
        # mixed client. The discarded objects hold no resources — their
        # constructors only assign fields — so this costs nothing at runtime.
        self._client = MixedFastAPIWebsocketClient(
            websocket, self._callbacks, ws_close_timeout=params.ws_close_timeout
        )
        self._input = MixedFastAPIWebsocketInputTransport(
            self, self._client, self._params, name=self._input_name
        )
        self._output = FastAPIWebsocketOutputTransport(
            self, self._client, self._params, name=self._output_name
        )

    @property
    def client(self) -> MixedFastAPIWebsocketClient:
        """The underlying WebSocket client, for out-of-band control frames."""
        return self._client
