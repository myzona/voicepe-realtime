import asyncio
import unittest
from unittest.mock import AsyncMock

from pipecat.frames.frames import (
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    OutputAudioRawFrame,
    InterruptionFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.processors.frame_processor import FrameProcessor

from app.output_lead_buffer import OutputLeadBuffer


def audio_frame(milliseconds: int = 5) -> OutputAudioRawFrame:
    samples = 24 * milliseconds  # 24 kHz mono PCM16
    return OutputAudioRawFrame(
        audio=b"\0" * samples * 2,
        sample_rate=24000,
        num_channels=1,
    )


class TestOutputLeadBuffer(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Unit-test this processor's state machine without requiring a complete
        # Pipecat PipelineTask/TaskManager around the base processor.
        self.base_process = unittest.mock.patch.object(
            FrameProcessor, "process_frame", new=AsyncMock()
        )
        self.base_process.start()
        self.buffer = OutputLeadBuffer(lead_ms=10, max_hold_ms=30)
        self.buffer.push_frame = AsyncMock()

    async def asyncTearDown(self):
        self.buffer._cancel_cap_timer()
        self.base_process.stop()

    async def test_cold_reply_is_held_then_released_in_order(self):
        first, second = audio_frame(), audio_frame()
        await self.buffer.process_frame(first, FrameDirection.DOWNSTREAM)
        self.buffer.push_frame.assert_not_awaited()

        await self.buffer.process_frame(second, FrameDirection.DOWNSTREAM)
        self.assertEqual(
            [call.args[0] for call in self.buffer.push_frame.await_args_list],
            [first, second],
        )

    async def test_interruption_drops_held_audio(self):
        held = audio_frame()
        await self.buffer.process_frame(held, FrameDirection.DOWNSTREAM)
        interrupt = InterruptionFrame()
        await self.buffer.process_frame(interrupt, FrameDirection.DOWNSTREAM)

        self.assertEqual(
            [call.args[0] for call in self.buffer.push_frame.await_args_list],
            [interrupt],
        )

    async def test_short_reply_flushes_on_bot_stop(self):
        held = audio_frame()
        await self.buffer.process_frame(held, FrameDirection.DOWNSTREAM)
        stopped = BotStoppedSpeakingFrame()
        await self.buffer.process_frame(stopped, FrameDirection.DOWNSTREAM)

        self.assertEqual(
            [call.args[0] for call in self.buffer.push_frame.await_args_list],
            [held, stopped],
        )

    async def test_mid_segment_audio_is_not_rebuffered(self):
        self.buffer = OutputLeadBuffer(lead_ms=10, idle_gap_ms=0, max_hold_ms=30)
        self.buffer.push_frame = AsyncMock()
        started = BotStartedSpeakingFrame()
        first, second, third = audio_frame(), audio_frame(), audio_frame()
        await self.buffer.process_frame(started, FrameDirection.DOWNSTREAM)
        await self.buffer.process_frame(first, FrameDirection.DOWNSTREAM)
        await self.buffer.process_frame(second, FrameDirection.DOWNSTREAM)
        await self.buffer.process_frame(third, FrameDirection.DOWNSTREAM)

        self.assertEqual(
            [call.args[0] for call in self.buffer.push_frame.await_args_list],
            [started, first, second, third],
        )

    async def test_stall_watchdog_releases_held_audio(self):
        held = audio_frame()
        await self.buffer.process_frame(held, FrameDirection.DOWNSTREAM)
        await asyncio.sleep(0.06)
        self.buffer.push_frame.assert_awaited_once_with(
            held, FrameDirection.DOWNSTREAM
        )


if __name__ == "__main__":
    unittest.main()
