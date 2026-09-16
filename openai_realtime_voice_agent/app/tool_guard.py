"""Shared tool-handler guard for the OpenAI service classes.

Both `SafeRealtimeLLMService` (gpt-realtime-*) and `SafeLiveLLMService`
(gpt-live-1) force `cancel_on_interruption=False` on every tool registration
and wrap the handler with:

  * the speaker gate (fork): tools listed in `male_only_tools` only execute
    when the last voice-type verdict is "male". Enforced HERE — below the
    model — so prompt tricks can't bypass it. Fails closed on uncertain /
    stale / absent verdicts. Convenience gating on a voice-type heuristic,
    not biometric auth;
  * turn-liveness ticks around the run, so the PhaseEmitter's thinking
    watchdog knows a tool is in flight and a slow tool (web search: 10-20 s
    of pipeline silence) is never mistaken for a dead turn.

All our handlers use the single-param FunctionCallParams signature, so the
wrapper does too (pipecat inspects the signature to pick the calling
convention). The wrapper is a plain closure: it deliberately does NOT carry
pipecat's `_pipecat_cleanup` attribute that the MCPClient tool wrapper has —
that attribute makes the LLM service close the MCP connection when THIS
service is cleaned up, which is wrong here because the MCP client is shared
by every device's session (see mcp_service.py).

The service is expected to expose `speaker_probe`, `male_only_tools` and
`turn_liveness` attributes (set by Application.create_openai_service).
"""
import logging

logger = logging.getLogger(__name__)


def guarded_tool_handler(service, function_name: str, handler):
    """Wrap `handler` with the speaker gate + liveness ticks for `service`."""

    async def liveness_tracked(params):
        male_only = getattr(service, "male_only_tools", None)
        if male_only and function_name in male_only:
            probe = getattr(service, "speaker_probe", None)
            speaker = probe.gate_speaker() if probe else "unknown"
            if speaker != "male":
                owner = (probe.male_name if probe else "") or "the owner"
                logger.info(f"⛔ speaker gate blocked '{function_name}' (speaker={speaker})")
                await params.result_callback({
                    "error": (
                        f"Not available: this capability is reserved for {owner}, "
                        f"and the current speaker's voice was not recognized as {owner}. "
                        f"Relay this politely."
                    )
                })
                return
        liveness = getattr(service, "turn_liveness", None)
        if liveness is not None:
            liveness.tool_started()
        try:
            return await handler(params)
        finally:
            if liveness is not None:
                liveness.tool_finished()

    return liveness_tracked
