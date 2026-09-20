"""Marker mixin shared by every full-duplex ("live") LLM service.

`websocket_handler.build_pipeline` and `Application._preseed_context` need to
tell a live-style service (GPT-Live-1, Gemini Live) apart from a cascade/
turn-based one (gpt-realtime-*) to decide: start from the aggregator's context
instead of a `ContextInitializer` replay, route device "stop"/"flush" through
the service's own hooks instead of Realtime's input-buffer events, and skip
the empty-context pre-seed (the live service seeds itself). Keying this off
one shared marker — instead of a growing tuple of isinstance checks, or a
concrete-class check that only knows about OpenAI's Live service — means a new
live provider (this file exists because of Gemini) only has to inherit
`LiveModeService`; nothing else in the shared pipeline code changes.
"""


class LiveModeService:
    """Empty marker mixin. See module docstring."""
