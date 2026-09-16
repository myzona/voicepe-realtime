"""Session management with context caching for OpenAI Realtime API."""
import logging
import time
from typing import Optional, Dict
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMAssistantAggregator,
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.services.openai.realtime.llm import OpenAIRealtimeLLMService
from pipecat.processors.frame_processor import FrameProcessor, FrameDirection
from pipecat.frames.frames import (
    Frame,
    StartFrame,
    LLMMessagesUpdateFrame,
    UserStartedSpeakingFrame,
)
from pipecat.turns.user_start.external_user_turn_start_strategy import (
    ExternalUserTurnStartStrategy,
)
from pipecat.turns.user_stop.external_user_turn_stop_strategy import (
    ExternalUserTurnStopStrategy,
)
from pipecat.turns.user_turn_strategies import UserTurnStrategies

logger = logging.getLogger(__name__)


def _inert_external_turn_strategies() -> UserTurnStrategies:
    """ExternalUserTurnStrategies(), pre-shaped the way realtime mode wants it.

    Equivalent to `ExternalUserTurnStrategies()` except the stop strategy is
    created with `wait_for_transcript=False` up front — the exact mutation the
    aggregator applies itself in realtime mode, which it otherwise logs as a
    WARNING on every connection because the strategies are "user-provided".
    """
    return UserTurnStrategies(
        start=[ExternalUserTurnStartStrategy(enable_interruptions=True)],
        stop=[ExternalUserTurnStopStrategy(wait_for_transcript=False)],
    )


class RealtimeAssistantAggregator(LLMAssistantAggregator):
    """Assistant aggregator that forwards tool results to OpenAI immediately.

    pipecat 0.0.97 pushed the context frame carrying a finished tool result
    upstream unconditionally; the realtime service then sent the
    function_call_output and created the follow-up response right away.
    pipecat 1.x gates that push: it is DROPPED while the user is speaking
    (`_user_speaking`, no re-trigger) and DEFERRED until BotStoppedSpeaking
    while the bot is speaking. Neither suits this add-on:

      * semantic_vad fires speech_started per utterance fragment, so a user
        merely continuing their sentence while an HA tool call is in flight
        would leave that tool's result unsent — the model never learns the
        light did turn on (the same race cancel_on_interruption=False guards
        against in main.py).
      * OpenAI marks a response done once its function_call item is done,
        even if the device is still playing that response's audio; the
        follow-up response.create is valid at that point and its audio simply
        queues behind. Waiting for the device to drain only adds latency.

    So both gates are removed here: tool results push as soon as they land
    (results still queued behind this one are bundled, as pipecat does).
    """

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, UserStartedSpeakingFrame):
            # The base class only uses this flag to gate tool-result pushes.
            self._user_speaking = False

    async def _maybe_push_context_after_function_result(self) -> None:  # type: ignore[override]
        from pipecat.frames.frames import FunctionCallResultFrame

        if self.has_queued_frame(FunctionCallResultFrame):
            logger.debug(f"{self}: more tool results queued — bundling into one push")
            return
        await self.push_context_frame(FrameDirection.UPSTREAM)


class RealtimeContextAggregatorPair(LLMContextAggregatorPair):
    """LLMContextAggregatorPair whose assistant half is RealtimeAssistantAggregator."""

    def __init__(self, context: LLMContext, **kwargs):
        super().__init__(context, **kwargs)
        # Same wiring as the base pair, just a different assistant class. The
        # discarded stock instance only assigned fields.
        self._assistant = RealtimeAssistantAggregator(
            context,
            params=self._assistant._params,
            _realtime_service_mode=kwargs.get("realtime_service_mode"),
            _paired_user_aggregator=self._user,
        )


def make_context_aggregator_pair(context: LLMContext) -> LLMContextAggregatorPair:
    """Build the context aggregator pair the realtime pipeline uses.

    pipecat 1.x moved user-turn detection out of the transport and into the
    user aggregator. Its default `UserTurnStrategies()` runs a local VAD /
    transcription start strategy plus the Smart-Turn v3 ONNX end-of-turn
    analyzer — none of which apply here: OpenAI's server-side VAD decides
    the turns and SafeRealtimeLLMService emits the UserStarted/Stopped
    frames itself, exactly as pipecat 0.0.97 did (see main.py). Passing
    external (inert) turn strategies explicitly

      * skips the eager Smart-Turn model load per connection (~0.7 s), and
      * stops a TranscriptionFrame from opening a phantom user turn (which
        would broadcast a spurious interruption + UserStartedSpeakingFrame).

    It never receives a proposal to resolve, so it is inert; the aggregators
    keep their realtime-mode role of writing the user transcript (when
    transcription is enabled) and the assistant text into the LLMContext that
    SessionManager caches across reconnects.

    `realtime_service_mode=True` is what the pair auto-detects from the
    realtime service anyway; set explicitly so the behaviour does not depend
    on the metadata broadcast timing. The assistant half is the
    RealtimeAssistantAggregator above (immediate tool-result forwarding).
    """
    return RealtimeContextAggregatorPair(
        context,
        user_params=LLMUserAggregatorParams(
            user_turn_strategies=_inert_external_turn_strategies(),
        ),
        realtime_service_mode=True,
    )


class ContextCacheEntry:
    """Entry in the context cache for a specific client."""
    
    def __init__(self, context: LLMContext, timestamp: float):
        self.context = context
        self.timestamp = timestamp


class SessionManager:
    """Manages OpenAI Realtime sessions with context caching per client device.
    
    For each new WebSocket connection, a new session is created, but the context
    from previous sessions for the same client is preserved if the last connection
    closed within the reuse timeout period.
    """
    
    def __init__(self, reuse_timeout: float = 300.0, max_restored_messages: int = 0):
        """Initialize session manager.

        Args:
            reuse_timeout: Time in seconds after which cached context expires
            max_restored_messages: Cap on how many of the most-recent cached
                messages are restored into a new session (0 = unlimited). The
                OpenAI Realtime conversation grows server-side and pipecat
                has no truncation, so every response.create re-bills the whole
                history (audio transcripts + tool results). The device reconnects
                often (follow-up windows, keepalive drops), and each reconnect
                restores the cached context — so capping it here bounds the
                per-turn token cost (and the rate-limit risk) without losing
                recent conversational continuity. A leading system message, if
                present, is always kept.
        """
        self.reuse_timeout = reuse_timeout
        self.max_restored_messages = max(0, int(max_restored_messages))
        # Dictionary mapping client_id to ContextCacheEntry
        self.context_caches: Dict[str, ContextCacheEntry] = {}
        # Dictionary mapping client_id to current service
        self.current_services: Dict[str, OpenAIRealtimeLLMService] = {}
        # Dictionary mapping client_id to context aggregator pair
        self.context_aggregators: Dict[str, LLMContextAggregatorPair] = {}
    
    def get_cached_context(self, client_id: str) -> Optional[LLMContext]:
        """Get cached context for a specific client if it's still valid.
        
        Args:
            client_id: Unique identifier for the client device
            
        Returns:
            Cached LLMContext if valid, None otherwise
        """
        if client_id not in self.context_caches:
            return None
        
        cache_entry = self.context_caches[client_id]
        time_since_cache = time.time() - cache_entry.timestamp
        
        if time_since_cache < self.reuse_timeout:
            logger.info(f"♻️ Using cached context for client {client_id} from {time_since_cache:.1f}s ago")
            return cache_entry.context
        else:
            # Cache expired
            logger.info(f"⏰ Context cache expired for client {client_id} ({time_since_cache:.1f}s ago, timeout: {self.reuse_timeout}s)")
            del self.context_caches[client_id]
            return None
    
    def cache_context_from_service(self, client_id: str, service: OpenAIRealtimeLLMService):
        """Extract and cache context from a service before it's closed.
        
        Args:
            client_id: Unique identifier for the client device
            service: The OpenAI Realtime service to extract context from
        """
        # First try to get context from the context aggregator (more reliable)
        context = None
        if client_id in self.context_aggregators:
            aggregator_pair = self.context_aggregators[client_id]
            # The context is shared between user and assistant aggregators
            user_aggregator = aggregator_pair.user()
            if hasattr(user_aggregator, '_context') and user_aggregator._context:
                context = user_aggregator._context
                logger.debug(f"🔍 Found context in aggregator for client {client_id}")
        
        # Fallback: try to get context from service
        if not context and service and hasattr(service, '_context') and service._context:
            context = service._context
            logger.debug(f"🔍 Found context in service for client {client_id}")
        
        # Cache the context if we found one
        if context:
            messages = context.get_messages() if hasattr(context, 'get_messages') else []
            message_count = len(messages) if messages else 0
            self.context_caches[client_id] = ContextCacheEntry(
                context=context,
                timestamp=time.time()
            )
            logger.info(f"💾 Cached context from previous session for client {client_id} ({message_count} messages)")
        else:
            if not service:
                logger.warning(f"⚠️ No service provided to cache context for client {client_id}")
            elif client_id not in self.context_aggregators:
                logger.warning(f"⚠️ No context aggregator found for client {client_id}")
            elif not hasattr(service, '_context'):
                logger.warning(f"⚠️ Service has no '_context' attribute for client {client_id}")
            elif not service._context:
                logger.warning(f"⚠️ Service context is None for client {client_id}")
            else:
                logger.debug(f"No context to cache from service for client {client_id}")
    
    def create_context_for_new_session(self, client_id: str) -> LLMContext:
        """Create a new context for a new session, reusing cached context if available.
        
        Args:
            client_id: Unique identifier for the client device
            
        Returns:
            LLMContext for the new session (cached or new)
        """
        # Log available cache keys for debugging
        if self.context_caches:
            logger.debug(f"🔍 Available cached contexts: {list(self.context_caches.keys())}")
        else:
            logger.debug("🔍 No cached contexts available")
        
        cached_context = self.get_cached_context(client_id)
        if cached_context:
            # Create a new context instance with the same messages
            # Use the constructor to properly copy messages and tools
            cached_messages = cached_context.get_messages()
            restore_messages = cached_messages.copy() if cached_messages else None
            # Cap the restored history to the most-recent N messages so the
            # per-turn token cost stays bounded (see __init__ docstring). Keep a
            # leading system message if there is one, then the last N of the rest.
            if restore_messages and self.max_restored_messages > 0 and \
                    len(restore_messages) > self.max_restored_messages:
                head = []
                body = restore_messages
                if isinstance(restore_messages[0], dict) and restore_messages[0].get("role") == "system":
                    head = [restore_messages[0]]
                    body = restore_messages[1:]
                trimmed = head + body[-self.max_restored_messages:]
                logger.info(
                    f"✂️ Trimmed restored context for client {client_id}: "
                    f"{len(restore_messages)} → {len(trimmed)} messages (cap {self.max_restored_messages})"
                )
                restore_messages = trimmed
            new_context = LLMContext(
                messages=restore_messages,
                tools=cached_context.tools if hasattr(cached_context, 'tools') else None,
                tool_choice=cached_context.tool_choice if hasattr(cached_context, 'tool_choice') else None
            )
            logger.info(f"✅ Created new context for client {client_id} with {len(new_context.get_messages())} messages from cache")
            return new_context
        else:
            logger.info(f"🆕 Creating new empty context for client {client_id}")
            return LLMContext()
    
    def get_current_service(self, client_id: str) -> Optional[OpenAIRealtimeLLMService]:
        """Get current OpenAI service for a specific client.
        
        Args:
            client_id: Unique identifier for the client device
            
        Returns:
            Current OpenAIRealtimeLLMService if exists, None otherwise
        """
        return self.current_services.get(client_id)
    
    def set_current_service(self, client_id: str, service: OpenAIRealtimeLLMService):
        """Set the current active service for a client.
        
        Args:
            client_id: Unique identifier for the client device
            service: The currently active OpenAI Realtime service
        """
        self.current_services[client_id] = service
    
    def set_context_aggregator(self, client_id: str, aggregator_pair: LLMContextAggregatorPair):
        """Set the context aggregator pair for a client.
        
        Args:
            client_id: Unique identifier for the client device
            aggregator_pair: The LLMContextAggregatorPair instance for this client
        """
        self.context_aggregators[client_id] = aggregator_pair
    
    def remove_context_aggregator(self, client_id: str):
        """Remove the context aggregator pair for a client.
        
        Args:
            client_id: Unique identifier for the client device
        """
        if client_id in self.context_aggregators:
            del self.context_aggregators[client_id]
    
    def cleanup_before_new_session(self, client_id: str):
        """Cleanup before creating a new session for a client.
        
        This should be called before creating a new session to cache
        the context from the current service.
        
        Args:
            client_id: Unique identifier for the client device
        """
        # Cache context from service/aggregator
        if client_id in self.current_services:
            self.cache_context_from_service(client_id, self.current_services[client_id])
            del self.current_services[client_id]
        
        # Remove context aggregator (will be recreated for new session)
        self.remove_context_aggregator(client_id)
    
    def create_context_aggregator(self, client_id: str) -> LLMContextAggregatorPair:
        """Create a context aggregator pair for a new session.
        
        Args:
            client_id: Unique identifier for the client device
            
        Returns:
            LLMContextAggregatorPair with cached or new context
        """
        context = self.create_context_for_new_session(client_id)
        aggregator_pair = make_context_aggregator_pair(context)
        self.set_context_aggregator(client_id, aggregator_pair)
        return aggregator_pair
    
    def create_context_initializer(self, client_id: str, context_aggregator: LLMContextAggregatorPair) -> Optional['ContextInitializer']:
        """Create a context initializer if cached messages exist.
        
        Args:
            client_id: Unique identifier for the client device
            context_aggregator: The context aggregator pair
            
        Returns:
            ContextInitializer if cached messages exist, None otherwise
        """
        context = context_aggregator.user().context
        if len(context.get_messages()) > 0:
            return ContextInitializer(
                context_aggregator=context_aggregator,
                cached_context=context,
                client_id=client_id
            )
        return None
    
    def handle_client_disconnect(self, client_id: str, service: OpenAIRealtimeLLMService) -> None:
        """Handle client disconnection by caching context.
        
        Args:
            client_id: Unique identifier for the client device
            service: The departing connection's service.
        """
        if self.current_services.get(client_id) is not service:
            logger.debug(f"Ignoring stale disconnect for client {client_id}")
            return

        logger.info(f"🔌 Client {client_id} disconnected - caching context")
        try:
            self.cache_context_from_service(client_id, service)
            logger.info(f"💾 Cached context for disconnected client {client_id}")
        except Exception as e:
            logger.exception(f"Error caching context for disconnected client {client_id}: {e}")
        finally:
            if self.current_services.get(client_id) is service:
                del self.current_services[client_id]
                self.remove_context_aggregator(client_id)


class ContextInitializer(FrameProcessor):
    """Processor that sends cached context after StartFrame has passed through the pipeline."""
    
    def __init__(self, context_aggregator, cached_context, client_id, **kwargs):
        super().__init__(**kwargs)
        self.context_aggregator = context_aggregator
        self.cached_context = cached_context
        self.client_id = client_id
        self.context_sent = False
    
    async def process_frame(self, frame: Frame, direction: FrameDirection):
        """Process frames and send cached context after StartFrame."""
        if isinstance(frame, StartFrame):
            await super().process_frame(frame, direction)
            await self.push_frame(frame, direction)
            
            # Send cached context after StartFrame has passed through the pipeline
            # Use LLMMessagesUpdateFrame with run_llm=False to set context without triggering a response
            if self.cached_context and not self.context_sent:
                context = self.cached_context
                messages = context.get_messages()
                if len(messages) > 0:
                    # Update messages without triggering LLM response
                    # The bot will wait for the user to speak first
                    update_frame = LLMMessagesUpdateFrame(messages=messages, run_llm=False)
                    await self.context_aggregator.user().push_frame(update_frame)
                    logger.info(f"📤 Sent cached context ({len(messages)} messages) to OpenAI for client {self.client_id} (waiting for user)")
                    self.context_sent = True
            return
        
        await self.push_frame(frame, direction)
