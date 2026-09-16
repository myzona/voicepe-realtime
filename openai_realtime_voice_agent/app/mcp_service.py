"""MCP service integration using Pipecat's MCPClient with StreamableHTTP.

pipecat 1.x changed the MCPClient lifecycle: 0.0.97 opened a fresh transport
for every schema fetch and every tool call (stateless, so a Home Assistant
restart was invisible to the add-on); 1.x keeps ONE persistent session in a
dedicated task and refuses to work until `start()` has been awaited. The
session is never re-established by pipecat if the server side goes away — a
`session.call_tool` on a dead session fails, and every later call fails the
same way. Home Assistant restarts a lot (core updates, integration reloads),
so this module wraps the 1.x client with the 0.0.97 robustness:

  * the session is started lazily and idempotently before every use;
  * a failed schema fetch or tool call closes the session, reconnects, and
    retries once — so the first call after an HA restart still succeeds.

The client is process-wide and shared by every device's OpenAI session (one
MCP connection per add-on, not per Voice PE), so its lifetime is owned here
and NOT by any single pipeline. SafeRealtimeLLMService.register_function
wraps every handler in a plain closure precisely so pipecat's per-service
`_pipecat_cleanup` hook never closes this shared connection (see main.py).
"""
import asyncio
import logging
from typing import Optional

from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.services.llm_service import FunctionCallParams
from pipecat.services.mcp_service import MCPClient, StreamableHttpParameters

logger = logging.getLogger(__name__)

# What pipecat's own MCP tool wrapper hands the model when a call yields no
# text. Kept identical so the model-facing behaviour does not change.
_MCP_CALL_FAILED = "Sorry, could not call the mcp tool"


class ResilientMCPClient(MCPClient):
    """MCPClient whose tool calls survive a Home Assistant restart.

    Overrides pipecat's private `_tool_wrapper` (the handler registered for
    every MCP tool) to (re)start the session on demand and to reconnect +
    retry once when the persistent session turns out to be dead. Response
    formatting is delegated back to pipecat's `_call_tool_text`, so tool
    output reaches the model exactly as the stock client would deliver it.
    """

    RECONNECT_LOG = "🔁 HA MCP session unusable (%s) — reconnecting and retrying once"

    async def ensure_started(self) -> None:
        """Start the persistent session if it is not up (idempotent)."""
        await self.start()

    async def restart(self) -> None:
        """Tear the session down and bring a fresh one up."""
        try:
            await self.close()
        except Exception as e:  # pragma: no cover - defensive
            logger.debug(f"MCP close during restart: {e!r}")
        await self.start()

    async def _tool_wrapper(self, params: FunctionCallParams) -> None:  # type: ignore[override]
        """Execute an MCP tool call, reconnecting once on a dead session."""
        function_name = params.function_name
        arguments = params.arguments
        response: Optional[str] = None
        for attempt in (1, 2):
            try:
                await self.ensure_started()
                session = self._ensure_connected()
                # pipecat swallows call_tool errors into the failure sentinel;
                # probe the session first so a dead one is detected as an
                # exception (and retried) rather than reported as a tool failure.
                results = await session.call_tool(function_name, arguments=arguments)
                response = self._format_results(function_name, results)
                break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if attempt == 1:
                    logger.warning(self.RECONNECT_LOG, f"{e.__class__.__name__}: {e}")
                    try:
                        await self.restart()
                    except Exception as e2:
                        logger.error(f"❌ HA MCP reconnect failed: {e2!r}")
                        break
                else:
                    logger.error(f"❌ MCP tool {function_name} failed after reconnect: {e!r}")
        await params.result_callback(response if response is not None else _MCP_CALL_FAILED)

    def _format_results(self, function_name: str, results) -> str:
        """Mirror pipecat's `_call_tool_text` result handling (text parts joined)."""
        response = ""
        if results is not None and getattr(results, "content", None):
            for content in results.content:
                text = getattr(content, "text", None)
                if text:
                    response += text
        if function_name in self._tools_output_filters:
            try:
                response = self._tools_output_filters[function_name](response)
            except Exception:
                logger.error(f"Error applying output filter for {function_name}")
                response = ""
        if response and isinstance(response, str):
            logger.info(f"Tool '{function_name}' completed successfully")
            return response
        return _MCP_CALL_FAILED


class HomeAssistantMCPService:
    """Home Assistant MCP service using Pipecat's MCPClient."""

    def __init__(self, url: str, access_token: str):
        """
        Initialize Home Assistant MCP service.

        Args:
            url: Home Assistant MCP Server URL (e.g., http://supervisor/core/api/mcp)
            access_token: Long-lived access token for Home Assistant
        """
        self.url = url
        self.access_token = access_token
        self.mcp_client: Optional[ResilientMCPClient] = None

    async def initialize(self) -> ResilientMCPClient:
        """Create the MCP client and open its persistent session.

        Connection failures are logged, not raised: Home Assistant may still be
        starting when the add-on boots, and the resilient client reconnects on
        first use, so a cold start must not disable HA tools for the session.
        """
        try:
            logger.info(f"🔗 Initializing Home Assistant MCP Client at {self.url}")

            # Create StreamableHTTP parameters with authentication
            server_params = StreamableHttpParameters(
                url=self.url,
                headers={
                    "Authorization": f"Bearer {self.access_token}"
                }
            )

            # Create MCP client
            self.mcp_client = ResilientMCPClient(server_params=server_params)
            try:
                await self.mcp_client.ensure_started()
                logger.info("✅ Home Assistant MCP Client connected")
            except Exception as e:
                logger.warning(
                    f"⚠️ Home Assistant MCP server not reachable yet ({e!r}); "
                    "will retry on first use"
                )

            logger.info("✅ Home Assistant MCP Client initialized")
            return self.mcp_client

        except Exception as e:
            logger.error(f"❌ Failed to initialize Home Assistant MCP Client: {e}", exc_info=True)
            raise

    def get_client(self) -> Optional[ResilientMCPClient]:
        """Get the MCP client instance."""
        return self.mcp_client

    async def fetch_tools_schema(self) -> ToolsSchema:
        """List the HA MCP tools, reconnecting once if the session is dead.

        Returns the pipecat ToolsSchema (standard_tools carry name /
        description / properties / required) WITHOUT handlers attached — the
        add-on converts these to OpenAI realtime tool dicts and registers the
        handlers itself (register_handlers), because the per-connection
        SafeRealtimeLLMService.register_function must wrap every handler.
        """
        client = self.mcp_client
        if client is None:
            raise RuntimeError("MCP client not initialized")
        for attempt in (1, 2):
            try:
                await client.ensure_started()
                session = client._ensure_connected()
                return await client._list_tools_helper(session)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if attempt == 1:
                    logger.warning(client.RECONNECT_LOG, f"{e.__class__.__name__}: {e}")
                    await client.restart()
                else:
                    raise
        raise RuntimeError("unreachable")  # pragma: no cover

    def register_handlers(self, tools_schema: ToolsSchema, llm) -> int:
        """Register the resilient tool handler for every tool in the schema.

        Same shape as pipecat's (deprecated in 1.8) `register_tools_schema`:
        every MCP tool gets the client's `_tool_wrapper`; the LLM service is
        expected to wrap it (liveness tracking, speaker gate — main.py).

        Returns:
            The number of handlers registered.
        """
        client = self.mcp_client
        if client is None:
            raise RuntimeError("MCP client not initialized")
        count = 0
        for function_schema in tools_schema.standard_tools:
            llm.register_function(function_schema.name, client._tool_wrapper)
            count += 1
        return count

    async def close(self) -> None:
        """Close the shared MCP session (add-on shutdown only)."""
        if self.mcp_client is not None:
            try:
                await self.mcp_client.close()
            except Exception as e:
                logger.debug(f"MCP close: {e!r}")
