import asyncio
import logging
import os
import signal
import sys
import time
import uuid
from concurrent import futures

os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "0")

import grpc

import agent_pb2
import agent_pb2_grpc

from claude_agent_sdk import query, ClaudeAgentOptions, AssistantMessage, ResultMessage, TextBlock, ThinkingBlock

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

AGENT_ID = os.getenv("AGENT_ID", "claude-proxy-1")
AGENT_TYPE = os.getenv("AGENT_TYPE", "claude")
GATEWAY_ADDR = os.getenv("GATEWAY_ADDR", "localhost:19090")
MAX_TURNS = int(os.getenv("MAX_TURNS", "3"))


class ClaudeProxyAgent:
    def __init__(self):
        self._stop_event = asyncio.Event()
        # Mapping: wrapper_session_id -> claude_session_id
        # This allows us to send session_id with the first chunk
        self._session_map: dict[str, str] = {}
        self._channel: grpc.aio.Channel | None = None

    async def run(self):
        while not self._stop_event.is_set():
            try:
                await self._connect_and_serve()
            except Exception as e:
                if self._stop_event.is_set():
                    break
                logger.error(f"connection lost: {e}, reconnecting in 3s...")
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=3)
                    break
                except asyncio.TimeoutError:
                    pass

    async def _connect_and_serve(self):
        channel = grpc.aio.insecure_channel(
            GATEWAY_ADDR,
            options=[("grpc.keepalive_time_ms", 60000), ("grpc.keepalive_permit_without_calls", 1)],
        )
        self._channel = channel
        stub = agent_pb2_grpc.AgentGatewayStub(channel)
        stream = stub.Connect()

        reg = agent_pb2.AgentMessage(
            register=agent_pb2.AgentRegister(agent_id=AGENT_ID, agent_type=AGENT_TYPE)
        )
        await stream.write(reg)
        logger.info(f"registered as {AGENT_TYPE}/{AGENT_ID} at {GATEWAY_ADDR}")

        heartbeat_task = asyncio.create_task(self._heartbeat_loop(stream))
        try:
            await self._recv_loop(stream)
        finally:
            heartbeat_task.cancel()
            await channel.close()

    async def _heartbeat_loop(self, stream):
        while True:
            await asyncio.sleep(30)
            hb = agent_pb2.AgentMessage(
                heartbeat=agent_pb2.Heartbeat(timestamp=int(time.time()))
            )
            await stream.write(hb)

    async def _recv_loop(self, stream):
        async for msg in stream:
            req = msg.request
            if not req.request_id:
                continue
            asyncio.create_task(self._handle_request(stream, req))

    async def _handle_request(self, stream, req: agent_pb2.AgentRequest):
        prompt = req.messages[-1].content if req.messages else ""
        wrapper_session_id = req.session_id
        logger.info(f"received request {req.request_id}, sessionID:{wrapper_session_id} prompt: {prompt[:80]}")

        if wrapper_session_id:
            # Look up the real Claude session ID from our mapping
            claude_session_id = self._session_map.get(wrapper_session_id)
            if claude_session_id:
                got_content = await self._query_claude(stream, req, prompt, wrapper_session_id, claude_session_id)
                if got_content:
                    return
                logger.info(f"resume session {wrapper_session_id} (claude: {claude_session_id}) failed, retrying as new session")
            else:
                logger.info(f"no claude session mapping for {wrapper_session_id}, starting new session")

        # New session: generate wrapper_session_id upfront
        new_wrapper_session_id = str(uuid.uuid4())
        await self._query_claude(stream, req, prompt, new_wrapper_session_id, None)

    async def _query_claude(
        self, stream, req, prompt: str, wrapper_session_id: str, claude_session_id: str | None
    ) -> bool:
        """
        Query Claude and stream responses.

        Args:
            wrapper_session_id: Our session ID sent to gateway (generated upfront)
            claude_session_id: Claude's actual session ID (used for resume, None for new session)

        Returns:
            True if got content, False if failed (e.g., resume failed)
        """
        options = ClaudeAgentOptions(
            permission_mode="bypassPermissions",
            max_turns=MAX_TURNS,
            env={
                "ANTHROPIC_AUTH_TOKEN": os.getenv("ANTHROPIC_AUTH_TOKEN", ""),
                "ANTHROPIC_BASE_URL": os.getenv("ANTHROPIC_BASE_URL", ""),
            },
        )
        if claude_session_id:
            options.resume = claude_session_id

        got_content = False

        try:
            async for msg in query(prompt=prompt, options=options):
                if isinstance(msg, AssistantMessage):
                    for block in msg.content:
                        if isinstance(block, TextBlock):
                            await self._send_chunk(stream, req.request_id, wrapper_session_id, block.text)
                            got_content = True
                        elif isinstance(block, ThinkingBlock) and block.thinking:
                            await self._send_thinking_chunk(stream, req.request_id, wrapper_session_id, block.thinking)

                elif isinstance(msg, ResultMessage):
                    if msg.session_id:
                        # Store mapping: wrapper -> claude
                        self._session_map[wrapper_session_id] = msg.session_id
                        logger.info(f"session mapping: {wrapper_session_id} -> {msg.session_id}")
                    if msg.result:
                        await self._send_chunk(stream, req.request_id, wrapper_session_id, msg.result)
                        got_content = True

        except Exception as e:
            logger.error(f"claude query failed: {e}")
            if not claude_session_id:
                # New session failed, send error
                await self._send_done(stream, req.request_id, wrapper_session_id, f"error: {e}")
                return True
            # Resume failed, caller should retry as new session
            return False

        if got_content:
            await self._send_done(stream, req.request_id, wrapper_session_id, "")

        return got_content

    async def _send_chunk(self, stream, request_id: str, session_id: str, content: str):
        resp = agent_pb2.AgentMessage(
            response=agent_pb2.AgentResponse(
                request_id=request_id,
                session_id=session_id,
                chunk=agent_pb2.StreamChunk(content=content),
                done=False,
            )
        )
        await stream.write(resp)

    async def _send_thinking_chunk(self, stream, request_id: str, session_id: str, reasoning: str):
        resp = agent_pb2.AgentMessage(
            response=agent_pb2.AgentResponse(
                request_id=request_id,
                session_id=session_id,
                chunk=agent_pb2.StreamChunk(reasoning_content=reasoning),
                done=False,
            )
        )
        await stream.write(resp)

    async def _send_done(self, stream, request_id: str, session_id: str, content: str):
        resp = agent_pb2.AgentMessage(
            response=agent_pb2.AgentResponse(
                request_id=request_id,
                session_id=session_id,
                message=agent_pb2.ChatMessage(role="assistant", content=content),
                done=True,
            )
        )
        await stream.write(resp)

    def stop(self):
        self._stop_event.set()
        if self._channel:
            asyncio.ensure_future(self._channel.close())


async def main():
    agent = ClaudeProxyAgent()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, agent.stop)

    logger.info(f"claude proxy agent [{AGENT_TYPE}/{AGENT_ID}] connecting to gateway at {GATEWAY_ADDR}")
    await agent.run()
    logger.info("shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
