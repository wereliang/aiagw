#!/usr/bin/env python3
"""Simple test script for claude_agent_sdk with session support."""

import argparse
import asyncio
import os

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ResultMessage,
    TextBlock,
    query,
)


async def test_claude(prompt: str, session_id: str | None = None, max_turns: int = 3) -> None:
    """Query Claude and print results with session info."""
    options = ClaudeAgentOptions(
        permission_mode="bypassPermissions",
        max_turns=max_turns,
        env={
            "ANTHROPIC_AUTH_TOKEN": os.getenv("ANTHROPIC_AUTH_TOKEN", ""),
            "ANTHROPIC_BASE_URL": os.getenv("ANTHROPIC_BASE_URL", ""),
        },
    )

    if session_id:
        options.resume = session_id
        print(f"[INFO] Resuming session: {session_id}")
    else:
        print("[INFO] Starting new session")

    print(f"[INFO] Prompt: {prompt}")
    print("-" * 60)

    result_session_id = ""

    try:
        async for msg in query(prompt=prompt, options=options):
            if isinstance(msg, AssistantMessage):
                for block in msg.content:
                    if isinstance(block, TextBlock):
                        print(f"[CHUNK] {block.text}")

            elif isinstance(msg, ResultMessage):
                if msg.session_id:
                    result_session_id = msg.session_id
                if msg.result:
                    print(f"[RESULT] {msg.result}")

    except Exception as e:
        print(f"[ERROR] {e}")
        return

    print("-" * 60)
    print(f"[INFO] Input Session ID:  {session_id or '(none)'}")
    print(f"[INFO] Output Session ID: {result_session_id or '(none)'}")

    if session_id and result_session_id and session_id != result_session_id:
        print("[WARN] Session ID changed! Agent created a new session.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Test claude_agent_sdk with session support")
    parser.add_argument("prompt", nargs="?", default="Say hello", help="Prompt to send")
    parser.add_argument("-s", "--session", help="Session ID to resume")
    parser.add_argument("-t", "--turns", type=int, default=3, help="Max turns (default: 3)")
    args = parser.parse_args()

    asyncio.run(test_claude(args.prompt, args.session, args.turns))


if __name__ == "__main__":
    main()
