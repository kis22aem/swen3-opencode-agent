#!/usr/bin/env python3
"""
swen3_query.py — CLI wrapper for SWEN v3 agent
Usage: python3 swen3_query.py "your question here"
"""
import sys
import os
import time

# Add agent path
sys.path.insert(0, os.path.expanduser("~/.config/opencode/agents/swen3"))

from agent import Swen3Agent, Swen3Config

def main():
    if len(sys.argv) < 2:
        print("Usage: swen3_query.py <question>")
        sys.exit(1)

    question = " ".join(sys.argv[1:])

    cfg = Swen3Config(
        zenoh_connect=["tcp/10.15.64.226:7447", "tcp/10.15.66.12:7447"],
        zenoh_listen=["tcp/0.0.0.0:7447"],
        roles=["glm_flash", "qwen3_5_4b_opus", "jetson_gemma4b"],
        deadline_ms=60000,
    )
    agent = Swen3Agent(cfg)

    if not agent.connect():
        print(f"[SWEN v3] ❌ Cannot connect: {agent.status}", file=sys.stderr)
        sys.exit(1)

    time.sleep(1.5)
    result = agent.ask(question)
    agent.disconnect()

    if result["status"] == "error":
        print(f"[SWEN v3] ❌ {result.get('error')}", file=sys.stderr)
        sys.exit(1)

    print(result["final_answer"])

if __name__ == "__main__":
    main()
