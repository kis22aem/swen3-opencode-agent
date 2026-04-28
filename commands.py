"""
SWEN v3 CLI commands for Opencode
"""

from .agent import get_agent, Swen3Config


class Swen3Commands:

    def __init__(self):
        self.agent = get_agent()

    def status(self) -> str:
        if not self.agent._connected:
            ok = self.agent.connect()
            if not ok:
                return f"SWEN v3: ○ offline ({self.agent.status})"
        self.agent.discover_workers(timeout=2.0)
        return self.agent.format_status()

    def workers(self) -> str:
        if not self.agent._connected:
            self.agent.connect()
        found = self.agent.discover_workers(timeout=3.0)
        if not found:
            return "SWEN v3: no worker cards found (workers may not publish cards yet)"
        lines = ["SWEN v3 Workers:", "-" * 50]
        for w in found.values():
            lines.append(f"● {w.role:<15} {w.model:<25} {w.host}")
        return "\n".join(lines)

    def ask(self, question: str) -> str:
        if not self.agent._connected:
            ok = self.agent.connect()
            if not ok:
                return "❌ Cannot connect to SWEN v3 mesh"

        result = self.agent.ask(question)

        if result["status"] == "error":
            return f"❌ {result.get('error', 'Unknown error')}"

        lines = []
        # Worker results
        for role, res in result.get("results", {}).items():
            icon = "✅" if res["ok"] else "❌"
            lines.append(f"[{icon} {role}] {res['latency_ms']}ms — {res['model']}")

        # Judge
        judge = result.get("judge_choice", "")
        total = result.get("total_ms", 0)
        lines.append(f"[judge → {judge}] total={total}ms\n")

        # Final answer
        lines.append(result.get("final_answer", ""))
        return "\n".join(lines)

    def connect(self) -> str:
        ok = self.agent.connect()
        return "✅ SWEN v3: Connected" if ok else f"❌ SWEN v3: {self.agent.status}"

    def disconnect(self) -> str:
        self.agent.disconnect()
        return "SWEN v3: Disconnected"

    def mode(self, mode: str) -> str:
        if mode == "local":
            self.agent.disconnect()
            return "SWEN v3: local mode (disconnected)"
        elif mode == "swarm":
            ok = self.agent.connect()
            return "✅ SWEN v3: swarm mode" if ok else f"❌ {self.agent.status}"
        return f"❌ Unknown mode: {mode}"


_commands = Swen3Commands()


def handle_command(args: list) -> str:
    if not args:
        return _commands.status()

    cmd = args[0].lower()

    if cmd == "status":
        return _commands.status()
    elif cmd == "workers":
        return _commands.workers()
    elif cmd == "ask":
        if len(args) < 2:
            return "Usage: /swen ask <question>"
        return _commands.ask(" ".join(args[1:]))
    elif cmd == "connect":
        return _commands.connect()
    elif cmd == "disconnect":
        return _commands.disconnect()
    elif cmd == "mode":
        if len(args) < 2:
            return "Usage: /swen mode <local|swarm>"
        return _commands.mode(args[1])
    else:
        return f"❌ Unknown: {cmd}\nAvailable: status, workers, ask, connect, disconnect, mode"
