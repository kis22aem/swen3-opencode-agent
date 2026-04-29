"""
SWEN v3 Agent for Opencode
Native integration with LangGraph + Zenoh swarm
"""

import asyncio
import json
import os
import uuid
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime

import zenoh
from zenoh import handlers as zenoh_handlers


@dataclass
class WorkerInfo:
    worker_id: str
    host: str
    role: str
    model: str
    max_in_flight: int = 1
    status: str = "online"
    load: int = 0
    last_seen: Optional[datetime] = None

    @property
    def is_available(self) -> bool:
        return self.status == "online" and self.load < self.max_in_flight


@dataclass
class AskResult:
    role: str
    worker_id: str
    ok: bool
    answer: str
    latency_ms: int
    model: str
    error: Optional[str] = None


@dataclass
class Swen3Config:
    enabled: bool = True
    transport: str = "zenoh"
    zenoh_mode: str = "peer"
    zenoh_connect: List[str] = field(default_factory=lambda: [
        "tcp/10.15.64.226:7447"
    ])
    zenoh_listen: List[str] = field(default_factory=list)
    roles: List[str] = field(default_factory=lambda: ["gpt_oss_20b"])
    deadline_ms: int = 60000
    fallback_enabled: bool = True


class Swen3Agent:
    """
    Native SWEN v3 agent for Opencode.
    Connects to Zenoh mesh, discovers workers, parallel fanout + judge.
    """

    def __init__(self, config: Optional[Swen3Config] = None):
        self.config = config or Swen3Config()
        self.session: Optional[zenoh.Session] = None
        self.workers: Dict[str, WorkerInfo] = {}
        self.status = "disconnected"
        self._connected = False

    # ── Connection ──────────────────────────────────────────────────────────

    def connect(self) -> bool:
        try:
            cfg = zenoh.Config()
            cfg.insert_json5("mode", f'"{self.config.zenoh_mode}"')
            if self.config.zenoh_connect:
                cfg.insert_json5("connect/endpoints", json.dumps(self.config.zenoh_connect))
            if self.config.zenoh_listen:
                cfg.insert_json5("listen/endpoints", json.dumps(self.config.zenoh_listen))
            cfg.insert_json5("scouting/multicast/enabled", "false")
            cfg.insert_json5("scouting/gossip/enabled", "true")

            self.session = zenoh.open(cfg)
            self.status = "connected"
            self._connected = True
            return True
        except Exception as e:
            self.status = f"error: {e}"
            return False

    def disconnect(self):
        if self.session:
            try:
                self.session.close()
            except Exception:
                pass
            self.session = None
        self.status = "disconnected"
        self._connected = False
        self.workers.clear()

    def _ensure_connected(self) -> bool:
        if not self._connected or self.session is None:
            return self.connect()
        return True

    # ── Worker discovery ────────────────────────────────────────────────────

    def discover_workers(self, timeout: float = 3.0) -> Dict[str, WorkerInfo]:
        """Return configured roles as WorkerInfo objects.
        
        Note: Live discovery via swen/v3/cards/** is not supported because
        Diana workers publish cards without declaring a queryable.
        The ask() method works fine by querying swen/v3/ask/{role} directly.
        """
        if not self._ensure_connected():
            return {}
        
        # Return configured roles as placeholder workers
        found = {}
        for i, role in enumerate(self.config.roles):
            wid = f"configured.{role}.{i}"
            found[wid] = WorkerInfo(
                worker_id=wid,
                host="diana",
                role=role,
                model=role,
                max_in_flight=3,
                status="online",
                last_seen=datetime.now()
            )
        self.workers = found
        return found

    # ── Single ask ──────────────────────────────────────────────────────────

    def ask_one(self, role: str, question: str, thread_id: str) -> AskResult:
        """Send question to one role, wait for answer."""
        # Handle Jetson HTTP worker directly
        if role == "jetson_qwen35_2b":
            return self._ask_jetson(question, thread_id)
        
        # Handle local MacBook LM Studio worker via Zenoh bridge
        if role == "macbook_huihui_qwen3_5_2b":
            return self._ask_macbook_via_zenoh(question, thread_id)
        
        if not self._ensure_connected():
            return AskResult(role=role, worker_id="?", ok=False,
                             answer="", latency_ms=0, model="?",
                             error="Not connected")
        req = {
            "request_id": str(uuid.uuid4()),
            "thread_id": thread_id,
            "role": role,
            "question": question,
            "deadline_ms": self.config.deadline_ms,
        }
        try:
            fifo = zenoh_handlers.FifoChannel(4)
            timeout_s = self.config.deadline_ms / 1000.0 + 5
            replies = self.session.get(
                f"swen/v3/ask/{role}",
                handler=fifo,
                payload=json.dumps(req).encode(),
                timeout=timeout_s
            )
            for reply in replies:
                try:
                    # Zenoh 1.9: reply.ok is Sample, reply.err is ReplyError
                    if reply.ok is None:
                        err_msg = str(reply.err) if reply.err else "no ok payload"
                        return AskResult(role=role, worker_id="?", ok=False,
                                         answer="", latency_ms=0, model="?",
                                         error=err_msg)
                    resp = json.loads(bytes(reply.ok.payload).decode())
                    return AskResult(
                        role=role,
                        worker_id=resp.get("worker_id", "?"),
                        ok=resp.get("ok", False),
                        answer=resp.get("answer", ""),
                        latency_ms=resp.get("latency_ms", 0),
                        model=resp.get("model", "?"),
                        error=resp.get("error")
                    )
                except Exception as e:
                    return AskResult(role=role, worker_id="?", ok=False,
                                     answer="", latency_ms=0, model="?",
                                     error=str(e))
        except Exception as e:
            return AskResult(role=role, worker_id="?", ok=False,
                             answer="", latency_ms=0, model="?",
                             error=str(e))

        return AskResult(role=role, worker_id="?", ok=False,
                         answer="", latency_ms=0, model="?",
                         error="no reply")
    
    def _ask_jetson(self, question: str, thread_id: str) -> AskResult:
        """Send question to Jetson via HTTP directly."""
        import urllib.request
        import time
        
        JETSON_URL = "http://10.15.66.12:8080/v1/chat/completions"
        MODEL = "Qwen3.5-2B.Q4_K_M.gguf"
        SYSTEM_PROMPT = "You are a helpful assistant. Be concise."
        
        try:
            api_req = {
                "model": MODEL,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": question}
                ],
                "temperature": 0.7,
                "max_tokens": 1024,
            }
            
            req_data = json.dumps(api_req).encode()
            api_request = urllib.request.Request(
                JETSON_URL,
                data=req_data,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            
            start = time.time()
            with urllib.request.urlopen(api_request, timeout=120) as resp:
                api_resp = json.loads(resp.read().decode())
            
            latency_ms = int((time.time() - start) * 1000)
            answer = api_resp["choices"][0]["message"]["content"]
            
            return AskResult(
                role="jetson_qwen35_2b",
                worker_id="jetson",
                ok=True,
                answer=answer,
                latency_ms=latency_ms,
                model=MODEL,
                error=None
            )
        except Exception as e:
            return AskResult(
                role="jetson_qwen35_2b",
                worker_id="jetson",
                ok=False,
                answer="",
                latency_ms=0,
                model=MODEL,
                error=str(e)
            )

    def _ask_macbook_via_zenoh(self, question: str, thread_id: str) -> AskResult:
        """Send question to MacBook LM Studio via Zenoh bridge."""
        if not self._ensure_connected():
            return AskResult(
                role="macbook_huihui_qwen3_5_2b",
                worker_id="?",
                ok=False,
                answer="",
                latency_ms=0,
                model="huihui-qwen3.5-2b-abliterated",
                error="Not connected to Zenoh"
            )
        
        req = {
            "request_id": str(uuid.uuid4()),
            "thread_id": thread_id,
            "role": "macbook_huihui_qwen3_5_2b",
            "question": question,
            "deadline_ms": self.config.deadline_ms,
        }
        
        try:
            fifo = zenoh_handlers.FifoChannel(4)
            timeout_s = self.config.deadline_ms / 1000.0 + 5
            replies = self.session.get(
                "swen/v3/ask/macbook_huihui_qwen3_5_2b",
                handler=fifo,
                payload=json.dumps(req).encode(),
                timeout=timeout_s
            )
            for reply in replies:
                try:
                    if reply.ok is None:
                        err_msg = str(reply.err) if reply.err else "no ok payload"
                        return AskResult(
                            role="macbook_huihui_qwen3_5_2b",
                            worker_id="?",
                            ok=False,
                            answer="",
                            latency_ms=0,
                            model="huihui-qwen3.5-2b-abliterated",
                            error=err_msg
                        )
                    resp = json.loads(bytes(reply.ok.payload).decode())
                    return AskResult(
                        role="macbook_huihui_qwen3_5_2b",
                        worker_id=resp.get("worker_id", "macbook"),
                        ok=resp.get("ok", False),
                        answer=resp.get("answer", ""),
                        latency_ms=resp.get("latency_ms", 0),
                        model=resp.get("model", "huihui-qwen3.5-2b-abliterated"),
                        error=resp.get("error")
                    )
                except Exception as e:
                    return AskResult(
                        role="macbook_huihui_qwen3_5_2b",
                        worker_id="?",
                        ok=False,
                        answer="",
                        latency_ms=0,
                        model="huihui-qwen3.5-2b-abliterated",
                        error=str(e)
                    )
        except Exception as e:
            return AskResult(
                role="macbook_huihui_qwen3_5_2b",
                worker_id="?",
                ok=False,
                answer="",
                latency_ms=0,
                model="huihui-qwen3.5-2b-abliterated",
                error=str(e)
            )
        
        return AskResult(
            role="macbook_huihui_qwen3_5_2b",
            worker_id="?",
            ok=False,
            answer="",
            latency_ms=0,
            model="huihui-qwen3.5-2b-abliterated",
            error="no reply"
        )

    # ── Parallel fanout ─────────────────────────────────────────────────────

    def ask(self, question: str, thread: Optional[str] = None) -> Dict[str, Any]:
        """
        Fan out question to all configured roles in parallel.
        Judge selects best answer.
        Returns structured result.
        """
        if not self._ensure_connected():
            return {"status": "error", "error": "Not connected to SWEN v3"}

        thread_id = thread or str(uuid.uuid4())
        roles = self.config.roles
        start = datetime.now()

        print(f"[SWEN v3] Fanout → {roles}")

        # Parallel fanout via threads (Zenoh is sync)
        results: Dict[str, AskResult] = {}
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=len(roles)) as pool:
            futures = {
                pool.submit(self.ask_one, role, question, thread_id): role
                for role in roles
            }
            for future in as_completed(futures):
                role = futures[future]
                try:
                    result = future.result()
                    results[role] = result
                    icon = "✅" if result.ok else "❌"
                    print(f"[SWEN v3] {icon} {role} ({result.latency_ms}ms)")
                except Exception as e:
                    results[role] = AskResult(
                        role=role, worker_id="?", ok=False,
                        answer="", latency_ms=0, model="?", error=str(e)
                    )

        # Judge: pick best answer
        best_role, best_answer = self._judge(results)
        total_ms = int((datetime.now() - start).total_seconds() * 1000)

        print(f"[SWEN v3] Judge → {best_role} | total={total_ms}ms")

        return {
            "status": "success" if best_answer else "error",
            "thread": thread_id,
            "question": question,
            "workers_used": [r for r, res in results.items() if res.ok],
            "results": {r: {"ok": res.ok, "answer": res.answer,
                            "latency_ms": res.latency_ms, "model": res.model,
                            "error": res.error}
                        for r, res in results.items()},
            "judge_choice": best_role,
            "final_answer": best_answer,
            "total_ms": total_ms,
        }

    def _judge(self, results: Dict[str, AskResult]) -> Tuple[str, str]:
        """
        Select best answer from results.
        Strategy: longest non-empty answer from successful worker.
        TODO: replace with LLM judge.
        """
        ok_results = {r: res for r, res in results.items() if res.ok and res.answer.strip()}
        if not ok_results:
            # Fallback: any answer
            for r, res in results.items():
                if res.answer.strip():
                    return r, res.answer.strip()
            return "", ""

        # Pick answer with most content
        best = max(ok_results.items(), key=lambda x: len(x[1].answer.strip()))
        return best[0], best[1].answer.strip()

    # ── Status ───────────────────────────────────────────────────────────────

    def get_status(self) -> Dict[str, Any]:
        peers = []
        if self._connected and self.session:
            try:
                peers = [str(p)[:8] for p in self.session.info.peers_zid()]
            except Exception:
                pass
        return {
            "status": self.status,
            "connected": self._connected,
            "peers": peers,
            "workers": [
                {
                    "role": w.role,
                    "host": w.host,
                    "model": w.model,
                    "status": w.status,
                    "load": f"{w.load}/{w.max_in_flight}",
                }
                for w in self.workers.values()
            ],
        }

    def format_status(self) -> str:
        st = self.get_status()
        icon = "●" if st["connected"] else "○"
        peers = st["peers"]

        lines = [
            "╔══════════════════════════════════════════╗",
            "║           SWEN v3 Status                 ║",
            "╠══════════════════════════════════════════╣",
            f"║ {icon} {self.status:<40}║",
            f"║ Peers: {', '.join(peers) if peers else 'none':<34}║",
        ]

        if st["workers"]:
            lines.append("╠══════════════════════════════════════════╣")
            for w in st["workers"]:
                icon2 = "●" if w["status"] == "online" else "○"
                lines.append(f"║ {icon2} {w['role']:<15} {w['model'][:20]:<20} ║")
        else:
            lines.append("╠══════════════════════════════════════════╣")
            lines.append(f"║ Configured roles: {', '.join(self.config.roles):<22}║")

        lines.append("╚══════════════════════════════════════════╝")
        return "\n".join(lines)


# ── Singleton ────────────────────────────────────────────────────────────────

_agent: Optional[Swen3Agent] = None


def get_agent() -> Swen3Agent:
    global _agent
    if _agent is None:
        _agent = Swen3Agent()
    return _agent
