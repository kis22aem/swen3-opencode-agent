#!/usr/bin/env python3
"""
SwarmProvider v4 — полностью децентрализованный рой
Peer-to-peer discovery, динамические воркеры, heartbeat
"""

import json
import os
import sys
import time
import uuid
import asyncio
import threading
from datetime import datetime
from typing import Dict, List, Optional, Any, AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
import uvicorn

sys.path.insert(0, os.path.expanduser("~/.local/share/swen3"))
from agent import Swen3Agent, Swen3Config, AskResult
from decentralized_swarm import MulticastDiscovery, WorkerInfo

app = FastAPI(title="SWEN v4 Decentralized SwarmProvider")

# ── Configuration ───────────────────────────────────────────────────────────

DEADLINE_MS = 60000
LOG_FILE = "/Users/alex/.local/share/swen3/swarm_provider.log"
HEARTBEAT_INTERVAL = 10

# ── Logging ─────────────────────────────────────────────────────────────────

def log_event(direction: str, data: dict):
    timestamp = datetime.now().isoformat()
    entry = {"timestamp": timestamp, "direction": direction, "data": data}
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

# ── Decentralized Swarm Backend ─────────────────────────────────────────────

class DecentralizedSwarmBackend:
    """Полностью децентрализованный бэкенд роя"""
    
    def __init__(self):
        self.agent: Optional[Swen3Agent] = None
        self.discovery: Optional[MulticastDiscovery] = None
        self.connected = False
        self.worker_states: Dict[str, dict] = {}
        self._lock = threading.Lock()
        
    def connect(self) -> bool:
        """Подключение к децентрализованному рою"""
        try:
            # 1. Подключаемся к Zenoh в peer mode (без роутера)
            cfg = Swen3Config(
                zenoh_mode="peer",
                zenoh_connect=[],  # Не подключаемся к роутеру
                deadline_ms=DEADLINE_MS,
            )
            self.agent = Swen3Agent(cfg)
            
            # Настраиваем Zenoh для peer-to-peer
            import zenoh
            zcfg = zenoh.Config()
            zcfg.insert_json5("mode", '"peer"')
            zcfg.insert_json5("listen/endpoints", '["tcp/0.0.0.0:0"]')
            zcfg.insert_json5("scouting/multicast/enabled", "true")
            zcfg.insert_json5("scouting/gossip/enabled", "true")
            
            self.agent.session = zenoh.open(zcfg)
            self.agent._connected = True
            
            # 2. Запускаем multicast discovery
            import socket
            hostname = socket.gethostname()
            local_ip = socket.getaddrinfo(hostname, None, socket.AF_INET)[0][4][0]
            
            self.discovery = MulticastDiscovery(
                worker_id=f"swarm-provider-{uuid.uuid4().hex[:8]}",
                role="provider",
                model="swarm-provider",
                zenoh_endpoint=f"tcp/{local_ip}:0"
            )
            self.discovery.start()
            
            # 3. Ждём обнаружения воркеров
            time.sleep(3)
            
            self.connected = True
            print(f"✅ Connected to decentralized swarm")
            print(f"   Workers found: {len(self.discovery.get_alive_workers())}")
            return True
            
        except Exception as e:
            print(f"[SwarmBackend] Connection error: {e}")
            return False
    
    def disconnect(self):
        if self.discovery:
            self.discovery.stop()
        if self.agent and self.agent.session:
            self.agent.session.close()
        self.connected = False
    
    def ensure_connected(self) -> bool:
        if not self.connected:
            return self.connect()
        return True
    
    def get_alive_workers(self) -> List[WorkerInfo]:
        """Возвращает список живых воркеров из multicast discovery"""
        if self.discovery:
            return self.discovery.get_alive_workers()
        return []
    
    def get_worker_status(self) -> Dict[str, Any]:
        """Возвращает текущее состояние всех воркеров"""
        workers = self.get_alive_workers()
        return {
            "swarm_connected": self.connected,
            "worker_count": len(workers),
            "workers": {
                w.worker_id: {
                    "role": w.role,
                    "model": w.model,
                    "host": w.host,
                    "status": "online" if w.is_alive else "dead",
                    "last_seen": w.last_seen,
                    "zenoh_endpoint": w.zenoh_endpoint
                }
                for w in workers
            },
            "timestamp": datetime.now().isoformat(),
        }
    
    def fanout(self, messages: List[Dict], model: str) -> Dict[str, Any]:
        """
        Отправляет сообщения на ВСЕХ обнаруженных воркеров параллельно.
        Возвращает лучший ответ + метаданные.
        """
        if not self.ensure_connected():
            return {"error": "Not connected to swarm", "status": "error"}
        
        # Получаем список живых воркеров
        workers = self.get_alive_workers()
        if not workers:
            return {"error": "No workers available", "status": "error"}
        
        question = self._extract_question(messages)
        thread_id = f"swarm-{uuid.uuid4().hex[:8]}"
        
        print(f"[SwarmProvider] Fanout → {len(workers)} workers")
        print(f"[SwarmProvider] Question: {question[:80]}...")
        
        # Параллельный fanout на всех воркеров
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        results: Dict[str, AskResult] = {}
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=len(workers)) as pool:
            futures = {
                pool.submit(self._ask_worker, worker, question, thread_id): worker
                for worker in workers
            }
            for future in as_completed(futures):
                worker = futures[future]
                try:
                    result = future.result()
                    results[worker.worker_id] = result
                    icon = "✅" if result.ok else "❌"
                    print(f"[SwarmProvider] {icon} {worker.role} ({result.latency_ms}ms)")
                except Exception as e:
                    results[worker.worker_id] = AskResult(
                        role=worker.role, worker_id=worker.worker_id, ok=False,
                        answer="", latency_ms=0, model=worker.model, error=str(e)
                    )
        
        total_ms = int((time.time() - start_time) * 1000)
        
        # Judge: выбираем лучший ответ
        best_worker, best_answer = self._judge(results)
        
        print(f"[SwarmProvider] Judge → {best_worker} | total={total_ms}ms")
        
        return {
            "status": "success" if best_answer else "error",
            "thread": thread_id,
            "question": question,
            "workers_used": [wid for wid, r in results.items() if r.ok],
            "results": {
                wid: {
                    "ok": r.ok,
                    "answer": r.answer,
                    "latency_ms": r.latency_ms,
                    "model": r.model,
                    "error": r.error
                }
                for wid, r in results.items()
            },
            "judge_choice": best_worker,
            "final_answer": best_answer,
            "total_ms": total_ms,
        }
    
    def _ask_worker(self, worker: WorkerInfo, question: str, thread_id: str) -> AskResult:
        """Отправляет вопрос конкретному воркеру через Zenoh"""
        if not self.agent or not self.agent.session:
            return AskResult(
                role=worker.role, worker_id=worker.worker_id, ok=False,
                answer="", latency_ms=0, model=worker.model, error="No Zenoh session"
            )
        
        req = {
            "request_id": str(uuid.uuid4()),
            "thread_id": thread_id,
            "role": worker.role,
            "question": question,
            "deadline_ms": DEADLINE_MS,
        }
        
        try:
            fifo = zenoh_handlers.FifoChannel(4)
            timeout_s = DEADLINE_MS / 1000.0 + 5
            replies = self.agent.session.get(
                f"swen/v4/ask/{worker.role}",
                handler=fifo,
                payload=json.dumps(req).encode(),
                timeout=timeout_s
            )
            for reply in replies:
                try:
                    if reply.ok is None:
                        err_msg = str(reply.err) if reply.err else "no ok payload"
                        return AskResult(
                            role=worker.role, worker_id=worker.worker_id, ok=False,
                            answer="", latency_ms=0, model=worker.model, error=err_msg
                        )
                    resp = json.loads(bytes(reply.ok.payload).decode())
                    return AskResult(
                        role=worker.role,
                        worker_id=resp.get("worker_id", worker.worker_id),
                        ok=resp.get("ok", False),
                        answer=resp.get("answer", ""),
                        latency_ms=resp.get("latency_ms", 0),
                        model=resp.get("model", worker.model),
                        error=resp.get("error")
                    )
                except Exception as e:
                    return AskResult(
                        role=worker.role, worker_id=worker.worker_id, ok=False,
                        answer="", latency_ms=0, model=worker.model, error=str(e)
                    )
        except Exception as e:
            return AskResult(
                role=worker.role, worker_id=worker.worker_id, ok=False,
                answer="", latency_ms=0, model=worker.model, error=str(e)
            )
        
        return AskResult(
            role=worker.role, worker_id=worker.worker_id, ok=False,
            answer="", latency_ms=0, model=worker.model, error="no reply"
        )
    
    def _extract_question(self, messages: List[Dict]) -> str:
        """Извлекает последнее user сообщение как строку вопроса"""
        for msg in reversed(messages):
            if msg.get("role") == "user":
                content = msg.get("content", "")
                if isinstance(content, str):
                    return content
                elif isinstance(content, list):
                    texts = []
                    for item in content:
                        if isinstance(item, dict) and item.get("type") == "text":
                            texts.append(item.get("text", ""))
                    return "\n".join(texts)
        return "No question found"
    
    def _judge(self, results: Dict[str, AskResult]) -> tuple:
        """Выбирает лучший ответ из результатов"""
        ok_results = {wid: r for wid, r in results.items() if r.ok and r.answer.strip()}
        if not ok_results:
            for wid, r in results.items():
                if r.answer.strip():
                    return wid, r.answer.strip()
            return "", ""
        
        best = max(ok_results.items(), key=lambda x: len(x[1].answer.strip()) - (x[1].latency_ms // 100))
        return best[0], best[1].answer.strip()

# ── Singleton ───────────────────────────────────────────────────────────────

_backend: Optional[DecentralizedSwarmBackend] = None

def get_backend() -> DecentralizedSwarmBackend:
    global _backend
    if _backend is None:
        _backend = DecentralizedSwarmBackend()
    return _backend

# ── OpenAI-compatible API ───────────────────────────────────────────────────

@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    """OpenAI-compatible endpoint — принимает запросы от opencode"""
    body = await request.body()
    body_str = body.decode("utf-8") if body else "{}"
    
    try:
        body_json = json.loads(body_str)
    except:
        return JSONResponse(content={"error": "Invalid JSON"}, status_code=400)
    
    is_stream = body_json.get("stream", False)
    messages = body_json.get("messages", [])
    model = body_json.get("model", "swarm/default")
    
    log_event("REQUEST", {
        "model": model,
        "stream": is_stream,
        "messages_count": len(messages),
    })
    
    backend = get_backend()
    result = backend.fanout(messages, model)
    
    if result.get("status") == "error":
        error_msg = result.get("error", "Swarm error")
        log_event("ERROR", {"error": error_msg})
        return JSONResponse(content={"error": error_msg}, status_code=500)
    
    final_answer = result.get("final_answer", "")
    judge_choice = result.get("judge_choice", "")
    total_ms = result.get("total_ms", 0)
    
    log_event("RESPONSE", {
        "judge_choice": judge_choice,
        "total_ms": total_ms,
        "workers_used": result.get("workers_used", []),
        "answer_preview": final_answer[:200]
    })
    
    if is_stream:
        async def stream_generator() -> AsyncGenerator[str, None]:
            chunk_id = f"chatcmpl-{uuid.uuid4().hex}"
            
            # Role chunk
            yield f"data: {json.dumps({
                'id': chunk_id,
                'object': 'chat.completion.chunk',
                'created': int(time.time()),
                'model': f'swarm/{judge_choice}',
                'choices': [{'index': 0, 'delta': {'role': 'assistant'}, 'finish_reason': None}]
            })}\n\n"
            
            # Content chunks
            words = final_answer.split(" ")
            for word in words:
                yield f"data: {json.dumps({
                    'id': chunk_id,
                    'object': 'chat.completion.chunk',
                    'created': int(time.time()),
                    'model': f'swarm/{judge_choice}',
                    'choices': [{'index': 0, 'delta': {'content': word + " "}, 'finish_reason': None}]
                })}\n\n"
            
            # Finish chunk
            yield f"data: {json.dumps({
                'id': chunk_id,
                'object': 'chat.completion.chunk',
                'created': int(time.time()),
                'model': f'swarm/{judge_choice}',
                'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'stop'}]
            })}\n\n"
            
            yield "data: [DONE]\n\n"
        
        return StreamingResponse(
            stream_generator(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
        )
    else:
        response = {
            "id": f"chatcmpl-{uuid.uuid4().hex}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": f"swarm/{judge_choice}",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": final_answer,
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": sum(len(str(m.get("content", ""))) for m in messages) // 4,
                "completion_tokens": len(final_answer) // 4,
                "total_tokens": (sum(len(str(m.get("content", ""))) for m in messages) + len(final_answer)) // 4,
            },
            "swarm_metadata": {
                "judge_choice": judge_choice,
                "total_ms": total_ms,
                "workers_used": result.get("workers_used", []),
                "all_results": result.get("results", {}),
            }
        }
        
        return JSONResponse(content=response)

@app.get("/v1/models")
async def list_models():
    """Возвращает список доступных моделей (динамически)"""
    backend = get_backend()
    workers = backend.get_alive_workers()
    
    models = []
    for worker in workers:
        models.append({
            "id": f"swarm/{worker.role}",
            "object": "model",
            "created": int(time.time()),
            "owned_by": "swen-v4",
        })
    
    # Добавляем дефолтную модель
    models.append({
        "id": "swarm/default",
        "object": "model",
        "created": int(time.time()),
        "owned_by": "swen-v4",
    })
    
    return JSONResponse(content={"object": "list", "data": models})

@app.get("/health")
async def health():
    """Health check endpoint"""
    backend = get_backend()
    return JSONResponse(content=backend.get_worker_status())

@app.get("/v1/workers")
async def workers_status():
    """Возвращает детальный статус всех воркеров"""
    backend = get_backend()
    return JSONResponse(content=backend.get_worker_status())

# ── Startup / Shutdown ──────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    print("🚀 SwarmProvider v4 starting (Decentralized)...")
    backend = get_backend()
    if backend.connect():
        print(f"✅ Connected to decentralized swarm")
    else:
        print("⚠️  Failed to connect to swarm, will retry on first request")

@app.on_event("shutdown")
async def shutdown():
    print("🛑 SwarmProvider shutting down...")
    backend = get_backend()
    backend.disconnect()

# ── Main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  SWEN v4 Decentralized SwarmProvider")
    print("  Fully P2P — No Router Required")
    print("=" * 60)
    print(f"  Port: 8080")
    print(f"  Discovery: Multicast {MulticastDiscovery.__module__}")
    print(f"  Heartbeat: {HEARTBEAT_INTERVAL}s")
    print("=" * 60)
    uvicorn.run(app, host="0.0.0.0", port=8080)
