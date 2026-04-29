#!/usr/bin/env python3
"""
SwarmProvider v4 — полностью децентрализованный рой
Поддержка ОБОИХ режимов:
1. Multicast discovery (для v4 воркеров)
2. Zenoh mesh (для v3 воркеров — обратная совместимость)
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

app = FastAPI(title="SWEN v4 Hybrid SwarmProvider")

# ── Configuration ───────────────────────────────────────────────────────────

# v3 Zenoh воркеры (обратная совместимость)
ZENOH_WORKERS = ["qwen3_5_4b_opus", "jetson_gemma4b", "macbook_huihui_qwen3_5_2b"]
ZENOH_CONNECT = ["tcp/10.15.64.226:7447", "tcp/10.15.66.12:7447"]

DEADLINE_MS = 60000
LOG_FILE = "/Users/alex/.local/share/swen3/swarm_provider.log"

# ── Logging ─────────────────────────────────────────────────────────────────

def log_event(direction: str, data: dict):
    timestamp = datetime.now().isoformat()
    entry = {"timestamp": timestamp, "direction": direction, "data": data}
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

# ── Hybrid Swarm Backend ────────────────────────────────────────────────────

class HybridSwarmBackend:
    """Гибридный бэкенд: v4 multicast + v3 Zenoh mesh"""
    
    def __init__(self):
        self.agent: Optional[Swen3Agent] = None
        self.discovery: Optional[MulticastDiscovery] = None
        self.connected = False
        self.worker_states: Dict[str, dict] = {}
        
    def connect(self) -> bool:
        """Подключение к обоим режимам"""
        try:
            # 1. Подключаемся к Zenoh mesh (для v3 воркеров)
            cfg = Swen3Config(
                zenoh_connect=ZENOH_CONNECT,
                roles=ZENOH_WORKERS,
                deadline_ms=DEADLINE_MS,
            )
            self.agent = Swen3Agent(cfg)
            
            # Подключаемся к Zenoh
            import zenoh
            zcfg = zenoh.Config()
            zcfg.insert_json5("mode", '"peer"')
            if ZENOH_CONNECT:
                zcfg.insert_json5("connect/endpoints", json.dumps(ZENOH_CONNECT))
            zcfg.insert_json5("scouting/multicast/enabled", "false")
            zcfg.insert_json5("scouting/gossip/enabled", "true")
            
            self.agent.session = zenoh.open(zcfg)
            self.agent._connected = True
            
            # 2. Запускаем multicast discovery (для v4 воркеров)
            import socket
            hostname = socket.gethostname()
            try:
                local_ip = socket.getaddrinfo(hostname, None, socket.AF_INET)[0][4][0]
            except:
                local_ip = "127.0.0.1"
            
            self.discovery = MulticastDiscovery(
                worker_id=f"swarm-provider-{uuid.uuid4().hex[:8]}",
                role="provider",
                model="swarm-provider",
                zenoh_endpoint=f"tcp/{local_ip}:0"
            )
            self.discovery.start()
            
            # 3. Ждём обнаружения
            time.sleep(3)
            
            self.connected = True
            
            # Считаем воркеров
            v3_workers = len(ZENOH_WORKERS)
            v4_workers = len(self.discovery.get_alive_workers())
            total = v3_workers + v4_workers
            
            print(f"✅ Connected to hybrid swarm")
            print(f"   v3 Zenoh workers: {v3_workers}")
            print(f"   v4 multicast workers: {v4_workers}")
            print(f"   Total: {total}")
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
    
    def get_all_workers(self) -> List[dict]:
        """Возвращает всех воркеров (v3 + v4)"""
        workers = []
        
        # v3 Zenoh воркеры
        for role in ZENOH_WORKERS:
            workers.append({
                "worker_id": role,
                "role": role,
                "type": "zenoh_v3",
                "status": "configured"
            })
        
        # v4 multicast воркеры
        if self.discovery:
            for w in self.discovery.get_alive_workers():
                workers.append({
                    "worker_id": w.worker_id,
                    "role": w.role,
                    "type": "multicast_v4",
                    "status": "online" if w.is_alive else "dead",
                    "host": w.host,
                    "model": w.model
                })
        
        return workers
    
    def get_worker_status(self) -> Dict[str, Any]:
        """Возвращает текущее состояние всех воркеров"""
        workers = self.get_all_workers()
        v3_count = sum(1 for w in workers if w["type"] == "zenoh_v3")
        v4_count = sum(1 for w in workers if w["type"] == "multicast_v4")
        
        return {
            "swarm_connected": self.connected,
            "total_workers": len(workers),
            "v3_zenoh_workers": v3_count,
            "v4_multicast_workers": v4_count,
            "workers": {
                w["worker_id"]: {
                    "role": w["role"],
                    "type": w["type"],
                    "status": w.get("status", "unknown")
                }
                for w in workers
            },
            "timestamp": datetime.now().isoformat(),
        }
    
    def fanout(self, messages: List[Dict], model: str) -> Dict[str, Any]:
        """
        Отправляет сообщения на ВСЕХ воркеров (v3 + v4) параллельно.
        """
        if not self.ensure_connected():
            return {"error": "Not connected to swarm", "status": "error"}
        
        question = self._extract_question(messages)
        thread_id = f"swarm-{uuid.uuid4().hex[:8]}"
        
        # Собираем всех воркеров
        all_workers = self.get_all_workers()
        if not all_workers:
            return {"error": "No workers available", "status": "error"}
        
        print(f"[SwarmProvider] Fanout → {len(all_workers)} workers")
        print(f"[SwarmProvider] Question: {question[:80]}...")
        
        # Параллельный fanout
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        results: Dict[str, AskResult] = {}
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=len(all_workers)) as pool:
            futures = {}
            
            # v3 Zenoh воркеры
            for worker in all_workers:
                if worker["type"] == "zenoh_v3":
                    future = pool.submit(
                        self._ask_zenoh_worker, 
                        worker["role"], 
                        question, 
                        thread_id
                    )
                    futures[future] = worker
                
                # v4 multicast воркеры
                elif worker["type"] == "multicast_v4":
                    # Для v4 пока используем Zenoh напрямую
                    # (в будущем можно добавить HTTP API)
                    future = pool.submit(
                        self._ask_zenoh_worker,
                        worker["role"],
                        question,
                        thread_id
                    )
                    futures[future] = worker
            
            for future in as_completed(futures):
                worker = futures[future]
                try:
                    result = future.result()
                    worker_id = worker["worker_id"]
                    results[worker_id] = result
                    icon = "✅" if result.ok else "❌"
                    print(f"[SwarmProvider] {icon} {worker_id} ({result.latency_ms}ms)")
                except Exception as e:
                    worker_id = worker["worker_id"]
                    results[worker_id] = AskResult(
                        role=worker["role"], 
                        worker_id=worker_id, 
                        ok=False,
                        answer="", 
                        latency_ms=0, 
                        model="?", 
                        error=str(e)
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
    
    def _ask_zenoh_worker(self, role: str, question: str, thread_id: str) -> AskResult:
        """Отправляет вопрос воркеру через Zenoh"""
        if not self.agent or not self.agent.session:
            return AskResult(
                role=role, worker_id=role, ok=False,
                answer="", latency_ms=0, model="?", error="No Zenoh session"
            )
        
        # Используем существующий ask_one из agent.py
        return self.agent.ask_one(role, question, thread_id)
    
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

_backend: Optional[HybridSwarmBackend] = None

def get_backend() -> HybridSwarmBackend:
    global _backend
    if _backend is None:
        _backend = HybridSwarmBackend()
    return _backend

# ── Streaming Generator ─────────────────────────────────────────────────────

async def stream_swarm_thinking(
    messages: List[Dict],
    model: str
) -> AsyncGenerator[str, None]:
    """Динамический генератор: отправляет этапы мышления по мере их выполнения."""
    backend = get_backend()
    chunk_id = f"chatcmpl-{uuid.uuid4().hex}"
    
    # 1. Начальный chunk с ролью
    yield f"data: {json.dumps({
        'id': chunk_id,
        'object': 'chat.completion.chunk',
        'created': int(time.time()),
        'model': 'swarm/processing',
        'choices': [{
            'index': 0,
            'delta': {'role': 'assistant'},
            'finish_reason': None
        }]
    })}\n\n"
    
    # 2. Подключение к рою
    yield f"data: {json.dumps({
        'id': chunk_id,
        'object': 'chat.completion.chunk',
        'created': int(time.time()),
        'model': 'swarm/processing',
        'choices': [{
            'index': 0,
            'delta': {
                'content': '',
                'reasoning_content': '🤖 SWEN v4 Hybrid Swarm — Начало мышления роя\n\n'
            },
            'finish_reason': None
        }]
    })}\n\n"
    
    if not backend.ensure_connected():
        yield f"data: {json.dumps({
            'id': chunk_id,
            'object': 'chat.completion.chunk',
            'created': int(time.time()),
            'model': 'swarm/error',
            'choices': [{
                'index': 0,
                'delta': {
                    'content': '',
                    'reasoning_content': '❌ Ошибка: Не удалось подключиться к рою\n'
                },
                'finish_reason': 'stop'
            }]
        })}\n\n"
        yield "data: [DONE]\n\n"
        return
    
    # 3. Извлечение вопроса
    question = backend._extract_question(messages)
    thread_id = f"swarm-{uuid.uuid4().hex[:8]}"
    
    yield f"data: {json.dumps({
        'id': chunk_id,
        'object': 'chat.completion.chunk',
        'created': int(time.time()),
        'model': 'swarm/processing',
        'choices': [{
            'index': 0,
            'delta': {
                'content': '',
                'reasoning_content': f'📋 Вопрос: {question[:80]}{"..." if len(question) > 80 else ""}\n🧵 Thread: {thread_id}\n\n'
            },
            'finish_reason': None
        }]
    })}\n\n"
    
    # 4. Получаем список воркеров
    all_workers = backend.get_all_workers()
    
    yield f"data: {json.dumps({
        'id': chunk_id,
        'object': 'chat.completion.chunk',
        'created': int(time.time()),
        'model': 'swarm/processing',
        'choices': [{
            'index': 0,
            'delta': {
                'content': '',
                'reasoning_content': f'────────────────────────────────────────\n📡 ЭТАП 1: Распределение запросов\n────────────────────────────────────────\n\n🚀 Отправка на {len(all_workers)} воркеров:\n'
            },
            'finish_reason': None
        }]
    })}\n\n"
    
    for worker in all_workers:
        worker_type = "🌐" if worker["type"] == "zenoh_v3" else "📡"
        yield f"data: {json.dumps({
            'id': chunk_id,
            'object': 'chat.completion.chunk',
            'created': int(time.time()),
            'model': 'swarm/processing',
            'choices': [{
                'index': 0,
                'delta': {
                    'content': '',
                    'reasoning_content': f'  {worker_type} {worker["worker_id"]} ({worker["type"]})\n'
                },
                'finish_reason': None
            }]
        })}\n\n"
    
    yield f"data: {json.dumps({
        'id': chunk_id,
        'object': 'chat.completion.chunk',
        'created': int(time.time()),
        'model': 'swarm/processing',
        'choices': [{
            'index': 0,
            'delta': {
                'content': '',
                'reasoning_content': '\n⏳ Ожидание ответов...\n\n'
            },
            'finish_reason': None
        }]
    })}\n\n"
    
    # 5. Выполняем fanout
    result = backend.fanout(messages, model)
    
    if result.get("status") == "error":
        yield f"data: {json.dumps({
            'id': chunk_id,
            'object': 'chat.completion.chunk',
            'created': int(time.time()),
            'model': 'swarm/error',
            'choices': [{
                'index': 0,
                'delta': {
                    'content': '',
                    'reasoning_content': f'❌ Ошибка: {result.get("error", "Unknown")}\n'
                },
                'finish_reason': 'stop'
            }]
        })}\n\n"
        yield "data: [DONE]\n\n"
        return
    
    # 6. Отображаем результаты воркеров
    for worker_id, worker_result in result.get("results", {}).items():
        icon = "✅" if worker_result.get("ok") else "❌"
        latency = worker_result.get("latency_ms", 0)
        model_name = worker_result.get("model", "?")
        
        worker_text = f"\n{icon} Воркер: {worker_id}\n"
        worker_text += f"   Модель: {model_name}\n"
        worker_text += f"   Задержка: {latency}ms\n"
        
        if worker_result.get("ok"):
            preview = worker_result.get("answer", "")[:120].replace("\n", " ")
            worker_text += f"   Ответ: {preview}{'...' if len(worker_result.get('answer', '')) > 120 else ''}\n"
        else:
            worker_text += f"   Ошибка: {worker_result.get('error', 'Unknown')[:100]}\n"
        
        yield f"data: {json.dumps({
            'id': chunk_id,
            'object': 'chat.completion.chunk',
            'created': int(time.time()),
            'model': f'swarm/{worker_id}',
            'choices': [{
                'index': 0,
                'delta': {
                    'content': '',
                    'reasoning_content': worker_text
                },
                'finish_reason': None
            }]
        })}\n\n"
    
    # 7. Judging
    judge_choice = result.get("judge_choice", "")
    total_ms = result.get("total_ms", 0)
    successful = len(result.get("workers_used", []))
    total_workers = len(result.get("results", {}))
    
    yield f"data: {json.dumps({
        'id': chunk_id,
        'object': 'chat.completion.chunk',
        'created': int(time.time()),
        'model': f'swarm/{judge_choice}',
        'choices': [{
            'index': 0,
            'delta': {
                'content': '',
                'reasoning_content': f'\n────────────────────────────────────────\n🏆 ЭТАП 2: Финальный выбор\n────────────────────────────────────────\n\nВыбран воркер: {judge_choice}\nОбщее время: {total_ms}ms\nУспешных воркеров: {successful}/{total_workers}\n\n════════════════════════════════════════\n\n'
            },
            'finish_reason': None
        }]
    })}\n\n"
    
    # 8. Финальный ответ
    final_answer = result.get("final_answer", "")
    if final_answer:
        words = final_answer.split(" ")
        for word in words:
            yield f"data: {json.dumps({
                'id': chunk_id,
                'object': 'chat.completion.chunk',
                'created': int(time.time()),
                'model': f'swarm/{judge_choice}',
                'choices': [{
                    'index': 0,
                    'delta': {'content': word + " "},
                    'finish_reason': None
                }]
            })}\n\n"
    
    # 9. Finish
    yield f"data: {json.dumps({
        'id': chunk_id,
        'object': 'chat.completion.chunk',
        'created': int(time.time()),
        'model': f'swarm/{judge_choice}',
        'choices': [{
            'index': 0,
            'delta': {},
            'finish_reason': 'stop'
        }]
    })}\n\n"
    
    yield "data: [DONE]\n\n"

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
    
    if is_stream:
        return StreamingResponse(
            stream_swarm_thinking(messages, model),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"}
        )
    else:
        backend = get_backend()
        result = backend.fanout(messages, model)
        
        if result.get("status") == "error":
            return JSONResponse(content={"error": result.get("error", "Swarm error")}, status_code=500)
        
        response = {
            "id": f"chatcmpl-{uuid.uuid4().hex}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": f"swarm/{result.get('judge_choice', 'unknown')}",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": result.get("final_answer", ""),
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": sum(len(str(m.get("content", ""))) for m in messages) // 4,
                "completion_tokens": len(result.get("final_answer", "")) // 4,
                "total_tokens": (sum(len(str(m.get("content", ""))) for m in messages) + len(result.get("final_answer", ""))) // 4,
            },
            "swarm_metadata": {
                "judge_choice": result.get("judge_choice"),
                "total_ms": result.get("total_ms"),
                "workers_used": result.get("workers_used", []),
                "all_results": result.get("results", {}),
            }
        }
        
        return JSONResponse(content=response)

@app.get("/v1/models")
async def list_models():
    """Возвращает список доступных моделей (динамически)"""
    backend = get_backend()
    workers = backend.get_all_workers()
    
    models = []
    for worker in workers:
        models.append({
            "id": f"swarm/{worker['worker_id']}",
            "object": "model",
            "created": int(time.time()),
            "owned_by": "swen-v4",
        })
    
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
    print("🚀 SwarmProvider v4 Hybrid starting...")
    backend = get_backend()
    if backend.connect():
        print(f"✅ Connected to hybrid swarm")
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
    print("  SWEN v4 Hybrid SwarmProvider")
    print("  v3 Zenoh + v4 Multicast")
    print("=" * 60)
    print(f"  Port: 8080")
    print(f"  v3 Workers: {ZENOH_WORKERS}")
    print(f"  v4 Discovery: Multicast")
    print("=" * 60)
    uvicorn.run(app, host="0.0.0.0", port=8080)
