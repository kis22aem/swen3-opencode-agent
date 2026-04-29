#!/usr/bin/env python3
"""
SwarmProvider v3 — OpenAI-compatible провайдер для SWEN v3 роя
С ДИНАМИЧЕСКИМ отображением процесса мышления через Server-Sent Events
"""

import json
import os
import sys
import time
import uuid
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any, AsyncGenerator
from dataclasses import dataclass, field

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
import uvicorn

sys.path.insert(0, os.path.expanduser("~/.local/share/swen3"))
from agent import Swen3Agent, Swen3Config, AskResult

app = FastAPI(title="SWEN v3 SwarmProvider v3")

# ── Configuration ───────────────────────────────────────────────────────────

SWARM_WORKERS = ["qwen3_5_4b_opus", "jetson_gemma4b"]
ZENOH_CONNECT = ["tcp/10.15.64.226:7447", "tcp/10.15.66.12:7447"]
DEADLINE_MS = 60000
LOG_FILE = "/Users/alex/.local/share/swen3/swarm_provider.log"

# ── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class WorkerState:
    """Текущее состояние воркера"""
    worker_id: str
    role: str
    status: str = "unknown"
    last_seen: Optional[float] = None
    latency_ms: int = 0
    last_error: Optional[str] = None
    total_requests: int = 0
    successful_requests: int = 0

# ── Logging ─────────────────────────────────────────────────────────────────

def log_event(direction: str, data: dict):
    timestamp = datetime.now().isoformat()
    entry = {"timestamp": timestamp, "direction": direction, "data": data}
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

# ── Swarm Backend ───────────────────────────────────────────────────────────

class SwarmBackend:
    """Управляет подключением к рою, воркерами и fanout запросами"""
    
    def __init__(self):
        self.agent: Optional[Swen3Agent] = None
        self.connected = False
        self.worker_states: Dict[str, WorkerState] = {}
        self._init_worker_states()
        
    def _init_worker_states(self):
        for worker in SWARM_WORKERS:
            self.worker_states[worker] = WorkerState(
                worker_id=worker,
                role=worker,
                status="unknown"
            )
    
    def connect(self) -> bool:
        try:
            cfg = Swen3Config(
                zenoh_connect=ZENOH_CONNECT,
                roles=SWARM_WORKERS,
                deadline_ms=DEADLINE_MS,
            )
            self.agent = Swen3Agent(cfg)
            if self.agent.connect():
                self.connected = True
                time.sleep(1)
                self._check_workers()
                return True
            return False
        except Exception as e:
            print(f"[SwarmBackend] Connection error: {e}")
            return False
    
    def _check_workers(self):
        if not self.agent:
            return
        for worker in SWARM_WORKERS:
            try:
                result = self.agent.ask_one(worker, "ping", f"check-{time.time()}")
                state = self.worker_states[worker]
                if result.ok:
                    state.status = "online"
                    state.latency_ms = result.latency_ms
                    state.last_error = None
                else:
                    state.status = "error"
                    state.last_error = result.error
                state.last_seen = time.time()
            except Exception as e:
                self.worker_states[worker].status = "error"
                self.worker_states[worker].last_error = str(e)
    
    def disconnect(self):
        if self.agent:
            self.agent.disconnect()
            self.connected = False
    
    def ensure_connected(self) -> bool:
        if not self.connected:
            return self.connect()
        return True
    
    def get_worker_status(self) -> Dict[str, Any]:
        return {
            "swarm_connected": self.connected,
            "workers": {
                w: {
                    "status": s.status,
                    "latency_ms": s.latency_ms,
                    "last_seen": s.last_seen,
                    "last_error": s.last_error,
                    "total_requests": s.total_requests,
                    "successful_requests": s.successful_requests,
                }
                for w, s in self.worker_states.items()
            },
            "timestamp": datetime.now().isoformat(),
        }
    
    def _extract_question(self, messages: List[Dict]) -> str:
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
    
    def _judge_with_reasoning(self, results: Dict[str, AskResult]) -> tuple:
        ok_results = {w: r for w, r in results.items() if r.ok and r.answer.strip()}
        
        comparison = []
        for w, r in results.items():
            comp = {
                "worker": w,
                "ok": r.ok,
                "length": len(r.answer.strip()),
                "latency_ms": r.latency_ms,
                "score": 0,
                "winner": False,
            }
            if r.ok and r.answer.strip():
                comp["score"] = len(r.answer.strip()) - (r.latency_ms // 100)
            comparison.append(comp)
        
        if not ok_results:
            for w, r in results.items():
                if r.answer.strip():
                    reasoning = f"⚠️  Все воркеры вернули ошибки, но {w} дал непустой ответ."
                    return w, r.answer.strip(), reasoning, comparison
            reasoning = "❌ Все воркеры недоступны или вернули пустые ответы."
            return "", "", reasoning, comparison
        
        best_worker, best_result = max(
            ok_results.items(),
            key=lambda x: len(x[1].answer.strip()) - (x[1].latency_ms // 100)
        )
        
        for comp in comparison:
            if comp["worker"] == best_worker:
                comp["winner"] = True
        
        reasoning = f"""✅ Оценка ответов:
        
• {best_worker}: {len(best_result.answer.strip())} символов, {best_result.latency_ms}ms → выбран победителем
• Критерий: баланс между полнотой ответа и скоростью"""
        
        for w, r in ok_results.items():
            if w != best_worker:
                reasoning += f"\n• {w}: {len(r.answer.strip())} символов, {r.latency_ms}ms"
        
        return best_worker, best_result.answer.strip(), reasoning, comparison

# ── Singleton ───────────────────────────────────────────────────────────────

_backend: Optional[SwarmBackend] = None

def get_backend() -> SwarmBackend:
    global _backend
    if _backend is None:
        _backend = SwarmBackend()
    return _backend

# ── Streaming Generator ─────────────────────────────────────────────────────

async def stream_swarm_thinking(
    messages: List[Dict],
    model: str
) -> AsyncGenerator[str, None]:
    """
    Динамический генератор: отправляет этапы мышления по мере их выполнения.
    Использует Server-Sent Events формат.
    """
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
                'reasoning_content': '🤖 SWEN v3 — Начало мышления роя\n\n'
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
    
    # 4. Fanout начало
    yield f"data: {json.dumps({
        'id': chunk_id,
        'object': 'chat.completion.chunk',
        'created': int(time.time()),
        'model': 'swarm/processing',
        'choices': [{
            'index': 0,
            'delta': {
                'content': '',
                'reasoning_content': '────────────────────────────────────────\n📡 ЭТАП 1: Распределение запросов\n────────────────────────────────────────\n\n🚀 Отправка на воркеров:\n'
            },
            'finish_reason': None
        }]
    })}\n\n"
    
    for worker in SWARM_WORKERS:
        yield f"data: {json.dumps({
            'id': chunk_id,
            'object': 'chat.completion.chunk',
            'created': int(time.time()),
            'model': 'swarm/processing',
            'choices': [{
                'index': 0,
                'delta': {
                    'content': '',
                    'reasoning_content': f'  • {worker}...\n'
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
    
    # 5. Параллельный fanout с динамической отправкой результатов
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    results: Dict[str, AskResult] = {}
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=len(SWARM_WORKERS)) as pool:
        futures = {
            pool.submit(backend.agent.ask_one, worker, question, thread_id): worker
            for worker in SWARM_WORKERS
        }
        
        for future in as_completed(futures):
            worker = futures[future]
            try:
                result = future.result()
                results[worker] = result
                
                # Обновляем статистику
                state = backend.worker_states[worker]
                state.total_requests += 1
                if result.ok:
                    state.successful_requests += 1
                    state.status = "online"
                    state.latency_ms = result.latency_ms
                else:
                    state.status = "error"
                    state.last_error = result.error
                state.last_seen = time.time()
                
                # ДИНАМИЧЕСКИ отправляем результат
                icon = "✅" if result.ok else "❌"
                status_text = "Успех" if result.ok else "Ошибка"
                
                worker_text = f"\n{icon} Воркер: {worker}\n"
                worker_text += f"   Статус: {status_text}\n"
                worker_text += f"   Модель: {result.model}\n"
                worker_text += f"   Задержка: {result.latency_ms}ms\n"
                
                if result.ok:
                    preview = result.answer[:120].replace('\n', ' ')
                    worker_text += f"   Ответ: {preview}{'...' if len(result.answer) > 120 else ''}\n"
                else:
                    worker_text += f"   Ошибка: {result.error[:100]}\n"
                
                yield f"data: {json.dumps({
                    'id': chunk_id,
                    'object': 'chat.completion.chunk',
                    'created': int(time.time()),
                    'model': f'swarm/{worker}',
                    'choices': [{
                        'index': 0,
                        'delta': {
                            'content': '',
                            'reasoning_content': worker_text
                        },
                        'finish_reason': None
                    }]
                })}\n\n"
                
            except Exception as e:
                results[worker] = AskResult(
                    role=worker, worker_id="?", ok=False,
                    answer="", latency_ms=0, model="?", error=str(e)
                )
                yield f"data: {json.dumps({
                    'id': chunk_id,
                    'object': 'chat.completion.chunk',
                    'created': int(time.time()),
                    'model': f'swarm/{worker}',
                    'choices': [{
                        'index': 0,
                        'delta': {
                            'content': '',
                            'reasoning_content': f'\n❌ Воркер: {worker}\n   Ошибка: {str(e)[:100]}\n'
                        },
                        'finish_reason': None
                    }]
                })}\n\n"
    
    total_ms = int((time.time() - start_time) * 1000)
    
    # 6. Judging
    yield f"data: {json.dumps({
        'id': chunk_id,
        'object': 'chat.completion.chunk',
        'created': int(time.time()),
        'model': 'swarm/judging',
        'choices': [{
            'index': 0,
            'delta': {
                'content': '',
                'reasoning_content': '\n────────────────────────────────────────\n⚖️  ЭТАП 2: Оценка ответов (Judging)\n────────────────────────────────────────\n\n'
            },
            'finish_reason': None
        }]
    })}\n\n"
    
    best_worker, best_answer, judge_reasoning, comparison = backend._judge_with_reasoning(results)
    
    # Отправляем сравнение
    yield f"data: {json.dumps({
        'id': chunk_id,
        'object': 'chat.completion.chunk',
        'created': int(time.time()),
        'model': 'swarm/judging',
        'choices': [{
            'index': 0,
            'delta': {
                'content': '',
                'reasoning_content': judge_reasoning + '\n\nСравнение:\n'
            },
            'finish_reason': None
        }]
    })}\n\n"
    
    for comp in comparison:
        marker = "👑" if comp.get('winner') else "  "
        yield f"data: {json.dumps({
            'id': chunk_id,
            'object': 'chat.completion.chunk',
            'created': int(time.time()),
            'model': 'swarm/judging',
            'choices': [{
                'index': 0,
                'delta': {
                    'content': '',
                    'reasoning_content': f'{marker} {comp["worker"]}: {comp["score"]} баллов ({comp["length"]} символов, {comp["latency_ms"]}ms)\n'
                },
                'finish_reason': None
            }]
        })}\n\n"
    
    # 7. Финальный выбор
    successful = sum(1 for r in results.values() if r.ok)
    
    yield f"data: {json.dumps({
        'id': chunk_id,
        'object': 'chat.completion.chunk',
        'created': int(time.time()),
        'model': f'swarm/{best_worker}',
        'choices': [{
            'index': 0,
            'delta': {
                'content': '',
                'reasoning_content': f'\n────────────────────────────────────────\n🏆 ЭТАП 3: Финальный выбор\n────────────────────────────────────────\n\nВыбран воркер: {best_worker}\nОбщее время: {total_ms}ms\nУспешных воркеров: {successful}/{len(SWARM_WORKERS)}\n\n════════════════════════════════════════\n\n'
            },
            'finish_reason': None
        }]
    })}\n\n"
    
    # 8. Финальный ответ
    if best_answer:
        words = best_answer.split(" ")
        for word in words:
            yield f"data: {json.dumps({
                'id': chunk_id,
                'object': 'chat.completion.chunk',
                'created': int(time.time()),
                'model': f'swarm/{best_worker}',
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
        'model': f'swarm/{best_worker}',
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
    
    # Логируем запрос
    log_event("REQUEST", {
        "model": model,
        "stream": is_stream,
        "messages_count": len(messages),
    })
    
    if is_stream:
        # ДИНАМИЧЕСКИЙ streaming с промежуточными этапами
        return StreamingResponse(
            stream_swarm_thinking(messages, model),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            }
        )
    else:
        # Non-streaming: собираем всё и возвращаем
        backend = get_backend()
        question = backend._extract_question(messages)
        thread_id = f"swarm-{uuid.uuid4().hex[:8]}"
        
        if not backend.ensure_connected():
            return JSONResponse(content={"error": "Not connected to swarm"}, status_code=500)
        
        # Fanout
        from concurrent.futures import ThreadPoolExecutor, as_completed
        results: Dict[str, AskResult] = {}
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=len(SWARM_WORKERS)) as pool:
            futures = {
                pool.submit(backend.agent.ask_one, worker, question, thread_id): worker
                for worker in SWARM_WORKERS
            }
            for future in as_completed(futures):
                worker = futures[future]
                try:
                    result = future.result()
                    results[worker] = result
                except Exception as e:
                    results[worker] = AskResult(
                        role=worker, worker_id="?", ok=False,
                        answer="", latency_ms=0, model="?", error=str(e)
                    )
        
        total_ms = int((time.time() - start_time) * 1000)
        best_worker, best_answer, judge_reasoning, comparison = backend._judge_with_reasoning(results)
        
        # Формируем reasoning текст
        reasoning_text = f"""🤖 SWEN v3 — Процесс мышления роя

📋 Вопрос: {question[:80]}{"..." if len(question) > 80 else ""}
🧵 Thread: {thread_id}

────────────────────────────────────────
📡 ЭТАП 1: Распределение запросов
────────────────────────────────────────

🚀 Отправка на {len(SWARM_WORKERS)} воркеров:
"""
        for worker in SWARM_WORKERS:
            reasoning_text += f"  • {worker}\n"
        
        reasoning_text += "\n"
        for worker, result in results.items():
            icon = "✅" if result.ok else "❌"
            reasoning_text += f"\n{icon} Воркер: {worker}\n"
            reasoning_text += f"   Модель: {result.model}\n"
            reasoning_text += f"   Задержка: {result.latency_ms}ms\n"
            if result.ok:
                preview = result.answer[:120].replace('\n', ' ')
                reasoning_text += f"   Ответ: {preview}{'...' if len(result.answer) > 120 else ''}\n"
            else:
                reasoning_text += f"   Ошибка: {result.error[:100]}\n"
        
        reasoning_text += f"""
────────────────────────────────────────
⚖️  ЭТАП 2: Оценка ответов (Judging)
────────────────────────────────────────

{judge_reasoning}

Сравнение:
"""
        for comp in comparison:
            marker = "👑" if comp.get('winner') else "  "
            reasoning_text += f"{marker} {comp['worker']}: {comp['score']} баллов ({comp['length']} символов, {comp['latency_ms']}ms)\n"
        
        successful = sum(1 for r in results.values() if r.ok)
        reasoning_text += f"""
────────────────────────────────────────
🏆 ЭТАП 3: Финальный выбор
────────────────────────────────────────

Выбран воркер: {best_worker}
Общее время: {total_ms}ms
Успешных воркеров: {successful}/{len(SWARM_WORKERS)}

════════════════════════════════════════
"""
        
        response = {
            "id": f"chatcmpl-{uuid.uuid4().hex}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": f"swarm/{best_worker}",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": best_answer,
                    "reasoning_content": reasoning_text,
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": sum(len(str(m.get("content", ""))) for m in messages) // 4,
                "completion_tokens": len(best_answer) // 4,
                "total_tokens": (sum(len(str(m.get("content", ""))) for m in messages) + len(best_answer)) // 4,
            },
            "swarm_metadata": {
                "judge_choice": best_worker,
                "total_ms": total_ms,
                "workers_used": [w for w, r in results.items() if r.ok],
                "all_results": {
                    w: {
                        "ok": r.ok,
                        "answer": r.answer,
                        "latency_ms": r.latency_ms,
                        "model": r.model,
                        "error": r.error
                    }
                    for w, r in results.items()
                },
            }
        }
        
        return JSONResponse(content=response)


@app.get("/v1/models")
async def list_models():
    """Возвращает список доступных моделей роя"""
    models = []
    for worker in SWARM_WORKERS:
        models.append({
            "id": f"swarm/{worker}",
            "object": "model",
            "created": int(time.time()),
            "owned_by": "swen-v3",
        })
    models.append({
        "id": "swarm/default",
        "object": "model",
        "created": int(time.time()),
        "owned_by": "swen-v3",
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


@app.post("/v1/workers/check")
async def check_workers():
    """Принудительная проверка доступности воркеров"""
    backend = get_backend()
    backend._check_workers()
    return JSONResponse(content=backend.get_worker_status())


# ── Startup / Shutdown ──────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    print("🚀 SwarmProvider v3 starting...")
    backend = get_backend()
    # Pre-connect in a thread to avoid blocking startup
    import threading
    def connect_bg():
        if backend.connect():
            print(f"✅ Connected to SWEN v3 swarm")
            print(f"   Workers: {SWARM_WORKERS}")
        else:
            print("⚠️  Failed to connect to swarm, will retry on first request")
    threading.Thread(target=connect_bg, daemon=True).start()


@app.on_event("shutdown")
async def shutdown():
    print("🛑 SwarmProvider shutting down...")
    backend = get_backend()
    backend.disconnect()


# ── Main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  SWEN v3 SwarmProvider v3")
    print("  Dynamic Thinking Streaming")
    print("=" * 60)
    print(f"  Port: 8080")
    print(f"  Workers: {SWARM_WORKERS}")
    print(f"  Zenoh: {ZENOH_CONNECT}")
    print("=" * 60)
    uvicorn.run(app, host="0.0.0.0", port=8080)
