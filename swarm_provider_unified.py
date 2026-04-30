#!/usr/bin/env python3
"""
SWEN v4 Unified SwarmProvider
Единый провайдер со встроенными HTTP воркерами (локальные + облачные)
Discovery: динамически через OpenRouter API + TOML конфиг
"""

import json
import os
import sys
import time
import uuid
import urllib.request
import asyncio
import threading
from datetime import datetime
from typing import Dict, List, Optional, Any, AsyncGenerator
from dataclasses import dataclass, field

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
import uvicorn

# ── Configuration ───────────────────────────────────────────────────────────

DEADLINE_MS = 60000
LOG_FILE = "/Users/alex/.local/share/swen3/swarm_provider.log"

OPENROUTER_API_KEY = "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"
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

app = FastAPI(title="SWEN v4 Unified SwarmProvider")

# ── Data Structures ─────────────────────────────────────────────────────────

@dataclass
class WorkerConfig:
    """Конфигурация воркера"""
    id: str
    name: str
    model: str
    provider: str  # "openrouter", "local", "groq", etc.
    base_url: Optional[str] = None  # для локальных моделей
    api_key: Optional[str] = None   # для облачных моделей
    enabled: bool = True
    
@dataclass  
class AskResult:
    """Результат запроса к воркеру"""
    worker_id: str
    ok: bool
    answer: str
    latency_ms: int
    model: str
    error: Optional[str] = None

# ── Worker Registry ─────────────────────────────────────────────────────────

class WorkerRegistry:
    """Реестр всех воркеров (локальных и облачных)"""
    
    def __init__(self):
        self.workers: Dict[str, WorkerConfig] = {}
        self._lock = threading.Lock()
        
    def load_from_openrouter(self) -> List[WorkerConfig]:
        """Загружает список бесплатных моделей из OpenRouter"""
        try:
            req = urllib.request.Request(
                f"{OPENROUTER_BASE_URL}/models",
                headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}"}
            )
            
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
            
            workers = []
            for model in data.get("data", []):
                pricing = model.get("pricing", {})
                # Берём только бесплатные модели
                if pricing.get("prompt") == "0" and pricing.get("completion") == "0":
                    model_id = model["id"]
                    worker = WorkerConfig(
                        id=f"or-{model_id.replace('/', '-').replace(':', '-')}",
                        name=model.get("name", model_id),
                        model=model_id,
                        provider="openrouter",
                        api_key=OPENROUTER_API_KEY
                    )
                    workers.append(worker)
            
            print(f"✅ Loaded {len(workers)} free models from OpenRouter")
            return workers
            
        except Exception as e:
            print(f"❌ Failed to load OpenRouter models: {e}")
            return []
    
    def load_from_toml(self, path: str) -> List[WorkerConfig]:
        """Загружает воркеров из TOML конфига"""
        try:
            import tomllib
            
            with open(path, "rb") as f:
                config = tomllib.load(f)
            
            workers = []
            for worker_data in config.get("workers", []):
                worker = WorkerConfig(
                    id=worker_data["id"],
                    name=worker_data.get("name", worker_data["id"]),
                    model=worker_data["model"],
                    provider=worker_data.get("provider", "openrouter"),
                    base_url=worker_data.get("base_url"),
                    api_key=worker_data.get("api_key"),
                    enabled=worker_data.get("enabled", True)
                )
                workers.append(worker)
            
            print(f"✅ Loaded {len(workers)} workers from TOML")
            return workers
            
        except Exception as e:
            print(f"❌ Failed to load TOML config: {e}")
            return []
    
    def add_worker(self, worker: WorkerConfig):
        """Добавляет воркера в реестр"""
        with self._lock:
            self.workers[worker.id] = worker
    
    def get_enabled_workers(self) -> List[WorkerConfig]:
        """Возвращает список активных воркеров"""
        with self._lock:
            return [w for w in self.workers.values() if w.enabled]
    
    def get_worker(self, worker_id: str) -> Optional[WorkerConfig]:
        """Возвращает воркера по ID"""
        with self._lock:
            return self.workers.get(worker_id)

# ── API Clients ─────────────────────────────────────────────────────────────

def ask_openrouter(question: str, model: str, api_key: str) -> dict:
    """Отправляет вопрос в OpenRouter API"""
    try:
        api_req = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You are a helpful assistant. Be concise."},
                {"role": "user", "content": question}
            ],
            "temperature": 0.7,
            "max_tokens": 1024,
        }
        
        req_data = json.dumps(api_req).encode()
        req = urllib.request.Request(
            f"{OPENROUTER_BASE_URL}/chat/completions",
            data=req_data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
                "HTTP-Referer": "https://swen-v4.local",
                "X-Title": "SWEN v4"
            },
            method="POST"
        )
        
        start = time.time()
        with urllib.request.urlopen(req, timeout=30) as resp:
            api_resp = json.loads(resp.read().decode())
        
        latency_ms = int((time.time() - start) * 1000)
        answer = api_resp["choices"][0]["message"]["content"]
        
        return {
            "ok": True,
            "answer": answer,
            "latency_ms": latency_ms,
            "model": model,
            "error": None
        }
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()
        return {
            "ok": False,
            "answer": "",
            "latency_ms": 0,
            "model": model,
            "error": f"HTTP {e.code}: {error_body[:200]}",
            "fatal": e.code in [401, 403]
        }
    except Exception as e:
        return {
            "ok": False,
            "answer": "",
            "latency_ms": 0,
            "model": model,
            "error": str(e),
            "fatal": False
        }

def ask_local(question: str, model: str, base_url: str) -> dict:
    """Отправляет вопрос в локальную модель через HTTP"""
    try:
        api_req = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You are a helpful assistant. Be concise."},
                {"role": "user", "content": question}
            ],
            "temperature": 0.7,
            "max_tokens": 1024,
        }
        
        req_data = json.dumps(api_req).encode()
        req = urllib.request.Request(
            f"{base_url}/v1/chat/completions",
            data=req_data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        
        start = time.time()
        with urllib.request.urlopen(req, timeout=30) as resp:
            api_resp = json.loads(resp.read().decode())
        
        latency_ms = int((time.time() - start) * 1000)
        answer = api_resp["choices"][0]["message"]["content"]
        
        return {
            "ok": True,
            "answer": answer,
            "latency_ms": latency_ms,
            "model": model,
            "error": None
        }
    except Exception as e:
        return {
            "ok": False,
            "answer": "",
            "latency_ms": 0,
            "model": model,
            "error": str(e),
            "fatal": False
        }

# ── Swarm Backend ───────────────────────────────────────────────────────────

class UnifiedSwarmBackend:
    """Единый бэкенд для всех воркеров"""
    
    def __init__(self):
        self.registry = WorkerRegistry()
        self.connected = False
        
    def connect(self) -> bool:
        """Инициализация — загрузка воркеров"""
        try:
            # 1. Загружаем из TOML (если есть)
            toml_path = os.path.expanduser("~/.config/swen/v4/workers.toml")
            if os.path.exists(toml_path):
                workers = self.registry.load_from_toml(toml_path)
                for w in workers:
                    self.registry.add_worker(w)
            
            # 2. Загружаем бесплатные модели из OpenRouter
            or_workers = self.registry.load_from_openrouter()
            for w in or_workers:
                # Добавляем только если ещё нет
                if w.id not in self.registry.workers:
                    self.registry.add_worker(w)
            
            self.connected = True
            enabled = len(self.registry.get_enabled_workers())
            print(f"✅ SwarmProvider ready with {enabled} workers")
            return True
            
        except Exception as e:
            print(f"❌ Connection error: {e}")
            return False
    
    def disconnect(self):
        self.connected = False
    
    def ensure_connected(self) -> bool:
        if not self.connected:
            return self.connect()
        return True
    
    def get_worker_status(self) -> Dict[str, Any]:
        """Возвращает статус всех воркеров"""
        workers = self.registry.get_enabled_workers()
        return {
            "swarm_connected": self.connected,
            "worker_count": len(workers),
            "workers": {
                w.id: {
                    "name": w.name,
                    "model": w.model,
                    "provider": w.provider,
                    "enabled": w.enabled
                }
                for w in workers
            },
            "timestamp": datetime.now().isoformat(),
        }
    
    def fanout(self, messages: List[Dict], model: str) -> Dict[str, Any]:
        """Отправляет сообщения на ВСЕХ воркеров параллельно"""
        if not self.ensure_connected():
            return {"error": "Not connected", "status": "error"}
        
        workers = self.registry.get_enabled_workers()
        if not workers:
            return {"error": "No workers available", "status": "error"}
        
        question = self._extract_question(messages)
        thread_id = f"swarm-{uuid.uuid4().hex[:8]}"
        
        print(f"[SwarmProvider] Fanout → {len(workers)} workers")
        print(f"[SwarmProvider] Question: {question[:80]}...")
        
        # Параллельный fanout
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        results: Dict[str, AskResult] = {}
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=len(workers)) as pool:
            futures = {
                pool.submit(self._ask_worker, worker, question): worker
                for worker in workers
            }
            for future in as_completed(futures):
                worker = futures[future]
                try:
                    result = future.result()
                    results[worker.id] = result
                    icon = "✅" if result.ok else "❌"
                    print(f"[SwarmProvider] {icon} {worker.id} ({result.latency_ms}ms)")
                except Exception as e:
                    results[worker.id] = AskResult(
                        worker_id=worker.id, ok=False,
                        answer="", latency_ms=0, model=worker.model, error=str(e)
                    )
        
        total_ms = int((time.time() - start_time) * 1000)
        
        # Judge
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
    
    def _ask_worker(self, worker: WorkerConfig, question: str) -> AskResult:
        """Отправляет вопрос воркеру"""
        if worker.provider == "openrouter":
            result = ask_openrouter(question, worker.model, worker.api_key or OPENROUTER_API_KEY)
        elif worker.provider == "local":
            result = ask_local(question, worker.model, worker.base_url or "http://localhost:8000")
        else:
            return AskResult(
                worker_id=worker.id, ok=False, answer="",
                latency_ms=0, model=worker.model, error=f"Unknown provider: {worker.provider}"
            )
        
        return AskResult(
            worker_id=worker.id,
            ok=result["ok"],
            answer=result["answer"],
            latency_ms=result["latency_ms"],
            model=result["model"],
            error=result.get("error")
        )
    
    def _extract_question(self, messages: List[Dict]) -> str:
        """Извлекает последнее user сообщение"""
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
        """Выбирает лучший ответ"""
        ok_results = {wid: r for wid, r in results.items() if r.ok and r.answer and r.answer.strip()}
        if not ok_results:
            for wid, r in results.items():
                if r.answer and r.answer.strip():
                    return wid, r.answer.strip()
            return "", ""
        
        best = max(ok_results.items(), key=lambda x: len(x[1].answer.strip()) - (x[1].latency_ms // 100))
        return best[0], best[1].answer.strip()

# ── Singleton ───────────────────────────────────────────────────────────────

_backend: Optional[UnifiedSwarmBackend] = None

def get_backend() -> UnifiedSwarmBackend:
    global _backend
    if _backend is None:
        _backend = UnifiedSwarmBackend()
    return _backend

# ── Logging ─────────────────────────────────────────────────────────────────

def log_event(direction: str, data: dict):
    timestamp = datetime.now().isoformat()
    entry = {"timestamp": timestamp, "direction": direction, "data": data}
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

# ── Streaming Generator ─────────────────────────────────────────────────────

async def stream_swarm_thinking(
    messages: List[Dict],
    model: str
) -> AsyncGenerator[str, None]:
    """Динамический генератор: отправляет этапы мышления"""
    backend = get_backend()
    chunk_id = f"chatcmpl-{uuid.uuid4().hex}"
    
    # 1. Начальный chunk
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
    
    # 2. Подключение
    yield f"data: {json.dumps({
        'id': chunk_id,
        'object': 'chat.completion.chunk',
        'created': int(time.time()),
        'model': 'swarm/processing',
        'choices': [{
            'index': 0,
            'delta': {
                'content': '',
                'reasoning_content': '🤖 SWEN v4 Unified — Начало мышления роя\n\n'
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
                    'reasoning_content': '❌ Ошибка подключения\n'
                },
                'finish_reason': 'stop'
            }]
        })}\n\n"
        yield "data: [DONE]\n\n"
        return
    
    # 3. Вопрос
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
    
    # 4. Список воркеров
    workers = backend.registry.get_enabled_workers()
    
    yield f"data: {json.dumps({
        'id': chunk_id,
        'object': 'chat.completion.chunk',
        'created': int(time.time()),
        'model': 'swarm/processing',
        'choices': [{
            'index': 0,
            'delta': {
                'content': '',
                'reasoning_content': f'────────────────────────────────────────\n📡 ЭТАП 1: Распределение запросов ({len(workers)} воркеров)\n────────────────────────────────────────\n\n'
            },
            'finish_reason': None
        }]
    })}\n\n"
    
    for worker in workers:
        yield f"data: {json.dumps({
            'id': chunk_id,
            'object': 'chat.completion.chunk',
            'created': int(time.time()),
            'model': 'swarm/processing',
            'choices': [{
                'index': 0,
                'delta': {
                    'content': '',
                    'reasoning_content': f'  • {worker.id} ({worker.provider}) — {worker.model}\n'
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
    
    # 5. Fanout
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
    
    # 6. Результаты
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
    """OpenAI-compatible endpoint"""
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
    """Возвращает список доступных моделей"""
    backend = get_backend()
    workers = backend.registry.get_enabled_workers()
    
    models = []
    for worker in workers:
        models.append({
            "id": f"swarm/{worker.id}",
            "object": "model",
            "created": int(time.time()),
            "owned_by": worker.provider,
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
    """Health check"""
    backend = get_backend()
    return JSONResponse(content=backend.get_worker_status())

@app.get("/v1/workers")
async def workers_status():
    """Детальный статус воркеров"""
    backend = get_backend()
    return JSONResponse(content=backend.get_worker_status())

# ── Startup / Shutdown ──────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    print("🚀 SWEN v4 Unified SwarmProvider starting...")
    backend = get_backend()
    if backend.connect():
        print("✅ Ready")
    else:
        print("⚠️  Failed to initialize, will retry on first request")

@app.on_event("shutdown")
async def shutdown():
    print("🛑 SwarmProvider shutting down...")
    backend = get_backend()
    backend.disconnect()

# ── Main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  SWEN v4 Unified SwarmProvider")
    print("  Discovery: OpenRouter API + TOML config")
    print("  Workers: Built-in HTTP clients")
    print("=" * 60)
    print(f"  Port: 8080")
    print("=" * 60)
    uvicorn.run(app, host="0.0.0.0", port=8080)
