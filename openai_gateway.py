#!/usr/bin/env python3
"""
SWEN v3 OpenAI-compatible API Gateway
Входная точка в рой через HTTP API
"""

import asyncio
import json
import os
import sys
import time
import uuid
from typing import AsyncGenerator, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
import uvicorn

sys.path.insert(0, os.path.expanduser("~/.local/share/swen3"))
from agent import Swen3Agent, Swen3Config

app = FastAPI(title="SWEN v3 Swarm API", version="1.0.0")

# Глобальный агент для связи с роем
swarm_agent: Optional[Swen3Agent] = None
AVAILABLE_WORKERS = ["glm_flash", "qwen3_5_4b_opus", "jetson_gemma4b"]


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatCompletionRequest(BaseModel):
    model: str = "swarm"
    messages: List[ChatMessage]
    temperature: Optional[float] = 0.7
    max_tokens: Optional[int] = None
    stream: bool = False
    # Дополнительные поля для совместимости
    top_p: Optional[float] = None
    frequency_penalty: Optional[float] = None
    presence_penalty: Optional[float] = None
    
    # Разрешаем любые дополнительные поля
    class Config:
        extra = "allow"


class ChatCompletionChoice(BaseModel):
    index: int
    message: Optional[Dict] = None
    delta: Optional[Dict] = None
    finish_reason: Optional[str] = None


class ChatCompletionResponse(BaseModel):
    id: str
    object: str = "chat.completion"
    created: int
    model: str
    choices: List[ChatCompletionChoice]
    usage: Dict = Field(default_factory=dict)


async def connect_swarm():
    """Подключение к Zenoh mesh"""
    global swarm_agent
    cfg = Swen3Config(
        zenoh_connect=["tcp/10.15.64.226:7447", "tcp/10.15.66.12:7447"],
        roles=AVAILABLE_WORKERS,
        deadline_ms=120000,
    )
    swarm_agent = Swen3Agent(cfg)
    if not swarm_agent.connect():
        raise Exception(f"Cannot connect to swarm: {swarm_agent.status}")
    time.sleep(1)
    print("✅ Connected to SWEN v3 swarm", flush=True)


def extract_last_message(messages: List[ChatMessage]) -> str:
    """Извлекает последнее user сообщение"""
    for msg in reversed(messages):
        if msg.role == "user":
            return msg.content
    return ""


async def ask_worker(worker: str, question: str) -> Dict:
    """Запрос к одному воркеру"""
    try:
        # Запускаем блокирующий вызов в отдельном потоке
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, 
            swarm_agent.ask_one, 
            worker, 
            question, 
            f"api-{time.time()}"
        )
        if result.ok:
            return {
                "worker": worker,
                "answer": result.answer,
                "latency_ms": result.latency_ms,
                "status": "success"
            }
        else:
            return {
                "worker": worker,
                "answer": "",
                "error": result.error,
                "status": "error"
            }
    except Exception as e:
        return {
            "worker": worker,
            "answer": "",
            "error": str(e),
            "status": "error"
        }


def judge_best_response(results: List[Dict]) -> Optional[Dict]:
    """Выбирает лучший ответ из всех воркеров"""
    successful = [r for r in results if r["status"] == "success" and r["answer"]]
    
    if not successful:
        return None
    
    # Простая эвристика: выбираем ответ с наибольшей длиной и успешным статусом
    # В будущем можно добавить LLM-based judging
    best = max(successful, key=lambda x: len(x["answer"]))
    return best


@app.on_event("startup")
async def startup_event():
    await connect_swarm()


@app.get("/v1/models")
async def list_models():
    """Список доступных моделей"""
    models = [
        {
            "id": "swarm",
            "object": "model",
            "created": int(time.time()),
            "owned_by": "swen3"
        }
    ]
    # Добавляем отдельных воркеров
    for worker in AVAILABLE_WORKERS:
        models.append({
            "id": worker,
            "object": "model",
            "created": int(time.time()),
            "owned_by": "swen3"
        })
    
    return {
        "object": "list",
        "data": models
    }


@app.post("/v1/chat/completions")
async def chat_completions(request: ChatCompletionRequest):
    """Основной endpoint для чат-комплишенов"""
    
    if not swarm_agent:
        raise HTTPException(status_code=503, detail="Swarm not connected")
    
    question = extract_last_message(request.messages)
    if not question:
        raise HTTPException(status_code=400, detail="No user message found")
    
    request_id = f"chatcmpl-{uuid.uuid4().hex}"
    created = int(time.time())
    
    # Если запрошен конкретный воркер
    if request.model in AVAILABLE_WORKERS:
        result = await ask_worker(request.model, question)
        
        if result["status"] != "success":
            raise HTTPException(status_code=500, detail=result.get("error", "Unknown error"))
        
        response = ChatCompletionResponse(
            id=request_id,
            created=created,
            model=request.model,
            choices=[
                ChatCompletionChoice(
                    index=0,
                    message={
                        "role": "assistant",
                        "content": result["answer"]
                    },
                    finish_reason="stop"
                )
            ],
            usage={
                "prompt_tokens": len(question.split()),
                "completion_tokens": len(result["answer"].split()),
                "total_tokens": len(question.split()) + len(result["answer"].split())
            }
        )
        return response
    
    # Если запрошен swarm — делаем fanout
    if request.model == "swarm":
        if request.stream:
            return StreamingResponse(
                stream_swarm_response(request_id, created, question),
                media_type="text/event-stream"
            )
        else:
            # Non-streaming
            results = await fanout_to_workers(question)
            best = judge_best_response(results)
            
            if not best:
                raise HTTPException(status_code=500, detail="All workers failed")
            
            response = ChatCompletionResponse(
                id=request_id,
                created=created,
                model="swarm",
                choices=[
                    ChatCompletionChoice(
                        index=0,
                        message={
                            "role": "assistant",
                            "content": best["answer"]
                        },
                        finish_reason="stop"
                    )
                ],
                usage={
                    "prompt_tokens": len(question.split()),
                    "completion_tokens": len(best["answer"].split()),
                    "total_tokens": len(question.split()) + len(best["answer"].split())
                }
            )
            return response
    
    raise HTTPException(status_code=400, detail=f"Unknown model: {request.model}")


async def fanout_to_workers(question: str) -> List[Dict]:
    """Отправляет запрос всем воркерам параллельно"""
    tasks = [ask_worker(worker, question) for worker in AVAILABLE_WORKERS]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    processed = []
    for result in results:
        if isinstance(result, Exception):
            processed.append({
                "worker": "unknown",
                "status": "error",
                "error": str(result)
            })
        else:
            processed.append(result)
    
    return processed


async def stream_swarm_response(request_id: str, created: int, question: str) -> AsyncGenerator[str, None]:
    """Потоковый ответ с мышлением всех воркеров"""
    
    # Отправляем начало
    yield f"data: {json.dumps({'id': request_id, 'object': 'chat.completion.chunk', 'created': created, 'model': 'swarm', 'choices': [{'index': 0, 'delta': {'role': 'assistant'}, 'finish_reason': None}]})}\n\n"
    
    # Запускаем всех воркеров
    tasks = {worker: asyncio.create_task(ask_worker(worker, question)) for worker in AVAILABLE_WORKERS}
    
    completed_workers = []
    
    # По мере готовности воркеров отправляем их ответы
    for worker in AVAILABLE_WORKERS:
        result = await tasks[worker]
        
        if result["status"] == "success":
            # Отправляем имя воркера
            yield f"data: {json.dumps({'id': request_id, 'object': 'chat.completion.chunk', 'created': created, 'model': 'swarm', 'choices': [{'index': 0, 'delta': {'content': f'\\n🤖 [{worker}]\\n'}, 'finish_reason': None}]})}\n\n"
            
            # Отправляем ответ частями (для имитации потокового вывода)
            words = result["answer"].split()
            chunk_size = 5
            for i in range(0, len(words), chunk_size):
                chunk = " ".join(words[i:i+chunk_size])
                yield f"data: {json.dumps({'id': request_id, 'object': 'chat.completion.chunk', 'created': created, 'model': 'swarm', 'choices': [{'index': 0, 'delta': {'content': chunk + ' '}, 'finish_reason': None}]})}\n\n"
                await asyncio.sleep(0.05)  # Небольшая задержка для эффекта потока
            
            completed_workers.append(result)
        else:
            yield f"data: {json.dumps({'id': request_id, 'object': 'chat.completion.chunk', 'created': created, 'model': 'swarm', 'choices': [{'index': 0, 'delta': {'content': f'\\n❌ [{worker}] failed\\n'}, 'finish_reason': None}]})}\n\n"
    
    # Выбираем лучший ответ и отправляем финальный
    best = judge_best_response(completed_workers)
    if best:
        best_worker = best["worker"]
        yield f"data: {json.dumps({'id': request_id, 'object': 'chat.completion.chunk', 'created': created, 'model': 'swarm', 'choices': [{'index': 0, 'delta': {'content': '\\n✅ Best: [' + best_worker + ']\\n'}, 'finish_reason': None}]})}\n\n"
    
    # Конец потока
    yield f"data: {json.dumps({'id': request_id, 'object': 'chat.completion.chunk', 'created': created, 'model': 'swarm', 'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'stop'}]})}\n\n"
    yield "data: [DONE]\n\n"


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "swarm_connected": swarm_agent is not None,
        "workers": AVAILABLE_WORKERS
    }


if __name__ == "__main__":
    print("🚀 Starting SWEN v3 OpenAI-compatible API Gateway...")
    print(f"📡 Workers: {', '.join(AVAILABLE_WORKERS)}")
    print("🔗 Endpoint: http://localhost:8080/v1/chat/completions")
    
    uvicorn.run(app, host="0.0.0.0", port=8080)
