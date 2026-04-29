#!/usr/bin/env python3
"""
Прозрачный туннель к LM Studio Net с полным логированием
Все запросы логируются в формате: [REQUEST/RESPONSE] + JSON
"""

import json
import sys
import time
from datetime import datetime
from urllib.parse import urljoin

import requests
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
import uvicorn

app = FastAPI()

# Куда туннелировать
TARGET_BASE = "http://10.15.64.226:1234/v1"
LOG_FILE = "/Users/alex/.local/share/swen3/lmstudio_tunnel.log"

def log_event(direction: str, data: dict):
    """Логирует событие в файл"""
    timestamp = datetime.now().isoformat()
    entry = {
        "timestamp": timestamp,
        "direction": direction,
        "data": data
    }
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    
    # Также выводим в консоль
    print(f"\n{'='*80}")
    print(f"[{direction}] {timestamp}")
    print(f"{'='*80}")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    print(f"{'='*80}\n")

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
async def tunnel(request: Request, path: str):
    """Туннелирует все запросы к LM Studio Net"""
    
    # Формируем URL назначения
    target_url = urljoin(TARGET_BASE, path)
    if request.query_params:
        target_url += "?" + str(request.query_params)
    
    # Получаем тело запроса
    body = await request.body()
    body_str = body.decode("utf-8") if body else None
    
    # Подменяем model если нужно
    if body_str:
        try:
            body_json = json.loads(body_str)
            original_model = body_json.get("model", "")
            # Если модель не из списка доступных, подменяем на gpt-oss-20b
            if original_model and original_model not in ["gpt-oss-20b", "zai-org/glm-4.7-flash", "unsloth/qwen3.6-27b", 
                                                          "qwen3.5-9b-claude-4.6-opus-reasoning-distilled-v2",
                                                          "qwen3.5-4b-claude-4.6-opus-reasoning-distilled-v2"]:
                print(f"🔄 Подменяем model: {original_model} → qwen3.5-4b-claude-4.6-opus-reasoning-distilled-v2")
                body_json["model"] = "qwen3.5-4b-claude-4.6-opus-reasoning-distilled-v2"
                body_str = json.dumps(body_json)
                body = body_str.encode("utf-8")
        except:
            pass
    
    # Логируем входящий запрос
    request_data = {
        "method": request.method,
        "url": str(request.url),
        "target_url": target_url,
        "headers": dict(request.headers),
        "body": json.loads(body_str) if body_str else None
    }
    log_event("REQUEST", request_data)
    
    # Делаем запрос к LM Studio Net
    try:
        response = requests.request(
            method=request.method,
            url=target_url,
            headers={k: v for k, v in request.headers.items() if k.lower() not in ["host", "content-length"]},
            data=body,
            timeout=120,
            stream=True
        )
        
        # Получаем тело ответа
        response_body = response.content
        
        # Пытаемся распарсить как JSON
        try:
            response_json = json.loads(response_body.decode("utf-8"))
        except:
            response_json = {"raw": response_body.decode("utf-8", errors="replace")[:1000]}
        
        # Логируем ответ
        response_data = {
            "status_code": response.status_code,
            "headers": dict(response.headers),
            "body": response_json
        }
        log_event("RESPONSE", response_data)
        
        # Возвращаем ответ клиенту (без Content-Length чтобы избежать ошибок)
        headers = {k: v for k, v in response.headers.items() 
                   if k.lower() not in ["content-encoding", "transfer-encoding", "content-length"]}
        return JSONResponse(
            content=response_json,
            status_code=response.status_code,
            headers=headers
        )
        
    except Exception as e:
        error_data = {
            "error": str(e),
            "type": type(e).__name__
        }
        log_event("ERROR", error_data)
        return JSONResponse(
            content={"error": str(e)},
            status_code=500
        )

if __name__ == "__main__":
    print("🚀 LM Studio Net Tunnel")
    print(f"📡 Туннелируем: http://localhost:8080 → {TARGET_BASE}")
    print(f"📝 Логи: {LOG_FILE}")
    print("="*80)
    uvicorn.run(app, host="0.0.0.0", port=8080)
