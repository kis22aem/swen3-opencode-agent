#!/usr/bin/env python3
"""
Tunnel — прозрачный туннель к LM Studio Net (qwen3.5-4b-opus)
Поддерживает streaming и non-streaming режимы
Логирует только короткие запросы для анализа
"""

import json
from datetime import datetime
from urllib.parse import urljoin

import requests
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
import uvicorn

app = FastAPI()

TARGET_BASE = "http://10.15.64.226:1234/v1"
LOG_FILE = "/Users/alex/.local/share/swen3/tunnel.log"

def log_event(direction: str, data: dict):
    timestamp = datetime.now().isoformat()
    entry = {"timestamp": timestamp, "direction": direction, "data": data}
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
async def tunnel(request: Request, path: str):
    target_url = urljoin(TARGET_BASE, path)
    if request.query_params:
        target_url += "?" + str(request.query_params)
    
    body = await request.body()
    body_str = body.decode("utf-8") if body else None
    
    # Подменяем model на qwen3.5-4b-opus и очищаем контекст
    is_stream = False
    if body_str:
        try:
            body_json = json.loads(body_str)
            original_model = body_json.get("model", "")
            is_stream = body_json.get("stream", False)
            if original_model != "qwen3.5-4b-claude-4.6-opus-reasoning-distilled-v2":
                body_json["model"] = "qwen3.5-4b-claude-4.6-opus-reasoning-distilled-v2"
            
            # Очищаем контекст - оставляем только system и последнее user сообщение
            messages = body_json.get("messages", [])
            cleaned_messages = []
            
            # Ищем system сообщение
            for msg in messages:
                if msg.get("role") == "system":
                    cleaned_messages.append(msg)
                    break
            
            # Ищем последнее user сообщение (не system-reminder)
            for msg in reversed(messages):
                if msg.get("role") == "user":
                    content = msg.get("content", "")
                    # Пропускаем сообщения с system-reminder
                    if isinstance(content, str) and "system-reminder" not in content and "plan mode" not in content.lower():
                        cleaned_messages.append(msg)
                        break
                    elif isinstance(content, list):
                        # Берем только text части, пропускаем system-reminder
                        clean_content = []
                        for item in content:
                            if isinstance(item, dict):
                                text = item.get("text", "")
                                if "system-reminder" not in text and "plan mode" not in text.lower():
                                    clean_content.append(item)
                        if clean_content:
                            cleaned_messages.append({"role": "user", "content": clean_content})
                            break
            
            body_json["messages"] = cleaned_messages
            body_str = json.dumps(body_json)
            body = body_str.encode("utf-8")
        except Exception as e:
            print(f"Error cleaning context: {e}")
    
    # Логируем запрос (короткий формат)
    if body_str:
        try:
            body_json = json.loads(body_str)
            messages = body_json.get("messages", [])
            log_data = {
                "method": request.method,
                "url": str(request.url),
                "stream": is_stream,
                "model": body_json.get("model"),
                "messages_count": len(messages),
                "messages": []
            }
            for msg in messages:
                content = msg.get("content", "")
                log_data["messages"].append({
                    "role": msg.get("role"),
                    "content_preview": content[:200] + "..." if len(content) > 200 else content
                })
            log_event("REQUEST", log_data)
        except:
            pass
    
    try:
        response = requests.request(
            method=request.method,
            url=target_url,
            headers={k: v for k, v in request.headers.items() if k.lower() not in ["host", "content-length"]},
            data=body,
            timeout=120,
            stream=is_stream
        )
        
        # Если streaming — проксируем поток
        if is_stream:
            async def stream_generator():
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        yield chunk
            
            return StreamingResponse(
                stream_generator(),
                status_code=response.status_code,
                headers={k: v for k, v in response.headers.items() 
                        if k.lower() not in ["content-encoding", "transfer-encoding", "content-length"]}
            )
        
        # Если не streaming — возвращаем JSON
        try:
            response_json = response.json()
            # Удаляем reasoning_content из ответа
            if "choices" in response_json:
                for choice in response_json["choices"]:
                    if "message" in choice and "reasoning_content" in choice["message"]:
                        del choice["message"]["reasoning_content"]
        except:
            response_json = {"raw": response.text[:1000]}
        
        log_event("RESPONSE", {
            "status_code": response.status_code,
            "model": response_json.get("model"),
            "content_preview": response_json.get("choices", [{}])[0].get("message", {}).get("content", "")[:200]
        })
        
        return JSONResponse(
            content=response_json,
            status_code=response.status_code,
            headers={k: v for k, v in response.headers.items() 
                    if k.lower() not in ["content-encoding", "transfer-encoding", "content-length"]}
        )
        
    except Exception as e:
        log_event("ERROR", {"error": str(e)})
        return JSONResponse(content={"error": str(e)}, status_code=500)

if __name__ == "__main__":
    print("🚀 Tunnel → qwen3.5-4b-opus @ 10.15.64.226:1234")
    uvicorn.run(app, host="0.0.0.0", port=8080)
