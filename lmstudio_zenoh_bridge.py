#!/usr/bin/env python3
"""
Zenoh Bridge для LM Studio на MacBook
Превращает локальный LM Studio HTTP API в полноценного Zenoh воркера SWEN v3
"""

import json
import sys
import time
import urllib.request
import threading
from typing import Optional

import zenoh
from zenoh import handlers as zenoh_handlers

# ── Configuration ───────────────────────────────────────────────────────────

LM_STUDIO_URL = "http://127.0.0.1:1234/v1/chat/completions"
MODEL = "huihui-qwen3.5-2b-abliterated"
WORKER_ROLE = "macbook_huihui_qwen3_5_2b"
WORKER_ID = "macbook-local"

# Подключение к Zenoh mesh (такое же как у других воркеров)
ZENOH_CONNECT = ["tcp/10.15.64.226:7447", "tcp/10.15.66.12:7447"]

# ── LM Studio Client ────────────────────────────────────────────────────────

def ask_lmstudio(question: str, system_prompt: str = "You are a helpful assistant. Be concise.") -> dict:
    """Отправляет вопрос в LM Studio и возвращает ответ"""
    try:
        api_req = {
            "model": MODEL,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": question}
            ],
            "temperature": 0.7,
            "max_tokens": 1024,
        }
        
        req_data = json.dumps(api_req).encode()
        api_request = urllib.request.Request(
            LM_STUDIO_URL,
            data=req_data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        
        start = time.time()
        with urllib.request.urlopen(api_request, timeout=120) as resp:
            api_resp = json.loads(resp.read().decode())
        
        latency_ms = int((time.time() - start) * 1000)
        answer = api_resp["choices"][0]["message"]["content"]
        
        return {
            "ok": True,
            "answer": answer,
            "latency_ms": latency_ms,
            "model": MODEL,
            "worker_id": WORKER_ID,
            "error": None
        }
    except Exception as e:
        return {
            "ok": False,
            "answer": "",
            "latency_ms": 0,
            "model": MODEL,
            "worker_id": WORKER_ID,
            "error": str(e)
        }

# ── Zenoh Worker ────────────────────────────────────────────────────────────

class LMStudioZenohWorker:
    """Zenoh воркер, который перенаправляет запросы в LM Studio"""
    
    def __init__(self):
        self.session: Optional[zenoh.Session] = None
        self.queryable = None
        self.running = False
        
    def connect(self) -> bool:
        """Подключение к Zenoh mesh"""
        try:
            cfg = zenoh.Config()
            cfg.insert_json5("mode", '"peer"')
            cfg.insert_json5("connect/endpoints", json.dumps(ZENOH_CONNECT))
            cfg.insert_json5("scouting/multicast/enabled", "false")
            cfg.insert_json5("scouting/gossip/enabled", "true")
            
            self.session = zenoh.open(cfg)
            print(f"✅ Connected to Zenoh mesh")
            print(f"   Role: {WORKER_ROLE}")
            print(f"   Model: {MODEL}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Zenoh: {e}")
            return False
    
    def start(self):
        """Запускает обработку запросов"""
        if not self.session:
            print("❌ Not connected to Zenoh")
            return
        
        # Создаём queryable для обработки запросов
        queryable_key = f"swen/v3/ask/{WORKER_ROLE}"
        
        def handle_query(query):
            """Обработчик входящих запросов"""
            try:
                # Получаем payload запроса
                payload = bytes(query.payload).decode() if query.payload else "{}"
                req = json.loads(payload)
                
                question = req.get("question", "")
                thread_id = req.get("thread_id", "unknown")
                
                print(f"📨 Request received: {question[:50]}...")
                
                # Отправляем в LM Studio
                result = ask_lmstudio(question)
                
                # Формируем ответ
                response = {
                    "worker_id": WORKER_ID,
                    "role": WORKER_ROLE,
                    "ok": result["ok"],
                    "answer": result["answer"],
                    "latency_ms": result["latency_ms"],
                    "model": result["model"],
                    "error": result["error"]
                }
                
                # Отправляем ответ обратно
                query.reply(query.key_expr, json.dumps(response).encode())
                
                status = "✅" if result["ok"] else "❌"
                print(f"{status} Response sent ({result['latency_ms']}ms)")
                
            except Exception as e:
                print(f"❌ Error handling query: {e}")
                error_response = {
                    "worker_id": WORKER_ID,
                    "role": WORKER_ROLE,
                    "ok": False,
                    "answer": "",
                    "latency_ms": 0,
                    "model": MODEL,
                    "error": str(e)
                }
                query.reply(query.key_expr, json.dumps(error_response).encode())
        
        # Регистрируем queryable
        self.queryable = self.session.declare_queryable(
            queryable_key,
            handle_query
        )
        
        self.running = True
        print(f"🚀 Worker started")
        print(f"   Listening on: {queryable_key}")
        print(f"   Press Ctrl+C to stop")
        
        # Держим worker живым
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Stopping worker...")
        finally:
            self.stop()
    
    def stop(self):
        """Остановка worker"""
        self.running = False
        if self.queryable:
            self.queryable.undeclare()
        if self.session:
            self.session.close()
        print("✅ Worker stopped")

# ── Main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("  LM Studio Zenoh Bridge")
    print("  MacBook Worker for SWEN v3")
    print("=" * 60)
    print(f"  LM Studio: {LM_STUDIO_URL}")
    print(f"  Model: {MODEL}")
    print(f"  Role: {WORKER_ROLE}")
    print(f"  Zenoh: {ZENOH_CONNECT}")
    print("=" * 60)
    
    worker = LMStudioZenohWorker()
    if worker.connect():
        worker.start()
    else:
        print("❌ Failed to start worker")
        sys.exit(1)
