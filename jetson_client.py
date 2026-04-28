#!/usr/bin/env python3
"""
Direct HTTP client for Jetson llama.cpp server
"""
import json
import urllib.request
import time

JETSON_URL = "http://10.15.66.12:8080/v1/chat/completions"
MODEL = "Qwen3.5-2B.Q4_K_M.gguf"
SYSTEM_PROMPT = "You are a helpful assistant. Be concise."

def ask_jetson(question: str) -> dict:
    """Send question to Jetson and return response"""
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
    
    latency = int((time.time() - start) * 1000)
    answer = api_resp["choices"][0]["message"]["content"]
    
    return {
        "ok": True,
        "answer": answer,
        "latency_ms": latency,
        "model": MODEL,
    }

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: jetson_client.py <question>")
        sys.exit(1)
    
    question = " ".join(sys.argv[1:])
    print(f"[Jetson] Sending: {question[:50]}...")
    
    try:
        result = ask_jetson(question)
        print(f"[Jetson] Answer ({result['latency_ms']}ms):")
        print(result["answer"])
    except Exception as e:
        print(f"[Jetson] Error: {e}")
