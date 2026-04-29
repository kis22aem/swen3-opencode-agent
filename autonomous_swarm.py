#!/usr/bin/env python3
"""
Autonomous Swarm — полностью автономный рой (упрощённая версия)
Рой сам решает: выбор воркеров, оценку, финальный ответ
"""
import json
import os
import sys
import time
from typing import List, Dict, Any

sys.path.insert(0, os.path.expanduser("~/.local/share/swen3"))
from agent import Swen3Agent, Swen3Config

class AutonomousSwarm:
    def __init__(self):
        self.agent = None
        self.available_workers = ["glm_flash", "qwen3_5_4b_opus", "jetson_gemma4b"]
        
    def connect(self):
        """Подключение к Zenoh mesh"""
        cfg = Swen3Config(
            zenoh_connect=["tcp/10.15.64.226:7447", "tcp/10.15.66.12:7447"],
            roles=self.available_workers,
            deadline_ms=60000,
        )
        self.agent = Swen3Agent(cfg)
        if not self.agent.connect():
            raise Exception(f"Cannot connect: {self.agent.status}")
        time.sleep(1)
    
    def process_single(self, worker: str, question: str) -> Dict[str, Any]:
        """Отправить на ОДНОГО воркера (для потокового вывода)"""
        try:
            result = self.agent.ask_one(worker, question, f"single-{time.time()}")
            if result.ok:
                return {
                    "status": "success",
                    "answer": result.answer,
                    "worker": worker,
                    "latency_ms": result.latency_ms,
                }
            else:
                return {"status": "error", "error": result.error, "answer": ""}
        except Exception as e:
            return {"status": "error", "error": str(e), "answer": ""}
    
    def process(self, question: str) -> Dict[str, Any]:
        """Автономная обработка: отправляем на ВСЕХ воркеров, выбираем лучший"""
        start_time = time.time()
        
        print(f"[Autonomous Swarm] Processing: {question[:60]}...")
        print(f"[Autonomous Swarm] Fanout to all workers...")
        
        # Отправляем на всех воркеров последовательно (избегаем deadlock)
        results = []
        for worker in self.available_workers:
            print(f"  → {worker}...", end=" ", flush=True)
            try:
                result = self.agent.ask_one(worker, question, f"auto-{time.time()}")
                if result.ok:
                    print(f"✅ ({result.latency_ms}ms)")
                    results.append({
                        "worker": worker,
                        "answer": result.answer,
                        "latency_ms": result.latency_ms,
                    })
                else:
                    print(f"❌ ({result.error[:30]})")
            except Exception as e:
                print(f"❌ ({str(e)[:30]})")
        
        if not results:
            return {"status": "error", "error": "All workers failed", "answer": ""}
        
        # Выбираем лучший ответ (самый длинный = скорее всего полный)
        best = max(results, key=lambda x: len(x["answer"]))
        
        total_time = int((time.time() - start_time) * 1000)
        
        return {
            "status": "success",
            "answer": best["answer"],
            "worker": best["worker"],
            "total_time_ms": total_time,
            "workers_used": [r["worker"] for r in results],
            "all_results": results,
        }
    
    def disconnect(self):
        if self.agent:
            self.agent.disconnect()

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Autonomous Swarm")
    parser.add_argument("question", nargs="+", help="Question to ask")
    parser.add_argument("--worker", help="Single worker to use (for streaming)")
    args = parser.parse_args()
    
    question = " ".join(args.question)
    
    try:
        swarm = AutonomousSwarm()
        swarm.connect()
        
        if args.worker:
            # Одиночный воркер (для потокового вывода)
            result = swarm.process_single(args.worker, question)
            if result["status"] == "success":
                print(result["answer"])
            else:
                print(f"[Error] {result.get('error', 'Unknown')}", file=sys.stderr)
                sys.exit(1)
        else:
            # Все воркеры (автономный выбор)
            result = swarm.process(question)
            
            if result["status"] == "error":
                print(f"[Error] {result['error']}", file=sys.stderr)
                sys.exit(1)
            
            print(f"\n{'='*60}")
            print(f"[Autonomous Swarm] Answer (from {result['worker']}):")
            print(f"{'='*60}")
            print(result["answer"])
            print(f"\n[Stats] Time: {result['total_time_ms']}ms")
            print(f"Workers: {', '.join(result['workers_used'])}")
            print(f"{'='*60}\n")
        
        swarm.disconnect()
        
    except Exception as e:
        print(f"[Error] {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
