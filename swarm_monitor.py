#!/usr/bin/env python3
"""
Swarm Monitor — система мониторинга воркеров роя
Проверяет доступность, скорость, статус воркеров
"""
import json
import os
import sys
import time
import concurrent.futures
from datetime import datetime
from typing import Dict, List, Any, Optional

sys.path.insert(0, os.path.expanduser("~/.local/share/swen3"))
from agent import Swen3Agent, Swen3Config

class SwarmMonitor:
    def __init__(self):
        self.agent = None
        self.worker_status = {}
        self.last_check = None
        
    def connect(self):
        """Подключение к Zenoh mesh"""
        cfg = Swen3Config(
            zenoh_connect=["tcp/10.15.64.226:7447", "tcp/10.15.66.12:7447"],
            zenoh_listen=["tcp/0.0.0.0:7447"],
            roles=["glm_flash", "qwen3_5_4b_opus", "jetson_gemma4b"],
            deadline_ms=30000,
        )
        self.agent = Swen3Agent(cfg)
        if not self.agent.connect():
            raise Exception(f"Cannot connect: {self.agent.status}")
        time.sleep(1)
        
    def check_worker(self, worker: str, test_question: str = "Hi") -> Dict[str, Any]:
        """Проверить одного воркера"""
        start = time.time()
        try:
            result = self.agent.ask_one(worker, test_question, f"health-check-{time.time()}")
            latency = int((time.time() - start) * 1000)
            
            return {
                "worker": worker,
                "status": "online" if result.ok else "error",
                "latency_ms": result.latency_ms if result.ok else latency,
                "answer_preview": result.answer[:100] if result.ok else "",
                "error": result.error if not result.ok else None,
                "timestamp": datetime.now().isoformat(),
            }
        except Exception as e:
            return {
                "worker": worker,
                "status": "offline",
                "latency_ms": 0,
                "answer_preview": "",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            }
    
    def check_all_workers(self) -> Dict[str, Any]:
        """Проверить всех воркеров"""
        workers = ["glm_flash", "qwen3_5_4b_opus", "jetson_gemma4b"]
        results = {}
        
        print(f"[Monitor] Checking {len(workers)} workers...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(workers)) as executor:
            futures = {
                executor.submit(self.check_worker, worker): worker
                for worker in workers
            }
            
            for future in concurrent.futures.as_completed(futures):
                worker = futures[future]
                try:
                    result = future.result()
                    results[worker] = result
                    status_icon = "✅" if result["status"] == "online" else "❌"
                    print(f"  {status_icon} {worker}: {result['status']} ({result['latency_ms']}ms)")
                except Exception as e:
                    results[worker] = {
                        "worker": worker,
                        "status": "error",
                        "error": str(e),
                    }
                    print(f"  ❌ {worker}: error - {e}")
        
        self.worker_status = results
        self.last_check = datetime.now().isoformat()
        
        return {
            "timestamp": self.last_check,
            "total_workers": len(workers),
            "online": sum(1 for r in results.values() if r["status"] == "online"),
            "offline": sum(1 for r in results.values() if r["status"] == "offline"),
            "error": sum(1 for r in results.values() if r["status"] == "error"),
            "workers": results,
        }
    
    def get_fastest_workers(self, n: int = 2) -> List[str]:
        """Получить N самых быстрых воркеров"""
        if not self.worker_status:
            self.check_all_workers()
        
        online_workers = [
            (name, data["latency_ms"])
            for name, data in self.worker_status.items()
            if data["status"] == "online"
        ]
        
        online_workers.sort(key=lambda x: x[1])
        return [name for name, _ in online_workers[:n]]
    
    def get_recommended_workers(self, task_type: str = "general") -> List[str]:
        """Получить рекомендуемых воркеров для типа задачи"""
        if not self.worker_status:
            self.check_all_workers()
        
        online = [w for w, d in self.worker_status.items() if d["status"] == "online"]
        
        if task_type == "code":
            # Для кода предпочитаем glm_flash и qwen
            priority = ["glm_flash", "qwen3_5_4b_opus", "jetson_gemma4b"]
        elif task_type == "simple":
            # Для простых задач — jetson (быстрый) и qwen
            priority = ["jetson_gemma4b", "qwen3_5_4b_opus", "glm_flash"]
        else:
            # По умолчанию — qwen (самый умный) и glm
            priority = ["qwen3_5_4b_opus", "glm_flash", "jetson_gemma4b"]
        
        return [w for w in priority if w in online]
    
    def print_status(self):
        """Вывести красивый статус"""
        if not self.worker_status:
            self.check_all_workers()
        
        print("\n" + "="*60)
        print("           SWARM MONITOR STATUS")
        print("="*60)
        print(f"Last check: {self.last_check}")
        print(f"Total: {len(self.worker_status)} workers")
        
        online = sum(1 for r in self.worker_status.values() if r["status"] == "online")
        offline = sum(1 for r in self.worker_status.values() if r["status"] == "offline")
        error = sum(1 for r in self.worker_status.values() if r["status"] == "error")
        
        print(f"Online:  {online} ✅")
        print(f"Offline: {offline} ❌")
        print(f"Error:   {error} ⚠️")
        print("-"*60)
        
        for name, data in self.worker_status.items():
            icon = "✅" if data["status"] == "online" else "❌" if data["status"] == "offline" else "⚠️"
            latency = f"{data['latency_ms']}ms" if data["status"] == "online" else "N/A"
            print(f"{icon} {name:20s} | {data['status']:10s} | {latency:10s}")
        
        print("="*60)
        
        # Рекомендации
        fastest = self.get_fastest_workers(2)
        print(f"\nFastest workers: {', '.join(fastest)}")
        
        recommended = self.get_recommended_workers("general")
        print(f"Recommended: {', '.join(recommended)}")
        
        print("="*60 + "\n")
    
    def disconnect(self):
        if self.agent:
            self.agent.disconnect()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Swarm Monitor")
    parser.add_argument("--check", action="store_true", help="Check all workers")
    parser.add_argument("--status", action="store_true", help="Show status")
    parser.add_argument("--fastest", type=int, default=0, help="Show N fastest workers")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    
    args = parser.parse_args()
    
    monitor = SwarmMonitor()
    monitor.connect()
    
    try:
        if args.check or args.status or not any([args.check, args.status, args.fastest]):
            result = monitor.check_all_workers()
            
            if args.json:
                print(json.dumps(result, indent=2, ensure_ascii=False))
            else:
                monitor.print_status()
        
        if args.fastest > 0:
            fastest = monitor.get_fastest_workers(args.fastest)
            print(f"Fastest {args.fastest} workers: {fastest}")
    
    finally:
        monitor.disconnect()

if __name__ == "__main__":
    main()
