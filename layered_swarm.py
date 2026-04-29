#!/usr/bin/env python3
"""
Layered Swarm Controller — многослойная роевая архитектура
Все операции распределяются на пулы воркеров
"""
import json
import os
import sys
import time
import concurrent.futures
from typing import List, Dict, Any, Optional

sys.path.insert(0, os.path.expanduser("~/.local/share/swen3"))
from agent import Swen3Agent, Swen3Config

# Конфигурация слоёв
LAYER_CONFIG = {
    "decomposition": {
        "workers": ["qwen3_5_4b_opus", "glm_flash"],
        "min_workers": 1,
        "max_workers": 2,
    },
    "execution": {
        "workers": ["qwen3_5_4b_opus", "glm_flash", "jetson_gemma4b"],
        "min_workers": 1,
        "max_workers": 3,
    },
    "judging": {
        "workers": ["qwen3_5_4b_opus", "glm_flash"],
        "min_workers": 2,
        "max_workers": 2,
    },
    "final_judge": {
        "workers": ["qwen3_5_4b_opus"],
        "min_workers": 1,
        "max_workers": 1,
    }
}

class LayeredSwarmController:
    def __init__(self):
        self.agent = None
        self.connect()
    
    def connect(self):
        """Подключение к Zenoh mesh"""
        cfg = Swen3Config(
            zenoh_connect=["tcp/10.15.64.226:7447", "tcp/10.15.66.12:7447"],
            zenoh_listen=["tcp/0.0.0.0:7447"],
            roles=["glm_flash", "qwen3_5_4b_opus", "jetson_gemma4b"],
            deadline_ms=120000,
        )
        self.agent = Swen3Agent(cfg)
        if not self.agent.connect():
            raise Exception(f"Cannot connect: {self.agent.status}")
        time.sleep(1)
    
    def ask_pool(self, question: str, workers: List[str], timeout_ms: int = 60000) -> List[Dict]:
        """Отправить вопрос пулу воркеров, вернуть все ответы"""
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(workers)) as executor:
            futures = {
                executor.submit(self.agent.ask_one, worker, question, f"layer-{time.time()}"): worker
                for worker in workers
            }
            
            for future in concurrent.futures.as_completed(futures):
                worker = futures[future]
                try:
                    result = future.result()
                    results.append({
                        "worker": worker,
                        "ok": result.ok,
                        "answer": result.answer,
                        "latency_ms": result.latency_ms,
                        "error": result.error,
                    })
                except Exception as e:
                    results.append({
                        "worker": worker,
                        "ok": False,
                        "answer": "",
                        "latency_ms": 0,
                        "error": str(e),
                    })
        
        return results
    
    def layer1_decompose(self, question: str) -> Dict[str, Any]:
        """
        Layer 1: Декомпозиция
        Несколько воркеров решают, на сколько частей разбить задачу (1-N)
        """
        print(f"[Layer 1] Decomposition pool: {LAYER_CONFIG['decomposition']['workers']}")
        
        decompose_prompt = f"""Analyze this task and decide how to decompose it.
        
Task: {question}

Respond in JSON format:
{{
    "complexity": "simple|medium|complex",
    "num_subtasks": 1-5,
    "subtasks": [
        "description of subtask 1",
        "description of subtask 2"
    ],
    "reasoning": "why this decomposition"
}}

If the task is simple (1 subtask), just return it as-is.
"""
        
        results = self.ask_pool(decompose_prompt, LAYER_CONFIG["decomposition"]["workers"])
        
        # Анализируем ответы
        decompositions = []
        for r in results:
            if r["ok"]:
                try:
                    # Пытаемся извлечь JSON из ответа
                    answer = r["answer"]
                    # Находим JSON в ответе
                    start = answer.find('{')
                    end = answer.rfind('}') + 1
                    if start >= 0 and end > start:
                        json_str = answer[start:end]
                        decomp = json.loads(json_str)
                        decompositions.append(decomp)
                except:
                    pass
        
        # Выбираем декомпозицию с наибольшим числом подзадач (консервативный подход)
        # или с наименьшим (агрессивный)
        if not decompositions:
            # Fallback: без декомпозиции
            return {
                "complexity": "simple",
                "num_subtasks": 1,
                "subtasks": [question],
            }
        
        # Берём среднее или моду
        num_subtasks_list = [d.get("num_subtasks", 1) for d in decompositions]
        avg_subtasks = sum(num_subtasks_list) / len(num_subtasks_list)
        
        # Выбираем декомпозицию ближайшую к среднему
        closest = min(decompositions, key=lambda d: abs(d.get("num_subtasks", 1) - avg_subtasks))
        
        print(f"[Layer 1] Decomposition: {closest.get('num_subtasks', 1)} subtasks")
        return closest
    
    def layer2_execute(self, subtasks: List[str]) -> List[Dict]:
        """
        Layer 2: Выполнение подзадач
        Распределяем подзадачи на пул воркеров
        """
        print(f"[Layer 2] Execution pool: {LAYER_CONFIG['execution']['workers']}")
        
        results = []
        workers = LAYER_CONFIG["execution"]["workers"]
        
        # Распределяем подзадачи по воркерам round-robin
        for i, subtask in enumerate(subtasks):
            worker = workers[i % len(workers)]
            print(f"[Layer 2] Subtask {i+1}/{len(subtasks)} → {worker}")
            
            result = self.ask_pool(subtask, [worker])
            if result and result[0]["ok"]:
                results.append({
                    "subtask": subtask,
                    "worker": worker,
                    "answer": result[0]["answer"],
                    "latency_ms": result[0]["latency_ms"],
                })
            else:
                # Fallback: пробуем другого воркера
                fallback_worker = workers[(i + 1) % len(workers)]
                print(f"[Layer 2] Fallback to {fallback_worker}")
                fallback_result = self.ask_pool(subtask, [fallback_worker])
                if fallback_result and fallback_result[0]["ok"]:
                    results.append({
                        "subtask": subtask,
                        "worker": fallback_worker,
                        "answer": fallback_result[0]["answer"],
                        "latency_ms": fallback_result[0]["latency_ms"],
                    })
        
        print(f"[Layer 2] Completed {len(results)}/{len(subtasks)} subtasks")
        return results
    
    def layer3_judge(self, question: str, subtask_results: List[Dict]) -> List[Dict]:
        """
        Layer 3: Перекрёстная оценка судей
        Несколько воркеров оценивают и композируют результаты
        """
        print(f"[Layer 3] Judging pool: {LAYER_CONFIG['judging']['workers']}")
        
        # Формируем контекст для судей
        context = f"Original task: {question}\n\nSubtask results:\n"
        for i, r in enumerate(subtask_results):
            context += f"\n[{i+1}] Worker: {r['worker']}\n"
            context += f"Subtask: {r['subtask']}\n"
            context += f"Answer: {r['answer'][:500]}...\n"
        
        judge_prompt = f"""{context}

Evaluate and synthesize these results. Create 2-3 alternative final answers:

Respond in JSON format:
{{
    "alternatives": [
        {{
            "answer": "complete final answer",
            "quality_score": 1-10,
            "reasoning": "why this answer is good"
        }}
    ],
    "best_alternative_index": 0-2
}}
"""
        
        results = self.ask_pool(judge_prompt, LAYER_CONFIG["judging"]["workers"])
        
        alternatives = []
        for r in results:
            if r["ok"]:
                try:
                    answer = r["answer"]
                    start = answer.find('{')
                    end = answer.rfind('}') + 1
                    if start >= 0 and end > start:
                        json_str = answer[start:end]
                        judge_result = json.loads(json_str)
                        if "alternatives" in judge_result:
                            alternatives.extend(judge_result["alternatives"])
                except:
                    pass
        
        print(f"[Layer 3] Generated {len(alternatives)} alternatives")
        return alternatives
    
    def layer4_final_judge(self, question: str, alternatives: List[Dict]) -> str:
        """
        Layer 4: Финальный судья
        Один воркер выбирает окончательный ответ
        """
        print(f"[Layer 4] Final judge: {LAYER_CONFIG['final_judge']['workers']}")
        
        if len(alternatives) == 0:
            return "[Error] No alternatives generated"
        
        if len(alternatives) == 1:
            return alternatives[0]["answer"]
        
        context = f"Task: {question}\n\nAlternatives:\n"
        for i, alt in enumerate(alternatives):
            context += f"\n[{i+1}] Score: {alt.get('quality_score', 'N/A')}/10\n"
            context += f"Answer: {alt['answer'][:500]}...\n"
            context += f"Reasoning: {alt.get('reasoning', 'N/A')}\n"
        
        final_prompt = f"""{context}

Select the BEST final answer and provide it in full.

Respond ONLY with the final answer, no JSON, no explanations.
"""
        
        results = self.ask_pool(final_prompt, LAYER_CONFIG["final_judge"]["workers"])
        
        if results and results[0]["ok"]:
            return results[0]["answer"]
        
        # Fallback: выбираем альтернативу с highest score
        best = max(alternatives, key=lambda x: x.get("quality_score", 0))
        return best["answer"]
    
    def process(self, question: str) -> Dict[str, Any]:
        """Полный pipeline обработки задачи"""
        start_time = time.time()
        
        print(f"\n{'='*60}")
        print(f"[Swarm] Processing: {question[:60]}...")
        print(f"{'='*60}\n")
        
        # Layer 1: Декомпозиция
        decomposition = self.layer1_decompose(question)
        
        if decomposition["num_subtasks"] == 1:
            # Простая задача — сразу на выполнение
            print(f"[Swarm] Simple task, skipping decomposition\n")
            subtasks = [question]
        else:
            subtasks = decomposition.get("subtasks", [question])
        
        # Layer 2: Выполнение
        execution_results = self.layer2_execute(subtasks)
        
        if len(subtasks) == 1:
            # Простая задача — возвращаем результат сразу
            if execution_results:
                total_time = int((time.time() - start_time) * 1000)
                return {
                    "answer": execution_results[0]["answer"],
                    "worker": execution_results[0]["worker"],
                    "total_time_ms": total_time,
                    "layers_used": 2,
                }
        
        # Layer 3: Перекрёстная оценка
        alternatives = self.layer3_judge(question, execution_results)
        
        # Layer 4: Финальный судья
        final_answer = self.layer4_final_judge(question, alternatives)
        
        total_time = int((time.time() - start_time) * 1000)
        
        return {
            "answer": final_answer,
            "total_time_ms": total_time,
            "layers_used": 4,
            "decomposition": decomposition,
            "alternatives_count": len(alternatives),
        }
    
    def disconnect(self):
        if self.agent:
            self.agent.disconnect()


def main():
    if len(sys.argv) < 2:
        print("Usage: layered_swarm.py <question>")
        sys.exit(1)
    
    question = " ".join(sys.argv[1:])
    
    try:
        controller = LayeredSwarmController()
        result = controller.process(question)
        controller.disconnect()
        
        print(f"\n{'='*60}")
        print(f"[Swarm] Final Answer:")
        print(f"{'='*60}")
        print(result["answer"])
        print(f"\n[Stats] Time: {result['total_time_ms']}ms | Layers: {result['layers_used']}")
        
    except Exception as e:
        print(f"[Error] {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
