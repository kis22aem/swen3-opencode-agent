import { spawn } from "child_process";
import { ReadableStream } from "stream/web";

export interface Swen3ProviderOptions {
  zenohConnect?: string[];
  workers?: string[];
  timeout?: number;
}

export interface Message {
  role: "system" | "user" | "assistant";
  content: string;
}

// System prompt для роя
const SWARM_SYSTEM_PROMPT = `Ты — распределённый рой ИИ-агентов SWEN v3.
Ты состоишь из нескольких воркеров, которые работают параллельно:
- qwen3_5_4b_opus (Diana): сложные задачи, анализ
- glm_flash (Diana): код, технические тексты  
- jetson_gemma4b (Jetson): простые задачи, факты

Все воркеры получают один и тот же запрос и отвечают независимо.
Затем выбирается лучший ответ.

Отвечай на языке запроса пользователя.`;

export class Swen3Provider {
  private options: Swen3ProviderOptions;

  constructor(options: Swen3ProviderOptions = {}) {
    this.options = {
      zenohConnect: ["tcp/10.15.64.226:7447", "tcp/10.15.66.12:7447"],
      workers: ["glm_flash", "qwen3_5_4b_opus", "jetson_gemma4b"],
      timeout: 120000,
      ...options,
    };
  }

  async generate(
    messages: Message[],
    options?: { signal?: AbortSignal }
  ): Promise<ReadableStream<Uint8Array>> {
    // Извлекаем последний user message
    const lastMessage = messages.filter((m) => m.role === "user").pop();
    const question = lastMessage?.content || "";

    // Создаём поток
    const stream = new ReadableStream<Uint8Array>({
      start: async (controller) => {
        try {
          // Отправляем заголовок
          controller.enqueue(
            new TextEncoder().encode(
              `🌐 SWEN v3 Swarm Thinking...\n\n`
            )
          );

          // Запускаем всех воркеров параллельно
          const workerStreams = this.options.workers!.map((worker) =>
            this.streamWorker(worker, question, controller)
          );

          // Ждём завершения всех
          await Promise.all(workerStreams);

          // Отправляем финальный ответ
          controller.enqueue(
            new TextEncoder().encode(`\n✅ Swarm complete\n`)
          );
          controller.close();
        } catch (error) {
          controller.error(error);
        }
      },
    });

    return stream;
  }

  private async streamWorker(
    worker: string,
    question: string,
    controller: ReadableStreamDefaultController<Uint8Array>
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      const venv = `${process.env.HOME}/.venvs/swen3`;
      const script = `${process.env.HOME}/.local/share/swen3/autonomous_swarm.py`;
      const python = `${venv}/bin/python3`;

      // Отправляем начало воркера
      controller.enqueue(
        new TextEncoder().encode(`\n🤖 [${worker}] thinking...\n`)
      );

      const proc = spawn(python, [script, question], {
        env: {
          ...process.env,
          VIRTUAL_ENV: venv,
          PATH: `${venv}/bin:${process.env.PATH}`,
          SWEN_WORKERS: worker, // Отправляем только на этого воркера
        },
      });

      let output = "";

      proc.stdout.on("data", (data: Buffer) => {
        const text = data.toString();
        output += text;

        // Отправляем чанки в поток (только ответ, без служебных строк)
        const lines = text.split("\n");
        for (const line of lines) {
          if (line.trim() && !line.startsWith("[Autonomous Swarm]")) {
            controller.enqueue(
              new TextEncoder().encode(`  ${line}\n`)
            );
          }
        }
      });

      proc.stderr.on("data", (data: Buffer) => {
        const text = data.toString();
        if (text.includes("Error")) {
          controller.enqueue(
            new TextEncoder().encode(`  ❌ Error: ${text.trim()}\n`)
          );
        }
      });

      proc.on("close", (code) => {
        try {
          if (code === 0) {
            controller.enqueue(
              new TextEncoder().encode(`  ✅ [${worker}] done\n`)
            );
          } else {
            controller.enqueue(
              new TextEncoder().encode(`  ❌ [${worker}] failed (code ${code})\n`)
            );
          }
        } catch (e) {
          // Controller already closed, ignore
        }
        resolve();
      });

      proc.on("error", (err) => {
        try {
          controller.enqueue(
            new TextEncoder().encode(`  ❌ [${worker}] error: ${err.message}\n`)
          );
        } catch (e) {
          // Controller already closed, ignore
        }
        resolve(); // Не reject, чтобы другие воркеры продолжали
      });

      // Таймаут
      setTimeout(() => {
        proc.kill();
        try {
          controller.enqueue(
            new TextEncoder().encode(`  ⏱️ [${worker}] timeout\n`)
          );
        } catch (e) {
          // Controller already closed, ignore
        }
        resolve();
      }, this.options.timeout);
    });
  }
}

// Экспорт для opencode
export default Swen3Provider;
