import { spawn } from "child_process";
import { ReadableStream } from "stream/web";

export interface Swen3ProviderOptions {
  zenohConnect?: string[];
  workers?: string[];
  timeout?: number;
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

// AI SDK stream part types
type TextDeltaPart = {
  type: "text-delta";
  textDelta: string;
};

type FinishPart = {
  type: "finish";
  finishReason: "stop" | "length" | "content-filter" | "tool-calls" | "error" | "other";
  usage?: {
    promptTokens: number;
    completionTokens: number;
  };
};

type StreamPart = TextDeltaPart | FinishPart;

class Swen3LanguageModel {
  readonly specificationVersion = "v1" as const;
  readonly provider = "swen3";
  readonly modelId = "swarm";
  readonly defaultObjectGenerationMode = undefined;
  readonly supportsImageUrls = false;
  readonly supportsStructuredOutputs = false;

  private options: Swen3ProviderOptions;

  constructor(options: Swen3ProviderOptions = {}) {
    this.options = {
      zenohConnect: ["tcp/10.15.64.226:7447", "tcp/10.15.66.12:7447"],
      workers: ["glm_flash", "qwen3_5_4b_opus", "jetson_gemma4b"],
      timeout: 120000,
      ...options,
    };
  }

  async doGenerate(options: any): Promise<any> {
    const stream = await this.doStream(options);
    const chunks: string[] = [];
    
    const reader = stream.stream.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value.type === "text-delta") {
        chunks.push(value.textDelta);
      }
    }

    const text = chunks.join("");
    
    return {
      text,
      finishReason: "stop" as const,
      usage: {
        promptTokens: 0,
        completionTokens: Math.ceil(text.length / 4),
      },
      rawCall: {
        rawPrompt: options.prompt,
        rawSettings: {},
      },
    };
  }

  async doStream(options: any): Promise<any> {
    const messages = options.prompt || [];
    const lastMessage = messages.filter((m: any) => m.role === "user").pop();
    const question = lastMessage?.content || "";

    const stream = new ReadableStream<StreamPart>({
      start: async (controller) => {
        try {
          controller.enqueue({
            type: "text-delta",
            textDelta: `🌐 SWEN v3 Swarm Thinking...\n\n`,
          });

          const workerStreams = this.options.workers!.map((worker) =>
            this.streamWorker(worker, question, controller)
          );

          await Promise.all(workerStreams);

          controller.enqueue({
            type: "text-delta",
            textDelta: `\n✅ Swarm complete\n`,
          });
          
          controller.enqueue({
            type: "finish",
            finishReason: "stop",
            usage: {
              promptTokens: 0,
              completionTokens: 0,
            },
          });
          
          controller.close();
        } catch (error) {
          controller.error(error);
        }
      },
    });

    return {
      stream,
      rawCall: {
        rawPrompt: options.prompt,
        rawSettings: {},
      },
    };
  }

  private async streamWorker(
    worker: string,
    question: string,
    controller: ReadableStreamDefaultController<StreamPart>
  ): Promise<void> {
    return new Promise((resolve) => {
      const venv = `${process.env.HOME}/.venvs/swen3`;
      const script = `${process.env.HOME}/.local/share/swen3/autonomous_swarm.py`;
      const python = `${venv}/bin/python3`;

      controller.enqueue({
        type: "text-delta",
        textDelta: `\n🤖 [${worker}] thinking...\n`,
      });

      const proc = spawn(python, [script, question], {
        env: {
          ...process.env,
          VIRTUAL_ENV: venv,
          PATH: `${venv}/bin:${process.env.PATH}`,
          SWEN_WORKERS: worker,
        },
      });

      proc.stdout.on("data", (data: Buffer) => {
        const text = data.toString();
        const lines = text.split("\n");
        for (const line of lines) {
          if (line.trim() && !line.startsWith("[Autonomous Swarm]")) {
            controller.enqueue({
              type: "text-delta",
              textDelta: `  ${line}\n`,
            });
          }
        }
      });

      proc.stderr.on("data", (data: Buffer) => {
        const text = data.toString();
        if (text.includes("Error")) {
          controller.enqueue({
            type: "text-delta",
            textDelta: `  ❌ Error: ${text.trim()}\n`,
          });
        }
      });

      proc.on("close", (code) => {
        try {
          if (code === 0) {
            controller.enqueue({
              type: "text-delta",
              textDelta: `  ✅ [${worker}] done\n`,
            });
          } else {
            controller.enqueue({
              type: "text-delta",
              textDelta: `  ❌ [${worker}] failed (code ${code})\n`,
            });
          }
        } catch (e) {
          // Controller already closed, ignore
        }
        resolve();
      });

      proc.on("error", (err) => {
        try {
          controller.enqueue({
            type: "text-delta",
            textDelta: `  ❌ [${worker}] error: ${err.message}\n`,
          });
        } catch (e) {
          // Controller already closed, ignore
        }
        resolve();
      });

      setTimeout(() => {
        proc.kill();
        try {
          controller.enqueue({
            type: "text-delta",
            textDelta: `  ⏱️ [${worker}] timeout\n`,
          });
        } catch (e) {
          // Controller already closed, ignore
        }
        resolve();
      }, this.options.timeout);
    });
  }
}

// Provider factory
export class Swen3Provider {
  private options: Swen3ProviderOptions;

  constructor(options: Swen3ProviderOptions = {}) {
    this.options = options;
  }

  languageModel(modelId: string): Swen3LanguageModel {
    return new Swen3LanguageModel(this.options);
  }
}

// Factory function for AI SDK compatibility
export function createSwen3Provider(options?: Swen3ProviderOptions): Swen3Provider {
  return new Swen3Provider(options);
}

// Default export
export default createSwen3Provider;
