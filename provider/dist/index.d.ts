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
export declare class Swen3Provider {
    private options;
    constructor(options?: Swen3ProviderOptions);
    generate(messages: Message[], options?: {
        signal?: AbortSignal;
    }): Promise<ReadableStream<Uint8Array>>;
    private streamWorker;
}
export default Swen3Provider;
//# sourceMappingURL=index.d.ts.map