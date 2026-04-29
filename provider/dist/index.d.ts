export interface Swen3ProviderOptions {
    zenohConnect?: string[];
    workers?: string[];
    timeout?: number;
}
declare class Swen3LanguageModel {
    readonly specificationVersion: "v1";
    readonly provider = "swen3";
    readonly modelId = "swarm";
    readonly defaultObjectGenerationMode: undefined;
    readonly supportsImageUrls = false;
    readonly supportsStructuredOutputs = false;
    private options;
    constructor(options?: Swen3ProviderOptions);
    doGenerate(options: any): Promise<any>;
    doStream(options: any): Promise<any>;
    private streamWorker;
}
export declare class Swen3Provider {
    private options;
    constructor(options?: Swen3ProviderOptions);
    languageModel(modelId: string): Swen3LanguageModel;
}
export declare function createSwen3Provider(options?: Swen3ProviderOptions): Swen3Provider;
export default createSwen3Provider;
//# sourceMappingURL=index.d.ts.map