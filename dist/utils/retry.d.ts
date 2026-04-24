export declare const retry: <T>(fn: () => Promise<T>, retries?: number, delay?: number) => Promise<T>;
