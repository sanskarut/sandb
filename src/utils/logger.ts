export const logError = (err: unknown, context: string) => {
    console.error(`[Cluster:${context}]`, err);
};