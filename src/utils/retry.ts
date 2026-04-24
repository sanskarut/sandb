export const retry = async <T>(
    fn: () => Promise<T>,
    retries = 3,
    delay = 200
): Promise<T> => {
    try {
        return await fn();
    } catch (err) {
        if (retries <= 0) throw err;
        await new Promise(res => setTimeout(res, delay));
        return retry(fn, retries - 1, delay * 2);
    }
};