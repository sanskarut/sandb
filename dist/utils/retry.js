"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.retry = void 0;
const retry = async (fn, retries = 3, delay = 200) => {
    try {
        return await fn();
    }
    catch (err) {
        if (retries <= 0)
            throw err;
        await new Promise(res => setTimeout(res, delay));
        return (0, exports.retry)(fn, retries - 1, delay * 2);
    }
};
exports.retry = retry;
