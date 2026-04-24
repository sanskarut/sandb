"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.logError = void 0;
const logError = (err, context) => {
    console.error(`[Cluster:${context}]`, err);
};
exports.logError = logError;
