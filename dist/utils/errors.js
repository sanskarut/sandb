"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ClusterFullError = exports.NoNodeAvailableError = exports.ClusterError = void 0;
class ClusterError extends Error {
    constructor(message) {
        super(message);
        this.name = "ClusterError";
    }
}
exports.ClusterError = ClusterError;
class NoNodeAvailableError extends Error {
    constructor() {
        super("No available database nodes");
        this.name = "NoNodeAvailableError";
    }
}
exports.NoNodeAvailableError = NoNodeAvailableError;
class ClusterFullError extends Error {
    constructor() {
        super("All database nodes are full");
        this.name = "ClusterFullError";
    }
}
exports.ClusterFullError = ClusterFullError;
