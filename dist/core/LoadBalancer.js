"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.LoadBalancer = void 0;
class LoadBalancer {
    constructor() {
        this.lastIndex = 0;
    }
    getNextNode(nodes) {
        const available = nodes.filter(n => n.status === "online");
        if (available.length === 0) {
            throw new Error("No available nodes");
        }
        const node = available[this.lastIndex % available.length];
        this.lastIndex++;
        return node;
    }
}
exports.LoadBalancer = LoadBalancer;
