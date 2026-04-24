import { NodeState } from "../types";

export class LoadBalancer {
    private lastIndex = 0;

    getNextNode(nodes: NodeState[]): NodeState {
        const available = nodes.filter(n => n.status === "online");

        if (available.length === 0) {
            throw new Error("No available nodes");
        }

        const node = available[this.lastIndex % available.length];
        this.lastIndex++;
        return node;
    }
}