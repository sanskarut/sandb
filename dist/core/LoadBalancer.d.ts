import { NodeState } from "../types";
export declare class LoadBalancer {
    private lastIndex;
    getNextNode(nodes: NodeState[]): NodeState;
}
