import { Db } from "mongodb";
export type NodeStatus = "online" | "offline";
export type NodeState = {
    db: Db;
    status: NodeStatus;
    latency: number;
};
export type ClusterOptions = {
    maxDocumentsPerDB?: number;
    retryAttempts?: number;
};
