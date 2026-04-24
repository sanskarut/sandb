export class ClusterError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "ClusterError";
    }
}

export class NoNodeAvailableError extends Error {
    constructor() {
        super("No available database nodes");
        this.name = "NoNodeAvailableError";
    }
}

export class ClusterFullError extends Error {
    constructor() {
        super("All database nodes are full");
        this.name = "ClusterFullError";
    }
}