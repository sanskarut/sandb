import {
    Db,
    DeleteResult,
    Filter,
    InsertManyResult,
    InsertOneResult,
    OptionalUnlessRequiredId,
    UpdateFilter,
    UpdateResult,
    WithId,
    Document,
    FindOptions,
    BulkWriteOptions,
    BulkWriteResult
} from "mongodb";
import { NodeState } from "../types";
import { NoNodeAvailableError } from "../utils/errors";
import { retry } from "../utils/retry";
import { MongoAdapter } from "../adapters/MongoAdapter";

export class MultiConnection {
    private nodes: NodeState[] = [];
    private lastIndex = 0;
    private maxDocumentsPerDB = Infinity;

    private connectionCache = new Map<string, Db>();
    private adapterCache = new Map<string, MongoAdapter>();
    
    async connect(
        dbs: (string | { uri: string; dbName?: string })[],
        maxDocumentsPerDB?: number
    ) {
        this.maxDocumentsPerDB = maxDocumentsPerDB ?? Infinity;
    
        const nodes: NodeState[] = [];
    
        for (const entry of dbs) {
            const uri = typeof entry === "string" ? entry : entry.uri;
            const dbName = typeof entry === "string" ? undefined : entry.dbName;
    
            try {
                let db: Db;
    
                const cacheKey = dbName ? `${uri}_${dbName}` : uri;
    
                if (this.connectionCache.has(cacheKey)) {
                    db = this.connectionCache.get(cacheKey)!;
                } else {
                    const adapter = new MongoAdapter(uri);
                    const rawDb = await adapter.connect();
    
                    db = dbName ? rawDb.client.db(dbName) : rawDb;
    
                    this.connectionCache.set(cacheKey, db);
                    this.adapterCache.set(cacheKey, adapter);
                }
    
                nodes.push({
                    db,
                    status: "online",
                    latency: 0
                });
            } catch {
                continue;
            }
        }
    
        this.nodes = nodes;
    }


    async disconnectAll() {
        for (const db of this.connectionCache.values()) {
            try {
                await db.client.close();
            } catch {}
        }
    
        this.connectionCache.clear();
        this.nodes = [];
    }
    
    private getAvailableNodes() {
        const nodes = this.nodes.filter(n => n.status === "online");
        if (!nodes.length) throw new NoNodeAvailableError();
        return nodes;
    }

    private getNextNode() {
        const nodes = this.getAvailableNodes();
        const node = nodes[this.lastIndex % nodes.length];
        this.lastIndex++;
        return node;
    }

    async insertOne<T extends Document>(
        collection: string,
        doc: OptionalUnlessRequiredId<T>
    ): Promise<InsertOneResult<T>> {
        const nodes = this.getAvailableNodes();
        for (let i = 0; i < nodes.length; i++) {
            const node = this.getNextNode();
            try {
                const count = await retry(() =>
                    node.db.collection(collection).estimatedDocumentCount()
                );
                if (count >= this.maxDocumentsPerDB) continue;
                return await retry(() =>
                    node.db.collection<T>(collection).insertOne(doc)
                );
            } catch {
                node.status = "offline";
            }
        }
        throw new Error("All nodes are full or unavailable");
    }

    async insertMany<T extends Document>(
        collection: string,
        docs: OptionalUnlessRequiredId<T>[]
    ): Promise<InsertManyResult<T>> {
        const node = this.getNextNode();
        return retry(() =>
            node.db.collection<T>(collection).insertMany(docs)
        );
    }

    async findOne<T extends Document>(
        collection: string,
        query: Filter<T>,
    ): Promise<WithId<T> | null> {
        const tasks = this.getAvailableNodes().map(node =>
            retry(() =>
                node.db.collection<T>(collection).findOne(query)
            ).catch(() => null)
        );

        const results = await Promise.all(tasks);

        for (const res of results) {
            if (res) return res;
        }

        return null;
    }

    async find<T extends Document>(
        collection: string,
        query: Filter<T>,
        options?: FindOptions
    ): Promise<WithId<T>[]> {
        const tasks = this.getAvailableNodes().map(node =>
            retry(() =>
                node.db.collection<T>(collection).find(query, options).toArray()
            ).catch(() => [])
        );
        const results = await Promise.all(tasks);
        return results.flat() as WithId<T>[];
    }

    async findPaginated<T extends Document>(
        collection: string,
        query: Filter<T>,
        page: number,
        limit: number
    ): Promise<{ data: WithId<T>[]; total: number }> {
        const skip = (page - 1) * limit;

        const [data, total] = await Promise.all([
            this.find(collection, query, { skip, limit }),
            this.countDocuments(collection, query)
        ]);

        return { data, total };
    }

    async countDocuments<T extends Document>(
        collection: string,
        query: Filter<T>
    ): Promise<number> {
        const tasks = this.getAvailableNodes().map(node =>
            retry(() =>
                node.db.collection<T>(collection).countDocuments(query)
            ).catch(() => 0)
        );
        const results = await Promise.all(tasks);
        return results.reduce((a, b) => a + b, 0);
    }

    async exists<T extends Document>(
        collection: string,
        query: Filter<T>
    ): Promise<boolean> {
        const doc = await this.findOne(collection, query);
        return !!doc;
    }

    async updateOne<T extends Document>(
        collection: string,
        query: Filter<T>,
        update: UpdateFilter<T>
    ): Promise<UpdateResult> {
        const tasks = this.getAvailableNodes().map(node =>
            retry(() =>
                node.db.collection<T>(collection).updateOne(query, update)
            ).catch(() => null)
        );
        const results = await Promise.all(tasks);
        for (const res of results) {
            if (res && res.matchedCount > 0) return res;
        }
        return {
            acknowledged: true,
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0,
            upsertedId: null
        };
    }

    async updateMany<T extends Document>(
        collection: string,
        query: Filter<T>,
        update: UpdateFilter<T>
    ): Promise<UpdateResult> {
        const tasks = this.getAvailableNodes().map(node =>
            retry(() =>
                node.db.collection<T>(collection).updateMany(query, update)
            ).catch(() => null)
        );
        const results = await Promise.all(tasks);

        let matched = 0;
        let modified = 0;

        for (const res of results) {
            if (res) {
                matched += res.matchedCount;
                modified += res.modifiedCount;
            }
        }

        return {
            acknowledged: true,
            matchedCount: matched,
            modifiedCount: modified,
            upsertedCount: 0,
            upsertedId: null
        };
    }

    async upsertOne<T extends Document>(
        collection: string,
        query: Filter<T>,
        update: UpdateFilter<T>
    ): Promise<UpdateResult> {
        const node = this.getNextNode();
        return retry(() =>
            node.db.collection<T>(collection).updateOne(query, update, { upsert: true })
        );
    }

    async deleteOne<T extends Document>(
        collection: string,
        query: Filter<T>
    ): Promise<DeleteResult> {
        const tasks = this.getAvailableNodes().map(node =>
            retry(() =>
                node.db.collection<T>(collection).deleteOne(query)
            ).catch(() => null)
        );
        const results = await Promise.all(tasks);

        for (const res of results) {
            if (res && res.deletedCount && res.deletedCount > 0) return res;
        }

        return {
            acknowledged: true,
            deletedCount: 0
        };
    }

    async deleteMany<T extends Document>(
        collection: string,
        query: Filter<T>
    ): Promise<DeleteResult> {
        const tasks = this.getAvailableNodes().map(node =>
            retry(() =>
                node.db.collection<T>(collection).deleteMany(query)
            ).catch(() => null)
        );
        const results = await Promise.all(tasks);

        let total = 0;
        for (const res of results) {
            if (res?.deletedCount) total += res.deletedCount;
        }

        return {
            acknowledged: true,
            deletedCount: total
        };
    }

    async aggregate<T extends Document>(
        collection: string,
        pipeline: object[]
    ): Promise<T[]> {
        const tasks = this.getAvailableNodes().map(node =>
            retry(() =>
                node.db.collection(collection).aggregate<T>(pipeline).toArray()
            ).catch(() => [])
        );
        const results = await Promise.all(tasks);
        return results.flat() as T[];
    }

    async distinct<T extends Document>(
        collection: string,
        field: keyof T,
        query: Filter<T>
    ): Promise<any[]> {
        const tasks = this.getAvailableNodes().map(node =>
            retry(() =>
                node.db.collection<T>(collection).distinct(field as string, query)
            ).catch(() => [])
        );
        const results = await Promise.all(tasks);
        return [...new Set(results.flat())];
    }

    async bulkWrite<T extends Document>(
        collection: string,
        operations: any[],
        options?: BulkWriteOptions
    ): Promise<BulkWriteResult> {
        const node = this.getNextNode();
        return retry(() =>
            node.db.collection<T>(collection).bulkWrite(operations, options)
        );
    }

    async dropCollection(collection: string): Promise<boolean> {
        const tasks = this.getAvailableNodes().map(node =>
            retry(() =>
                node.db.collection(collection).drop().catch(() => false)
            )
        );
        const results = await Promise.all(tasks);
        return results.some(Boolean);
    }

    async healthCheck() {
        await Promise.all(
            this.nodes.map(async node => {
                const start = Date.now();
                try {
                    await node.db.command({ ping: 1 });
                    node.status = "online";
                    node.latency = Date.now() - start;
                } catch {
                    node.status = "offline";
                }
            })
        );
    }

    async replaceOne<T extends Document>(
        collection: string,
        query: Filter<T>,
        replacement: OptionalUnlessRequiredId<T>
    ): Promise<UpdateResult> {
        const tasks = this.getAvailableNodes().map(node =>
            retry(() =>
                node.db.collection<T>(collection).replaceOne(query, replacement)
            ).catch(() => null)
        );
        const results = await Promise.all(tasks);
        for (const res of results) {
            if (res && res.matchedCount > 0) return res;
        }
        return {
            acknowledged: true,
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0,
            upsertedId: null
        };
    }

    async findOneAndUpdate<T extends Document>(
        collection: string,
        query: Filter<T>,
        update: UpdateFilter<T>
    ): Promise<WithId<T> | null> {
        const node = this.getNextNode();
        const res = await retry(() =>
            node.db.collection<T>(collection).findOneAndUpdate(query, update, { returnDocument: "after" })
        );
        return res;
    }

    async findOneAndDelete<T extends Document>(
        collection: string,
        query: Filter<T>
    ): Promise<WithId<T> | null> {
        const node = this.getNextNode();
        const res = await retry(() =>
            node.db.collection<T>(collection).findOneAndDelete(query)
        );
        return res;
    }

    async findOneAndReplace<T extends Document>(
        collection: string,
        query: Filter<T>,
        replacement: T
    ): Promise<WithId<T> | null> {
        const node = this.getNextNode();
        const res = await retry(() =>
            node.db.collection<T>(collection).findOneAndReplace(query, replacement, { returnDocument: "after" })
        );
        return res;
    }

    async estimatedDocumentCount(
        collection: string
    ): Promise<number> {
        const tasks = this.getAvailableNodes().map(node =>
            retry(() =>
                node.db.collection(collection).estimatedDocumentCount()
            ).catch(() => 0)
        );
        const results = await Promise.all(tasks);
        return results.reduce((a, b) => a + b, 0);
    }

    async createIndex<T extends Document>(
        collection: string,
        field: keyof T
    ): Promise<string[]> {
        const tasks = this.getAvailableNodes().map(node =>
            retry(() =>
                node.db.collection<T>(collection).createIndex({ [field]: 1 })
            ).catch(() => null)
        );
        const results = await Promise.all(tasks);
        return results.filter(Boolean) as string[];
    }

    async listIndexes(collection: string): Promise<any[]> {
        const node = this.getNextNode();
        return retry(() =>
            node.db.collection(collection).indexes()
        );
    }

    async dropIndex<T extends Document>(
        collection: string,
        indexName: string
    ): Promise<boolean> {
        const tasks = this.getAvailableNodes().map(node =>
            retry(() =>
                node.db.collection<T>(collection).dropIndex(indexName)
            ).catch(() => false)
        );
        const results = await Promise.all(tasks);
        return results.some(Boolean);
    }

    async stats(collection: string): Promise<any[]> {
        const tasks = this.getAvailableNodes().map(node =>
            retry(() =>
                node.db.command({ collStats: collection })
            ).catch(() => null)
        );
        const results = await Promise.all(tasks);
        return results.filter(Boolean);
    }

    async renameCollection(
        oldName: string,
        newName: string
    ): Promise<boolean> {
        const tasks = this.getAvailableNodes().map(node =>
            retry(() =>
                node.db.collection(oldName).rename(newName)
            ).catch(() => false)
        );
        const results = await Promise.all(tasks);
        return results.some(Boolean);
    }

    async listCollections(): Promise<string[]> {
        const node = this.getNextNode();
        const cols = await retry(() =>
            node.db.listCollections().toArray()
        );
        return cols.map(c => c.name);
    }

    async watch<T extends Document>(
        collection: string,
        pipeline: object[] = []
    ) {
        const node = this.getNextNode();
        return node.db.collection<T>(collection).watch(pipeline);
    }

    async transaction<T>(
        fn: (db: Db) => Promise<T>
    ): Promise<T> {
        const node = this.getNextNode();
        const session = node.db.client.startSession();

        try {
            let result: T;
            await session.withTransaction(async () => {
                result = await fn(node.db);
            });
            return result!;
        } finally {
            await session.endSession();
        }
    }

    async findOneOrFail<T extends Document>(
        collection: string,
        query: Filter<T>
    ): Promise<WithId<T>> {
        const doc = await this.findOne(collection, query);
        if (!doc) throw new Error("Document not found");
        return doc;
    }

    async insertIfNotExists<T extends Document>(
        collection: string,
        query: Filter<T>,
        doc: OptionalUnlessRequiredId<T>
    ): Promise<InsertOneResult<T> | null> {
        const exists = await this.findOne(collection, query);
        if (exists) return null;
        return this.insertOne(collection, doc);
    }

    async updateOrInsert<T extends Document>(
        collection: string,
        query: Filter<T>,
        update: UpdateFilter<T>
    ): Promise<UpdateResult> {
        return this.upsertOne(collection, query, update);
    }

    async deleteIfExists<T extends Document>(
        collection: string,
        query: Filter<T>
    ): Promise<boolean> {
        const res = await this.deleteOne(collection, query);
        return !!res.deletedCount;
    }

    async findLatest<T extends Document>(
        collection: string,
        sortField: string
    ): Promise<WithId<T> | null> {
        const node = this.getNextNode();
        return retry(() =>
            node.db.collection<T>(collection)
                .find({})
                .sort({ [sortField]: -1 })
                .limit(1)
                .next()
        );
    }

    async findOldest<T extends Document>(
        collection: string,
        sortField: string
    ): Promise<WithId<T> | null> {
        const node = this.getNextNode();
        return retry(() =>
            node.db.collection<T>(collection)
                .find({})
                .sort({ [sortField]: 1 })
                .limit(1)
                .next()
        );
    }

    async incrementField<T extends Document>(
        collection: string,
        query: Filter<T>,
        field: keyof T,
        value: number = 1
    ): Promise<UpdateResult> {
        return this.updateOne(collection, query, {
            $inc: { [field]: value } as any
        });
    }

    async pushToArray<T extends Document>(
        collection: string,
        query: Filter<T>,
        field: keyof T,
        value: any
    ): Promise<UpdateResult> {
        return this.updateOne(collection, query, {
            $push: { [field]: value } as any
        });
    }

    async pullFromArray<T extends Document>(
        collection: string,
        query: Filter<T>,
        field: keyof T,
        value: any
    ): Promise<UpdateResult> {
        return this.updateOne(collection, query, {
            $pull: { [field]: value } as any
        });
    }

    async addToSet<T extends Document>(
        collection: string,
        query: Filter<T>,
        field: keyof T,
        value: any
    ): Promise<UpdateResult> {
        return this.updateOne(collection, query, {
            $addToSet: { [field]: value } as any
        });
    }

    async unsetField<T extends Document>(
        collection: string,
        query: Filter<T>,
        field: keyof T
    ): Promise<UpdateResult> {
        return this.updateOne(collection, query, {
            $unset: { [field]: "" } as any
        });
    }

    async renameField<T extends Document>(
        collection: string,
        query: Filter<T>,
        oldField: keyof T,
        newField: string
    ): Promise<UpdateResult> {
        return this.updateOne(collection, query, {
            $rename: { [oldField]: newField } as any
        });
    }

    async cloneCollection(
        source: string,
        target: string
    ): Promise<boolean> {
        const data = await this.find(source, {});
        if (!data.length) return false;
        await this.insertMany(target, data as any);
        return true;
    }

    async clearCollection(
        collection: string
    ): Promise<DeleteResult> {
        return this.deleteMany(collection, {});
    }

    async getNodeStats() {
        return this.nodes.map(n => ({
            status: n.status,
            latency: n.latency
        }));
    }

    async reconnectNode(index: number) {
        const node = this.nodes[index];
        try {
            await node.db.command({ ping: 1 });
            node.status = "online";
        } catch {
            node.status = "offline";
        }
    }

    async findWithProjection<
        T extends Document,
        K extends Extract<keyof WithId<T>, string>
    >(
        collection: string,
        query: Filter<T>,
        fields: K[]
    ): Promise<Array<Pick<WithId<T>, K>>> {
        const projection: Record<string, 1> = {};
        for (const f of fields) projection[f] = 1;

        const tasks = this.getAvailableNodes().map(node =>
            retry(() =>
                node.db
                    .collection<T>(collection)
                    .find(query, { projection })
                    .toArray()
            ).catch(() => [] as WithId<T>[])
        );

        const results = await Promise.all(tasks);
        const flat = results.flat() as WithId<T>[];

        return flat.map(doc => {
            const out = {} as Pick<WithId<T>, K>;
            for (const f of fields) {
                (out as any)[f] = doc[f];
            }
            return out;
        });
    }

    async stream<T extends Document>(
        collection: string,
        query: Filter<T>,
        onData: (doc: WithId<T>) => void
    ): Promise<void> {
        const node = this.getNextNode();
        const cursor = node.db.collection<T>(collection).find(query);

        for await (const doc of cursor) {
            onData(doc as WithId<T>);
        }
    }

    async mapReduceLike<T extends Document, R>(
        collection: string,
        map: (doc: WithId<T>) => R,
        reduce: (acc: R, curr: R) => R,
        initial: R
    ): Promise<R> {
        const data = await this.find<T>(collection, {});
        return data.map(d => map(d)).reduce(reduce, initial);
    }

    async parallelInsert<T extends Document>(
        collection: string,
        docs: OptionalUnlessRequiredId<T>[]
    ): Promise<InsertOneResult<T>[]> {
        const tasks = docs.map(doc => this.insertOne(collection, doc));
        return Promise.all(tasks);
    }

    async shardInsert<T extends Document>(
        collection: string,
        docs: OptionalUnlessRequiredId<T>[],
        shardKey: keyof T
    ): Promise<void> {
        const nodes = this.getAvailableNodes();

        for (const doc of docs) {
            const value = (doc as any)[shardKey];
            const hash =
                typeof value === "number"
                    ? value
                    : String(value || "")
                        .split("")
                        .reduce((a, c) => a + c.charCodeAt(0), 0);

            const index = Math.abs(hash) % nodes.length;
            const node = nodes[index];

            await retry(() =>
                node.db.collection<T>(collection).insertOne(doc)
            );
        }
    }

    async rebalanceCollection<T extends Document>(
        collection: string,
        shardKey: keyof T
    ): Promise<void> {
        const allData = await this.find<T>(collection, {});
        await this.clearCollection(collection);
        await this.shardInsert(collection, allData as OptionalUnlessRequiredId<T>[], shardKey);
    }

    async backupCollection<T extends Document>(
        collection: string
    ): Promise<WithId<T>[]> {
        return this.find<T>(collection, {});
    }

    async restoreCollection<T extends Document>(
        collection: string,
        data: OptionalUnlessRequiredId<T>[]
    ): Promise<InsertManyResult<T>> {
        return this.insertMany(collection, data);
    }

    async queryWithTimeout<T extends Document>(
        collection: string,
        query: Filter<T>,
        timeout: number
    ): Promise<WithId<T>[]> {
        return Promise.race([
            this.find<T>(collection, query),
            new Promise<WithId<T>[]>((_, reject) =>
                setTimeout(() => reject(new Error("Query timeout")), timeout)
            )
        ]);
    }

    async multiAggregate<T extends Document>(
        collection: string,
        pipelines: object[][]
    ): Promise<T[][]> {
        const tasks = pipelines.map(p => this.aggregate<T>(collection, p));
        return Promise.all(tasks);
    }

    async safeExecute<T>(
        fn: () => Promise<T>
    ): Promise<{ success: boolean; data?: T; error?: unknown }> {
        try {
            const data = await fn();
            return { success: true, data };
        } catch (error) {
            return { success: false, error };
        }
    }

    async measureLatency(): Promise<number[]> {
        const latencies: number[] = [];

        await Promise.all(
            this.nodes.map(async node => {
                const start = Date.now();
                try {
                    await node.db.command({ ping: 1 });
                    const latency = Date.now() - start;
                    node.latency = latency;
                    latencies.push(latency);
                } catch {
                    node.status = "offline";
                }
            })
        );

        return latencies;
    }

    async getFastestNode(): Promise<NodeState | undefined> {
        await this.measureLatency();
        return this.nodes
            .filter(n => n.status === "online")
            .sort((a, b) => a.latency - b.latency)[0];
    }

    async executeOnFastest<T>(
        fn: (db: Db) => Promise<T>
    ): Promise<T> {
        const node = await this.getFastestNode();
        if (!node) throw new NoNodeAvailableError();
        return fn(node.db);
    }

    async lockAndUpdate<T extends Document>(
        collection: string,
        query: Filter<T>,
        update: UpdateFilter<T>
    ): Promise<boolean> {
        const node = this.getNextNode();

        const locked = await retry(() =>
            node.db.collection<T>(collection).findOneAndUpdate(
                { ...(query as any), locked: { $ne: true } },
                { $set: { locked: true } } as any
            )
        );

        if (!locked) return false;

        await retry(() =>
            node.db.collection<T>(collection).updateOne(query, update)
        );

        await retry(() =>
            node.db.collection<T>(collection).updateOne(query, {
                $unset: { locked: "" }
            } as any)
        );

        return true;
    }

    async batchProcess<T extends Document>(
        collection: string,
        query: Filter<T>,
        batchSize: number,
        handler: (docs: WithId<T>[]) => Promise<void>
    ): Promise<void> {
        const node = this.getNextNode();
        const cursor = node.db.collection<T>(collection).find(query).batchSize(batchSize);

        let batch: WithId<T>[] = [];

        for await (const doc of cursor) {
            batch.push(doc as WithId<T>);
            if (batch.length >= batchSize) {
                await handler(batch);
                batch = [];
            }
        }

        if (batch.length) {
            await handler(batch);
        }
    }

    async readPreferenceQuery<T extends Document>(
        collection: string,
        query: Filter<T>,
        mode: "primary" | "secondary" | "nearest" = "nearest"
    ): Promise<WithId<T>[]> {
        const nodes = this.getAvailableNodes();
    
        let selected: NodeState[];
    
        if (mode === "primary") {
            selected = [nodes[0]];
        } else if (mode === "secondary") {
            selected = nodes.slice(1);
        } else {
            selected = [...nodes].sort((a, b) => a.latency - b.latency);
        }
    
        const tasks = selected.map(node =>
            retry(() =>
                node.db.collection<T>(collection).find(query).toArray()
            ).catch(() => [])
        );
    
        const results = await Promise.all(tasks);
        return results.flat() as WithId<T>[];
    }
    
    async writeWithConcern<T extends Document>(
        collection: string,
        doc: OptionalUnlessRequiredId<T>,
        requiredAcks: number = 1
    ): Promise<InsertOneResult<T>> {
        const nodes = this.getAvailableNodes();
        let success = 0;
        let lastResult: InsertOneResult<T> | null = null;
    
        for (const node of nodes) {
            try {
                const res = await retry(() =>
                    node.db.collection<T>(collection).insertOne(doc)
                );
                success++;
                lastResult = res;
                if (success >= requiredAcks) return res;
            } catch {
                node.status = "offline";
            }
        }
    
        if (!lastResult) throw new Error("Write failed on all nodes");
        return lastResult;
    }
    
    async quorumUpdate<T extends Document>(
        collection: string,
        query: Filter<T>,
        update: UpdateFilter<T>,
        quorum: number = 1
    ): Promise<boolean> {
        const nodes = this.getAvailableNodes();
        let success = 0;
    
        await Promise.all(nodes.map(async node => {
            try {
                const res = await retry(() =>
                    node.db.collection<T>(collection).updateOne(query, update)
                );
                if (res.modifiedCount > 0) success++;
            } catch {
                node.status = "offline";
            }
        }));
    
        return success >= quorum;
    }
    
    async failoverExecute<T>(
        fn: (db: Db) => Promise<T>
    ): Promise<T> {
        const nodes = this.getAvailableNodes();
    
        for (const node of nodes) {
            try {
                return await retry(() => fn(node.db));
            } catch {
                node.status = "offline";
            }
        }
    
        throw new NoNodeAvailableError();
    }
    
    async consistencyCheck<T extends Document>(
        collection: string,
        query: Filter<T>
    ): Promise<boolean> {
        const nodes = this.getAvailableNodes();
    
        const results = await Promise.all(
            nodes.map(node =>
                retry(() =>
                    node.db.collection<T>(collection).find(query).toArray()
                ).catch(() => [])
            )
        );
    
        const serialized = results.map(r => JSON.stringify(r));
        return serialized.every(r => r === serialized[0]);
    }
    
    async syncMissingData<T extends Document>(
        collection: string,
        query: Filter<T>
    ): Promise<void> {
        const nodes = this.getAvailableNodes();
    
        const datasets = await Promise.all(
            nodes.map(node =>
                retry(() =>
                    node.db.collection<T>(collection).find(query).toArray()
                ).catch(() => [])
            )
        );
    
        const base = datasets[0] || [];
    
        await Promise.all(
            nodes.map(async (node, i) => {
                const current = datasets[i];
                const missing = base.filter(b =>
                    !current.some(c => (c as any)._id === (b as any)._id)
                );
    
                if (missing.length) {
                    await retry(() =>
                        node.db.collection<T>(collection).insertMany(missing as any)
                    );
                }
            })
        );
    }
    
    async circuitBreakerExecute<T>(
        fn: (db: Db) => Promise<T>,
        failureThreshold = 3,
        cooldown = 5000
    ): Promise<T> {
        for (const node of this.getAvailableNodes()) {
            try {
                const res = await retry(() => fn(node.db));
                (node as any).failures = 0;
                return res;
            } catch {
                (node as any).failures = ((node as any).failures || 0) + 1;
    
                if ((node as any).failures >= failureThreshold) {
                    node.status = "offline";
                    setTimeout(() => {
                        node.status = "online";
                        (node as any).failures = 0;
                    }, cooldown);
                }
            }
        }
    
        throw new NoNodeAvailableError();
    }
    
    async weightedQuery<T extends Document>(
        collection: string,
        query: Filter<T>
    ): Promise<WithId<T>[]> {
        const nodes = this.getAvailableNodes();
    
        const weighted = nodes
            .map(n => ({
                node: n,
                weight: 1 / (n.latency || 1)
            }))
            .sort((a, b) => b.weight - a.weight);
    
        for (const { node } of weighted) {
            try {
                const res = await retry(() =>
                    node.db.collection<T>(collection).find(query).toArray()
                );
                if (res.length) return res;
            } catch {
                node.status = "offline";
            }
        }
    
        return [];
    }
    
    async autoHealNodes(): Promise<void> {
        await Promise.all(
            this.nodes.map(async node => {
                if (node.status === "offline") {
                    try {
                        await node.db.command({ ping: 1 });
                        node.status = "online";
                    } catch {}
                }
            })
        );
    }
    
    async metrics() {
        return this.nodes.map(n => ({
            status: n.status,
            latency: n.latency
        }));
    }
    
    async adaptiveInsert<T extends Document>(
        collection: string,
        doc: OptionalUnlessRequiredId<T>
    ): Promise<InsertOneResult<T>> {
        const nodes = await this.measureLatency().then(() =>
            this.nodes
                .filter(n => n.status === "online")
                .sort((a, b) => a.latency - b.latency)
        );
    
        for (const node of nodes) {
            try {
                return await retry(() =>
                    node.db.collection<T>(collection).insertOne(doc)
                );
            } catch {
                node.status = "offline";
            }
        }
    
        throw new NoNodeAvailableError();
    }
}