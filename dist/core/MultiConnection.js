"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MultiConnection = void 0;
const errors_1 = require("../utils/errors");
const retry_1 = require("../utils/retry");
class MultiConnection {
    constructor() {
        this.nodes = [];
        this.lastIndex = 0;
        this.maxDocumentsPerDB = Infinity;
    }
    async connect(dbs, maxDocumentsPerDB) {
        this.maxDocumentsPerDB = maxDocumentsPerDB ?? Infinity;
        this.nodes = dbs.map(db => ({
            db,
            status: "online",
            latency: 0
        }));
    }
    getAvailableNodes() {
        const nodes = this.nodes.filter(n => n.status === "online");
        if (!nodes.length)
            throw new errors_1.NoNodeAvailableError();
        return nodes;
    }
    getNextNode() {
        const nodes = this.getAvailableNodes();
        const node = nodes[this.lastIndex % nodes.length];
        this.lastIndex++;
        return node;
    }
    async insertOne(collection, doc) {
        const nodes = this.getAvailableNodes();
        for (let i = 0; i < nodes.length; i++) {
            const node = this.getNextNode();
            try {
                const count = await (0, retry_1.retry)(() => node.db.collection(collection).estimatedDocumentCount());
                if (count >= this.maxDocumentsPerDB)
                    continue;
                return await (0, retry_1.retry)(() => node.db.collection(collection).insertOne(doc));
            }
            catch {
                node.status = "offline";
            }
        }
        throw new Error("All nodes are full or unavailable");
    }
    async insertMany(collection, docs) {
        const node = this.getNextNode();
        return (0, retry_1.retry)(() => node.db.collection(collection).insertMany(docs));
    }
    async findOne(collection, query) {
        const tasks = this.getAvailableNodes().map(node => (0, retry_1.retry)(() => node.db.collection(collection).findOne(query)).catch(() => null));
        const results = await Promise.all(tasks);
        for (const res of results) {
            if (res)
                return res;
        }
        return null;
    }
    async find(collection, query, options) {
        const tasks = this.getAvailableNodes().map(node => (0, retry_1.retry)(() => node.db.collection(collection).find(query, options).toArray()).catch(() => []));
        const results = await Promise.all(tasks);
        return results.flat();
    }
    async findPaginated(collection, query, page, limit) {
        const skip = (page - 1) * limit;
        const [data, total] = await Promise.all([
            this.find(collection, query, { skip, limit }),
            this.countDocuments(collection, query)
        ]);
        return { data, total };
    }
    async countDocuments(collection, query) {
        const tasks = this.getAvailableNodes().map(node => (0, retry_1.retry)(() => node.db.collection(collection).countDocuments(query)).catch(() => 0));
        const results = await Promise.all(tasks);
        return results.reduce((a, b) => a + b, 0);
    }
    async exists(collection, query) {
        const doc = await this.findOne(collection, query);
        return !!doc;
    }
    async updateOne(collection, query, update) {
        const tasks = this.getAvailableNodes().map(node => (0, retry_1.retry)(() => node.db.collection(collection).updateOne(query, update)).catch(() => null));
        const results = await Promise.all(tasks);
        for (const res of results) {
            if (res && res.matchedCount > 0)
                return res;
        }
        return {
            acknowledged: true,
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0,
            upsertedId: null
        };
    }
    async updateMany(collection, query, update) {
        const tasks = this.getAvailableNodes().map(node => (0, retry_1.retry)(() => node.db.collection(collection).updateMany(query, update)).catch(() => null));
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
    async upsertOne(collection, query, update) {
        const node = this.getNextNode();
        return (0, retry_1.retry)(() => node.db.collection(collection).updateOne(query, update, { upsert: true }));
    }
    async deleteOne(collection, query) {
        const tasks = this.getAvailableNodes().map(node => (0, retry_1.retry)(() => node.db.collection(collection).deleteOne(query)).catch(() => null));
        const results = await Promise.all(tasks);
        for (const res of results) {
            if (res && res.deletedCount && res.deletedCount > 0)
                return res;
        }
        return {
            acknowledged: true,
            deletedCount: 0
        };
    }
    async deleteMany(collection, query) {
        const tasks = this.getAvailableNodes().map(node => (0, retry_1.retry)(() => node.db.collection(collection).deleteMany(query)).catch(() => null));
        const results = await Promise.all(tasks);
        let total = 0;
        for (const res of results) {
            if (res?.deletedCount)
                total += res.deletedCount;
        }
        return {
            acknowledged: true,
            deletedCount: total
        };
    }
    async aggregate(collection, pipeline) {
        const tasks = this.getAvailableNodes().map(node => (0, retry_1.retry)(() => node.db.collection(collection).aggregate(pipeline).toArray()).catch(() => []));
        const results = await Promise.all(tasks);
        return results.flat();
    }
    async distinct(collection, field, query) {
        const tasks = this.getAvailableNodes().map(node => (0, retry_1.retry)(() => node.db.collection(collection).distinct(field, query)).catch(() => []));
        const results = await Promise.all(tasks);
        return [...new Set(results.flat())];
    }
    async bulkWrite(collection, operations, options) {
        const node = this.getNextNode();
        return (0, retry_1.retry)(() => node.db.collection(collection).bulkWrite(operations, options));
    }
    async dropCollection(collection) {
        const tasks = this.getAvailableNodes().map(node => (0, retry_1.retry)(() => node.db.collection(collection).drop().catch(() => false)));
        const results = await Promise.all(tasks);
        return results.some(Boolean);
    }
    async healthCheck() {
        await Promise.all(this.nodes.map(async (node) => {
            const start = Date.now();
            try {
                await node.db.command({ ping: 1 });
                node.status = "online";
                node.latency = Date.now() - start;
            }
            catch {
                node.status = "offline";
            }
        }));
    }
    async replaceOne(collection, query, replacement) {
        const tasks = this.getAvailableNodes().map(node => (0, retry_1.retry)(() => node.db.collection(collection).replaceOne(query, replacement)).catch(() => null));
        const results = await Promise.all(tasks);
        for (const res of results) {
            if (res && res.matchedCount > 0)
                return res;
        }
        return {
            acknowledged: true,
            matchedCount: 0,
            modifiedCount: 0,
            upsertedCount: 0,
            upsertedId: null
        };
    }
    async findOneAndUpdate(collection, query, update) {
        const node = this.getNextNode();
        const res = await (0, retry_1.retry)(() => node.db.collection(collection).findOneAndUpdate(query, update, { returnDocument: "after" }));
        return res;
    }
    async findOneAndDelete(collection, query) {
        const node = this.getNextNode();
        const res = await (0, retry_1.retry)(() => node.db.collection(collection).findOneAndDelete(query));
        return res;
    }
    async findOneAndReplace(collection, query, replacement) {
        const node = this.getNextNode();
        const res = await (0, retry_1.retry)(() => node.db.collection(collection).findOneAndReplace(query, replacement, { returnDocument: "after" }));
        return res;
    }
    async estimatedDocumentCount(collection) {
        const tasks = this.getAvailableNodes().map(node => (0, retry_1.retry)(() => node.db.collection(collection).estimatedDocumentCount()).catch(() => 0));
        const results = await Promise.all(tasks);
        return results.reduce((a, b) => a + b, 0);
    }
    async createIndex(collection, field) {
        const tasks = this.getAvailableNodes().map(node => (0, retry_1.retry)(() => node.db.collection(collection).createIndex({ [field]: 1 })).catch(() => null));
        const results = await Promise.all(tasks);
        return results.filter(Boolean);
    }
    async listIndexes(collection) {
        const node = this.getNextNode();
        return (0, retry_1.retry)(() => node.db.collection(collection).indexes());
    }
    async dropIndex(collection, indexName) {
        const tasks = this.getAvailableNodes().map(node => (0, retry_1.retry)(() => node.db.collection(collection).dropIndex(indexName)).catch(() => false));
        const results = await Promise.all(tasks);
        return results.some(Boolean);
    }
    async stats(collection) {
        const tasks = this.getAvailableNodes().map(node => (0, retry_1.retry)(() => node.db.command({ collStats: collection })).catch(() => null));
        const results = await Promise.all(tasks);
        return results.filter(Boolean);
    }
    async renameCollection(oldName, newName) {
        const tasks = this.getAvailableNodes().map(node => (0, retry_1.retry)(() => node.db.collection(oldName).rename(newName)).catch(() => false));
        const results = await Promise.all(tasks);
        return results.some(Boolean);
    }
    async listCollections() {
        const node = this.getNextNode();
        const cols = await (0, retry_1.retry)(() => node.db.listCollections().toArray());
        return cols.map(c => c.name);
    }
    async watch(collection, pipeline = []) {
        const node = this.getNextNode();
        return node.db.collection(collection).watch(pipeline);
    }
    async transaction(fn) {
        const node = this.getNextNode();
        const session = node.db.client.startSession();
        try {
            let result;
            await session.withTransaction(async () => {
                result = await fn(node.db);
            });
            return result;
        }
        finally {
            await session.endSession();
        }
    }
    async findOneOrFail(collection, query) {
        const doc = await this.findOne(collection, query);
        if (!doc)
            throw new Error("Document not found");
        return doc;
    }
    async insertIfNotExists(collection, query, doc) {
        const exists = await this.findOne(collection, query);
        if (exists)
            return null;
        return this.insertOne(collection, doc);
    }
    async updateOrInsert(collection, query, update) {
        return this.upsertOne(collection, query, update);
    }
    async deleteIfExists(collection, query) {
        const res = await this.deleteOne(collection, query);
        return !!res.deletedCount;
    }
    async findLatest(collection, sortField) {
        const node = this.getNextNode();
        return (0, retry_1.retry)(() => node.db.collection(collection)
            .find({})
            .sort({ [sortField]: -1 })
            .limit(1)
            .next());
    }
    async findOldest(collection, sortField) {
        const node = this.getNextNode();
        return (0, retry_1.retry)(() => node.db.collection(collection)
            .find({})
            .sort({ [sortField]: 1 })
            .limit(1)
            .next());
    }
    async incrementField(collection, query, field, value = 1) {
        return this.updateOne(collection, query, {
            $inc: { [field]: value }
        });
    }
    async pushToArray(collection, query, field, value) {
        return this.updateOne(collection, query, {
            $push: { [field]: value }
        });
    }
    async pullFromArray(collection, query, field, value) {
        return this.updateOne(collection, query, {
            $pull: { [field]: value }
        });
    }
    async addToSet(collection, query, field, value) {
        return this.updateOne(collection, query, {
            $addToSet: { [field]: value }
        });
    }
    async unsetField(collection, query, field) {
        return this.updateOne(collection, query, {
            $unset: { [field]: "" }
        });
    }
    async renameField(collection, query, oldField, newField) {
        return this.updateOne(collection, query, {
            $rename: { [oldField]: newField }
        });
    }
    async cloneCollection(source, target) {
        const data = await this.find(source, {});
        if (!data.length)
            return false;
        await this.insertMany(target, data);
        return true;
    }
    async clearCollection(collection) {
        return this.deleteMany(collection, {});
    }
    async getNodeStats() {
        return this.nodes.map(n => ({
            status: n.status,
            latency: n.latency
        }));
    }
    async reconnectNode(index) {
        const node = this.nodes[index];
        try {
            await node.db.command({ ping: 1 });
            node.status = "online";
        }
        catch {
            node.status = "offline";
        }
    }
    async findWithProjection(collection, query, fields) {
        const projection = {};
        for (const f of fields)
            projection[f] = 1;
        const tasks = this.getAvailableNodes().map(node => (0, retry_1.retry)(() => node.db
            .collection(collection)
            .find(query, { projection })
            .toArray()).catch(() => []));
        const results = await Promise.all(tasks);
        const flat = results.flat();
        return flat.map(doc => {
            const out = {};
            for (const f of fields) {
                out[f] = doc[f];
            }
            return out;
        });
    }
    async stream(collection, query, onData) {
        const node = this.getNextNode();
        const cursor = node.db.collection(collection).find(query);
        for await (const doc of cursor) {
            onData(doc);
        }
    }
    async mapReduceLike(collection, map, reduce, initial) {
        const data = await this.find(collection, {});
        return data.map(d => map(d)).reduce(reduce, initial);
    }
    async parallelInsert(collection, docs) {
        const tasks = docs.map(doc => this.insertOne(collection, doc));
        return Promise.all(tasks);
    }
    async shardInsert(collection, docs, shardKey) {
        const nodes = this.getAvailableNodes();
        for (const doc of docs) {
            const value = doc[shardKey];
            const hash = typeof value === "number"
                ? value
                : String(value || "")
                    .split("")
                    .reduce((a, c) => a + c.charCodeAt(0), 0);
            const index = Math.abs(hash) % nodes.length;
            const node = nodes[index];
            await (0, retry_1.retry)(() => node.db.collection(collection).insertOne(doc));
        }
    }
    async rebalanceCollection(collection, shardKey) {
        const allData = await this.find(collection, {});
        await this.clearCollection(collection);
        await this.shardInsert(collection, allData, shardKey);
    }
    async backupCollection(collection) {
        return this.find(collection, {});
    }
    async restoreCollection(collection, data) {
        return this.insertMany(collection, data);
    }
    async queryWithTimeout(collection, query, timeout) {
        return Promise.race([
            this.find(collection, query),
            new Promise((_, reject) => setTimeout(() => reject(new Error("Query timeout")), timeout))
        ]);
    }
    async multiAggregate(collection, pipelines) {
        const tasks = pipelines.map(p => this.aggregate(collection, p));
        return Promise.all(tasks);
    }
    async safeExecute(fn) {
        try {
            const data = await fn();
            return { success: true, data };
        }
        catch (error) {
            return { success: false, error };
        }
    }
    async measureLatency() {
        const latencies = [];
        await Promise.all(this.nodes.map(async (node) => {
            const start = Date.now();
            try {
                await node.db.command({ ping: 1 });
                const latency = Date.now() - start;
                node.latency = latency;
                latencies.push(latency);
            }
            catch {
                node.status = "offline";
            }
        }));
        return latencies;
    }
    async getFastestNode() {
        await this.measureLatency();
        return this.nodes
            .filter(n => n.status === "online")
            .sort((a, b) => a.latency - b.latency)[0];
    }
    async executeOnFastest(fn) {
        const node = await this.getFastestNode();
        if (!node)
            throw new errors_1.NoNodeAvailableError();
        return fn(node.db);
    }
    async lockAndUpdate(collection, query, update) {
        const node = this.getNextNode();
        const locked = await (0, retry_1.retry)(() => node.db.collection(collection).findOneAndUpdate({ ...query, locked: { $ne: true } }, { $set: { locked: true } }));
        if (!locked)
            return false;
        await (0, retry_1.retry)(() => node.db.collection(collection).updateOne(query, update));
        await (0, retry_1.retry)(() => node.db.collection(collection).updateOne(query, {
            $unset: { locked: "" }
        }));
        return true;
    }
    async batchProcess(collection, query, batchSize, handler) {
        const node = this.getNextNode();
        const cursor = node.db.collection(collection).find(query).batchSize(batchSize);
        let batch = [];
        for await (const doc of cursor) {
            batch.push(doc);
            if (batch.length >= batchSize) {
                await handler(batch);
                batch = [];
            }
        }
        if (batch.length) {
            await handler(batch);
        }
    }
    async readPreferenceQuery(collection, query, mode = "nearest") {
        const nodes = this.getAvailableNodes();
        let selected;
        if (mode === "primary") {
            selected = [nodes[0]];
        }
        else if (mode === "secondary") {
            selected = nodes.slice(1);
        }
        else {
            selected = [...nodes].sort((a, b) => a.latency - b.latency);
        }
        const tasks = selected.map(node => (0, retry_1.retry)(() => node.db.collection(collection).find(query).toArray()).catch(() => []));
        const results = await Promise.all(tasks);
        return results.flat();
    }
    async writeWithConcern(collection, doc, requiredAcks = 1) {
        const nodes = this.getAvailableNodes();
        let success = 0;
        let lastResult = null;
        for (const node of nodes) {
            try {
                const res = await (0, retry_1.retry)(() => node.db.collection(collection).insertOne(doc));
                success++;
                lastResult = res;
                if (success >= requiredAcks)
                    return res;
            }
            catch {
                node.status = "offline";
            }
        }
        if (!lastResult)
            throw new Error("Write failed on all nodes");
        return lastResult;
    }
    async quorumUpdate(collection, query, update, quorum = 1) {
        const nodes = this.getAvailableNodes();
        let success = 0;
        await Promise.all(nodes.map(async (node) => {
            try {
                const res = await (0, retry_1.retry)(() => node.db.collection(collection).updateOne(query, update));
                if (res.modifiedCount > 0)
                    success++;
            }
            catch {
                node.status = "offline";
            }
        }));
        return success >= quorum;
    }
    async failoverExecute(fn) {
        const nodes = this.getAvailableNodes();
        for (const node of nodes) {
            try {
                return await (0, retry_1.retry)(() => fn(node.db));
            }
            catch {
                node.status = "offline";
            }
        }
        throw new errors_1.NoNodeAvailableError();
    }
    async consistencyCheck(collection, query) {
        const nodes = this.getAvailableNodes();
        const results = await Promise.all(nodes.map(node => (0, retry_1.retry)(() => node.db.collection(collection).find(query).toArray()).catch(() => [])));
        const serialized = results.map(r => JSON.stringify(r));
        return serialized.every(r => r === serialized[0]);
    }
    async syncMissingData(collection, query) {
        const nodes = this.getAvailableNodes();
        const datasets = await Promise.all(nodes.map(node => (0, retry_1.retry)(() => node.db.collection(collection).find(query).toArray()).catch(() => [])));
        const base = datasets[0] || [];
        await Promise.all(nodes.map(async (node, i) => {
            const current = datasets[i];
            const missing = base.filter(b => !current.some(c => c._id === b._id));
            if (missing.length) {
                await (0, retry_1.retry)(() => node.db.collection(collection).insertMany(missing));
            }
        }));
    }
    async circuitBreakerExecute(fn, failureThreshold = 3, cooldown = 5000) {
        for (const node of this.getAvailableNodes()) {
            try {
                const res = await (0, retry_1.retry)(() => fn(node.db));
                node.failures = 0;
                return res;
            }
            catch {
                node.failures = (node.failures || 0) + 1;
                if (node.failures >= failureThreshold) {
                    node.status = "offline";
                    setTimeout(() => {
                        node.status = "online";
                        node.failures = 0;
                    }, cooldown);
                }
            }
        }
        throw new errors_1.NoNodeAvailableError();
    }
    async weightedQuery(collection, query) {
        const nodes = this.getAvailableNodes();
        const weighted = nodes
            .map(n => ({
            node: n,
            weight: 1 / (n.latency || 1)
        }))
            .sort((a, b) => b.weight - a.weight);
        for (const { node } of weighted) {
            try {
                const res = await (0, retry_1.retry)(() => node.db.collection(collection).find(query).toArray());
                if (res.length)
                    return res;
            }
            catch {
                node.status = "offline";
            }
        }
        return [];
    }
    async autoHealNodes() {
        await Promise.all(this.nodes.map(async (node) => {
            if (node.status === "offline") {
                try {
                    await node.db.command({ ping: 1 });
                    node.status = "online";
                }
                catch { }
            }
        }));
    }
    async metrics() {
        return this.nodes.map(n => ({
            status: n.status,
            latency: n.latency
        }));
    }
    async adaptiveInsert(collection, doc) {
        const nodes = await this.measureLatency().then(() => this.nodes
            .filter(n => n.status === "online")
            .sort((a, b) => a.latency - b.latency));
        for (const node of nodes) {
            try {
                return await (0, retry_1.retry)(() => node.db.collection(collection).insertOne(doc));
            }
            catch {
                node.status = "offline";
            }
        }
        throw new errors_1.NoNodeAvailableError();
    }
}
exports.MultiConnection = MultiConnection;
