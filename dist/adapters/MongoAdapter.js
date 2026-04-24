"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MongoAdapter = void 0;
const mongodb_1 = require("mongodb");
class MongoAdapter {
    constructor(uri) {
        this.uri = uri;
        this.client = new mongodb_1.MongoClient(uri);
    }
    async connect() {
        await this.client.connect();
        return this.client.db("main");
    }
    async disconnect() {
        await this.client.close();
    }
}
exports.MongoAdapter = MongoAdapter;
