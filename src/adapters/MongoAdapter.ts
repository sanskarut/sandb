import { MongoClient, Db } from "mongodb";

export class MongoAdapter {
    private client: MongoClient;

    constructor(private uri: string) {
        this.client = new MongoClient(uri);
    }

    async connect(): Promise<Db> {
        await this.client.connect();
        return this.client.db("main");
    }

    async disconnect() {
        await this.client.close();
    }
}