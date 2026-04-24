import { Db } from "mongodb";
export declare class MongoAdapter {
    private uri;
    private client;
    constructor(uri: string);
    connect(): Promise<Db>;
    disconnect(): Promise<void>;
}
