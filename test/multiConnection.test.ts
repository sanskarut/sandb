import { MultiConnection } from "../src";
import { MongoAdapter } from "../src/adapters/MongoAdapter";

async function test(){
        const dburl = "mongodb+srv://root:root@snaksarut.eap3t0l.mongodb.net/?appName=snaksarut";

        const db = new MongoAdapter(dburl);
        const c = await db.connect()
        const res = await c.collection("users").find({}).toArray();
        console.log("working:",res)


        const mdc = new MultiConnection();
        await mdc.connect([dburl]);
        const res2 = (await mdc.find("users", {})).flat();
        console.log("working 2:",res2)

        await mdc.connect([{
            uri: dburl,
            dbName: "users"
        }]);

        const res3 = (await mdc.find("users", {})).flat();
        console.log("working 3:",res3)

}

test();