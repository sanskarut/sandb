# Multi Mongo Cluster

A lightweight **multi-database MongoDB connection manager** with load balancing, failover, retry logic, and distributed query support.

---

## 🚀 Features

* Multiple MongoDB connections
* Automatic load balancing (round-robin + latency-based)
* Failover handling (auto skip offline nodes)
* Retry mechanism for unstable connections
* Distributed reads (query across all nodes)
* Write strategies (single node / quorum)
* Aggregation across nodes
* Health check & latency tracking
* Circuit breaker support
* Sharding (basic hash-based distribution)

---

## 📦 Installation

```bash
npm install multi-mongo-cluster
```

---

## ⚙️ Basic Usage

```ts
import { MongoClient } from "mongodb";
import { MultiConnection } from "multi-mongo-cluster";

async function main() {
    const client1 = await new MongoClient("mongodb://localhost:27017").connect();
    const client2 = await new MongoClient("mongodb://localhost:27018").connect();

    const db1 = client1.db("test");
    const db2 = client2.db("test");

    const cluster = new MultiConnection({
        readPreference: "nearest",
        retryAttempts: 3
    });

    await cluster.connect([db1, db2]);

    await cluster.insertOne("users", {
        name: "Sanskar",
        email: "sanskar@gmail.com"
    });

    const user = await cluster.findOne("users", { name: "Sanskar" });

    console.log(user);
}

main();
```

---

## 📘 API Overview

### 🔹 Connection

```ts
connect(dbs: Db[], maxDocumentsPerDB?: number)
```

---

### 🔹 Insert

```ts
insertOne(collection, document)
insertMany(collection, documents)
```

---

### 🔹 Find

```ts
findOne(collection, query)
find(collection, query)
findPaginated(collection, query, page, limit)
```

---

### 🔹 Update

```ts
updateOne(collection, query, update)
updateMany(collection, query, update)
upsertOne(collection, query, update)
```

---

### 🔹 Delete

```ts
deleteOne(collection, query)
deleteMany(collection, query)
```

---

### 🔹 Aggregation

```ts
aggregate(collection, pipeline)
```

---

### 🔹 Advanced

```ts
readPreferenceQuery(collection, query, mode)
writeWithConcern(collection, doc, requiredAcks)
quorumUpdate(collection, query, update, quorum)
failoverExecute(fn)
circuitBreakerExecute(fn)
```

---

### 🔹 Utilities

```ts
healthCheck()
measureLatency()
getFastestNode()
metrics()
```

---

## 🧠 Read Preferences

```ts
"primary"   // first node
"secondary" // other nodes
"nearest"   // lowest latency
```

---

## ⚡ Example: Distributed Query

```ts
const users = await cluster.find("users", {});
```

→ Queries all nodes and merges results

---

## ⚡ Example: Quorum Write

```ts
await cluster.writeWithConcern("users", data, 2);
```

→ Requires at least 2 successful writes

---

## ⚠️ Limitations

* No built-in replication (Mongo handles that)
* No distributed transactions across nodes
* Basic sharding strategy (not production-grade)

---

## 🛠️ Use Cases

* Multi-database apps
* Horizontal scaling experiments
* High availability systems
* Custom backend infrastructure

---

## 📌 Roadmap

* Advanced load balancing strategies
* Plugin system
* Query caching
* Schema validation layer
* Observability tools

---

## 🤝 Contributing

Pull requests are welcome. For major changes, open an issue first.

---

## 📄 License

MIT
