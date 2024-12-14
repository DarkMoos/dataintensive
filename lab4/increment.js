const { MongoClient } = require("mongodb");

// Налаштування підключення до Primary (mongo2)
const uri = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=myReplicaSet";
const dbName = "likesDB";
const collectionName = "likesCounter";

// Функція для інкрементування каунтера
async function incrementCounter(clientId, iterations = 10000) {
    const client = new MongoClient(uri);
    await client.connect();

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    for (let i = 0; i < iterations; i++) {
        await collection.findOneAndUpdate(
            { _id: "likeCounter" },
            { $inc: { count: 1 } },
            { writeConcern: { w: "majority" } }  // або 1 для writeconserne=1 
        );
    }

    console.log(`Клієнт ${clientId} завершив роботу.`);
    await client.close();
}

// Функція для запуску 10 клієнтів одночасно
async function runConcurrentClients() {
    const clients = [];
    const clientCount = 10;

    console.log("Запускаємо клієнтів...");

    const startTime = Date.now(); // Початок вимірювання часу

    for (let i = 0; i < clientCount; i++) {
        clients.push(incrementCounter(i + 1));
    }

    await Promise.all(clients);

    const endTime = Date.now(); // Кінець вимірювання часу
    console.log(`Усі клієнти завершили роботу.`);
    console.log(`Час виконання: ${(endTime - startTime) / 1000} секунд`);

    // Після завершення перевіряємо значення каунтера
    const client = new MongoClient(uri);
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const result = await collection.findOne({ _id: "likeCounter" });
    console.log(`Кінцеве значення каунтера: ${result.count}`);
    await client.close();
}

// Запускаємо тест
runConcurrentClients().catch(console.error);
