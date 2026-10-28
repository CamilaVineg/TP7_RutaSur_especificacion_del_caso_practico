import { MongoClient } from 'mongodb';

const URI =
  process.env.MONGO_URI || 'mongodb://mongo:27017/rutasur?replicaSet=rs0';
const DB = process.env.MONGO_DB || 'rutasur';

// En desarrollo Next recarga los modulos: se reutiliza el cliente global.
const globalConMongo = globalThis;

export async function getDb() {
  if (!globalConMongo.__rutasurMongo) {
    globalConMongo.__rutasurMongo = (async () => {
      const cliente = new MongoClient(URI, {
        serverSelectionTimeoutMS: 10000,
      });
      await cliente.connect();
      return { cliente, db: cliente.db(DB) };
    })();
  }
  const { db } = await globalConMongo.__rutasurMongo;
  return db;
}
