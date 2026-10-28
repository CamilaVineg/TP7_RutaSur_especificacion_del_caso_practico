import { MongoClient } from 'mongodb';
import { ensureIndexes } from './indexes.js';
import { dormir } from './logger.js';

let clienteCompartido = null;

export async function connectMongo({
  uri,
  dbName,
  logger,
  intentos = 60,
  esperaMs = 2000,
}) {
  let ultimoError = null;

  for (let intento = 1; intento <= intentos; intento += 1) {
    const client = new MongoClient(uri, {
      serverSelectionTimeoutMS: 5000,
      connectTimeoutMS: 10000,
    });
    try {
      await client.connect();
      const db = client.db(dbName);
      await db.command({ ping: 1 });
      const fallosIndices = await ensureIndexes(db, logger);
      if (fallosIndices.length > 0) {
        logger?.warn('Algunos indices no se pudieron crear', { fallosIndices });
      }
      clienteCompartido = client;
      logger?.info('MongoDB conectado', { dbName });
      return { client, db };
    } catch (error) {
      ultimoError = error;
      await client.close().catch(() => {});
      logger?.warn('Reintentando conexion a MongoDB', {
        intento,
        total: intentos,
        error: error.message,
      });
      await dormir(esperaMs);
    }
  }

  throw new Error(`No se pudo conectar a MongoDB: ${ultimoError?.message}`);
}

export function clienteMongo() {
  return clienteCompartido;
}

export { ensureIndexes };
