import { Kafka, logLevel } from 'kafkajs';

export function createKafka({ clientId, brokers }) {
  return new Kafka({
    clientId,
    brokers,
    logLevel: logLevel.ERROR,
    connectionTimeout: 10000,
    requestTimeout: 30000,
    retry: { initialRetryTime: 500, retries: 20 },
  });
}

export async function connectProducer(kafka, { logger } = {}) {
  const producer = kafka.producer({
    idempotent: true,
    maxInFlightRequests: 5,
    allowAutoTopicCreation: false,
  });
  await producer.connect();
  logger?.info('Productor Kafka conectado');
  return producer;
}

async function confirmar(consumer, topic, partition, offset) {
  await consumer.commitOffsets([
    { topic, partition, offset: String(Number(offset) + 1) },
  ]);
}

/**
 * Consumidor generico.
 * onEvent(payload, meta) procesa un evento valido.
 * onInvalid({ raw, motivo, topic, partition, offset }) se invoca cuando el
 * mensaje no es JSON. Si onEvent lanza, el offset NO se confirma y Kafka
 * reintenta el mensaje (garantia at-least-once).
 */
export async function startConsumer({
  kafka,
  groupId,
  topics,
  fromBeginning = false,
  autoCommit = false,
  logger,
  onEvent,
  onInvalid,
}) {
  const consumer = kafka.consumer({
    groupId,
    sessionTimeout: 45000,
    heartbeatInterval: 3000,
    maxWaitTimeInMs: 1000,
    allowAutoTopicCreation: false,
  });

  await consumer.connect();
  await consumer.subscribe({ topics, fromBeginning });
  logger?.info('Consumidor suscripto', { groupId, topics, fromBeginning });

  await consumer.run({
    autoCommit,
    eachMessage: async ({ topic, partition, message }) => {
      const meta = {
        topic,
        partition,
        offset: message.offset,
        key: message.key?.toString() ?? null,
      };
      const raw = message.value ? message.value.toString() : null;

      let payload = null;
      let errorJson = null;
      if (raw === null) {
        errorJson = 'Mensaje vacio';
      } else {
        try {
          payload = JSON.parse(raw);
        } catch (error) {
          errorJson = `JSON invalido: ${error.message}`;
        }
      }

      if (errorJson) {
        if (onInvalid) {
          await onInvalid({ raw, motivo: errorJson, ...meta });
        } else {
          logger?.warn('Mensaje invalido descartado', { ...meta, motivo: errorJson });
        }
      } else {
        await onEvent(payload, meta);
      }

      if (!autoCommit) {
        await confirmar(consumer, topic, partition, message.offset);
      }
    },
  });

  return consumer;
}

export async function enviarEvento(producer, topic, key, evento) {
  await producer.send({
    topic,
    messages: [
      {
        key: key ?? evento.shipmentId ?? null,
        value: JSON.stringify(evento),
      },
    ],
  });
}
