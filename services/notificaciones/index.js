import { randomUUID } from 'node:crypto';

import { GROUPS, TOPICS, cfg } from '../shared/config.js';
import { createLogger } from '../shared/logger.js';
import { connectMongo } from '../shared/mongo.js';
import {
  createKafka,
  connectProducer,
  startConsumer,
} from '../shared/kafka.js';
import { registrarEventoProcesado } from '../shared/indexes.js';
import { validarAlerta, validarEstado } from '../shared/events.js';

const logger = createLogger('notificaciones');

const { db } = await connectMongo({ uri: cfg.mongoUri, dbName: cfg.dbName, logger });
const kafka = createKafka({
  clientId: 'rutasur-notificaciones',
  brokers: cfg.kafkaBrokers,
});
const productor = await connectProducer(kafka, { logger });

function textoFallo(evt) {
  const motivos = {
    AUSENTE: 'no habia nadie en el domicilio',
    DIRECCION_INCORRECTA: 'la direccion no coincide con la registrada',
    RECHAZADO: 'el destinatario rechazo el paquete',
    ZONA_INACCESIBLE: 'la zona no era accesible en el momento del reparto',
  };
  const detalle = motivos[evt.motivo] ?? 'no pudimos completar la entrega';
  return `No pudimos entregar tu envio ${evt.shipmentId}: ${detalle}. Podes reprogramar la entrega o corregir los datos del domicilio.`;
}

async function notificarFallo(evt, meta) {
  const yaProcesado = await registrarEventoProcesado(
    db,
    GROUPS.NOTIFICACIONES,
    evt.eventId,
  );
  if (yaProcesado) {
    logger.info('Notificacion ya emitida para el evento', {
      eventId: evt.eventId,
      topic: meta.topic,
      partition: meta.partition,
      offset: meta.offset,
    });
    return;
  }

  const notificacion = {
    eventId: randomUUID(),
    shipmentId: evt.shipmentId,
    ocurridoEn: new Date().toISOString(),
    version: 1,
    canal: 'EMAIL_SIMULADO',
    tipo: 'ENTREGA_FALLIDA',
    motivo: evt.motivo,
    mensaje: textoFallo(evt),
    opciones: [
      'REPROGRAMAR_ENTREGA',
      'CORREGIR_DIRECCION',
      'CONTACTAR_AL_TRANSPORTISTA',
    ],
    origenEventId: evt.eventId,
  };

  await productor.send({
    topic: TOPICS.NOTIFICACIONES,
    messages: [
      {
        key: evt.shipmentId,
        value: JSON.stringify(notificacion),
      },
    ],
  });

  logger.info('Notificacion por entrega fallida emitida', {
    eventId: notificacion.eventId,
    origenEventId: evt.eventId,
    shipmentId: evt.shipmentId,
    partition: meta.partition,
    offset: meta.offset,
  });
}

async function notificarRiesgo(evt, meta) {
  const yaProcesado = await registrarEventoProcesado(
    db,
    GROUPS.NOTIFICACIONES,
    evt.eventId,
  );
  if (yaProcesado) return;

  const notificacion = {
    eventId: randomUUID(),
    shipmentId: evt.shipmentId,
    ocurridoEn: new Date().toISOString(),
    version: 1,
    canal: 'EMAIL_SIMULADO',
    tipo: 'RIESGO_SLA',
    prioridad: evt.prioridad,
    mensaje: `Tu envio ${evt.shipmentId} esta en riesgo de no cumplir la fecha prometida. Estamos trabajando para entregarlo a tiempo.`,
    opciones: ['VER_SEGUIMIENTO'],
    origenEventId: evt.eventId,
  };

  await productor.send({
    topic: TOPICS.NOTIFICACIONES,
    messages: [
      {
        key: evt.shipmentId,
        value: JSON.stringify(notificacion),
      },
    ],
  });

  logger.info('Notificacion por riesgo de SLA emitida', {
    eventId: notificacion.eventId,
    origenEventId: evt.eventId,
    shipmentId: evt.shipmentId,
    prioridad: evt.prioridad,
    partition: meta.partition,
    offset: meta.offset,
  });
}

const consumidor = await startConsumer({
  kafka,
  groupId: GROUPS.NOTIFICACIONES,
  topics: [TOPICS.ESTADOS, TOPICS.ALERTAS],
  logger,
  onInvalid: ({ raw, motivo, ...meta }) => {
    logger.warn('Mensaje invalido descartado', { ...meta, motivo });
  },
  onEvent: async (evento, meta) => {
    if (meta.topic === TOPICS.ESTADOS) {
      const validacion = validarEstado(evento);
      if (!validacion.ok) return;
      const evt = validacion.valor;
      if (evt.tipo === 'ENTREGA_FALLIDA') {
        await notificarFallo(evt, meta);
      }
      return;
    }

    if (meta.topic === TOPICS.ALERTAS) {
      const validacion = validarAlerta(evento);
      if (!validacion.ok) return;
      const evt = validacion.valor;
      if (evt.abierta) {
        await notificarRiesgo(evt, meta);
      }
    }
  },
});

logger.info('Servicio de notificaciones listo', {
  groupId: GROUPS.NOTIFICACIONES,
});

process.on('SIGTERM', async () => {
  logger.info('Deteniendo notificaciones');
  await consumidor.disconnect().catch(() => {});
  await productor.disconnect().catch(() => {});
  process.exit(0);
});

process.on('unhandledRejection', (error) => {
  logger.error('Promesa rechazada sin manejar', {
    error: error?.message ?? String(error),
    stack: error?.stack,
  });
});
