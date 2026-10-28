import { randomUUID } from 'node:crypto';

import {
  FRANJA_OPUESTA,
  GROUPS,
  TOPICS,
  cfg,
  franjaDe,
  proximoIntento,
} from '../shared/config.js';
import { createLogger } from '../shared/logger.js';
import { connectMongo } from '../shared/mongo.js';
import {
  createKafka,
  connectProducer,
  startConsumer,
} from '../shared/kafka.js';
import { validarEstado } from '../shared/events.js';

const logger = createLogger('reprogramacion');

const { db } = await connectMongo({ uri: cfg.mongoUri, dbName: cfg.dbName, logger });
const kafka = createKafka({
  clientId: 'rutasur-reprogramacion',
  brokers: cfg.kafkaBrokers,
});
const productor = await connectProducer(kafka, { logger });
const estado = db.collection('reprog_envios');

function decidir({ motivo, intento }) {
  if (intento >= 3) {
    return { decision: 'DEVOLUCION', detalle: 'tercer intento fallido' };
  }
  if (motivo === 'RECHAZADO') {
    return {
      decision: 'DEVOLUCION',
      detalle: 'el destinatario rechazo el paquete',
    };
  }
  if (motivo === 'DIRECCION_INCORRECTA') {
    return {
      decision: 'RETENIDO',
      detalle: 'retenido hasta que se corrija la direccion',
    };
  }
  return {
    decision: 'REPROGRAMADO',
    detalle:
      motivo === 'ZONA_INACCESIBLE'
        ? 'zona inaccesible: se reintenta el dia habil siguiente en la franja opuesta'
        : 'reprogramado para el dia habil siguiente en la franja opuesta',
  };
}

async function manejarFallo(evt, meta) {
  const doc = await estado.findOne({ shipmentId: evt.shipmentId });

  if (doc && evt.secuencia <= (doc.ultimaSecuencia ?? -1)) {
    logger.info('Fallo duplicado o fuera de orden descartado', {
      eventId: evt.eventId,
      shipmentId: evt.shipmentId,
      partition: meta.partition,
      offset: meta.offset,
    });
    return;
  }

  const fallos = (doc?.fallos ?? 0) + 1;
  const intento = evt.intento ?? fallos;
  const franjaFallo = franjaDe(evt.ocurridoEn);
  const { decision, detalle } = decidir({ motivo: evt.motivo, intento });

  const reprogramacion = {
    eventId: randomUUID(),
    shipmentId: evt.shipmentId,
    ocurridoEn: new Date().toISOString(),
    version: 1,
    motivo: evt.motivo,
    intento,
    decision,
    franjaFallo,
    proximoIntentoEn:
      decision === 'REPROGRAMADO'
        ? proximoIntento(evt.ocurridoEn, franjaFallo).toISOString()
        : null,
    detalle,
  };

  await productor.send({
    topic: TOPICS.REPROGRAMACIONES,
    messages: [
      {
        key: evt.shipmentId,
        value: JSON.stringify(reprogramacion),
      },
    ],
  });

  await estado.updateOne(
    { shipmentId: evt.shipmentId },
    {
      $set: {
        shipmentId: evt.shipmentId,
        ultimaSecuencia: evt.secuencia,
        fallos,
        motivo: evt.motivo,
        intento,
        decision,
        proximoIntentoEn: reprogramacion.proximoIntentoEn,
        actualizadoEn: new Date(),
      },
    },
    { upsert: true },
  );

  logger.info('Reprogramacion emitida', {
    eventId: reprogramacion.eventId,
    origenEventId: evt.eventId,
    shipmentId: evt.shipmentId,
    motivo: evt.motivo,
    intento,
    decision,
    franjaFallo,
    franjaOpuesta: FRANJA_OPUESTA[franjaFallo],
    proximoIntentoEn: reprogramacion.proximoIntentoEn,
    partition: meta.partition,
    offset: meta.offset,
  });
}

const consumidor = await startConsumer({
  kafka,
  groupId: GROUPS.REPROGRAMACION,
  topics: [TOPICS.ESTADOS],
  logger,
  onInvalid: ({ raw, motivo, ...meta }) => {
    logger.warn('Mensaje invalido descartado', { ...meta, motivo });
  },
  onEvent: async (evento, meta) => {
    const validacion = validarEstado(evento);
    if (!validacion.ok) return;
    const evt = validacion.valor;
    if (evt.tipo !== 'ENTREGA_FALLIDA') return;
    await manejarFallo(evt, meta);
  },
});

logger.info('Servicio de reprogramacion listo', {
  groupId: GROUPS.REPROGRAMACION,
});

process.on('SIGTERM', async () => {
  logger.info('Deteniendo reprogramacion');
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
