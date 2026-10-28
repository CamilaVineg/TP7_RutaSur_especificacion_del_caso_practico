import { randomUUID } from 'node:crypto';

import {
  GROUPS,
  PRIORIDAD_ORDEN,
  TOPICS,
  cfg,
} from '../shared/config.js';
import { createLogger } from '../shared/logger.js';
import { connectMongo } from '../shared/mongo.js';
import {
  createKafka,
  connectProducer,
  startConsumer,
} from '../shared/kafka.js';
import {
  validarAlerta,
  validarEstado,
  validarNotificacion,
  validarReprogramacion,
} from '../shared/events.js';

const logger = createLogger('proyector');

const { db } = await connectMongo({ uri: cfg.mongoUri, dbName: cfg.dbName, logger });
const kafka = createKafka({ clientId: 'rutasur-proyector', brokers: cfg.kafkaBrokers });
const productor = await connectProducer(kafka, { logger });

const envios = db.collection('envios');
const alertas = db.collection('alertas');
const notificaciones = db.collection('notificaciones');

function metaLog(evento, meta) {
  return {
    eventId: evento?.eventId,
    shipmentId: evento?.shipmentId,
    topic: meta.topic,
    partition: meta.partition,
    offset: meta.offset,
  };
}

/**
 * DLQ: el proyector centraliza las escrituras para no duplicar entradas.
 * El topico admite escrituras de cualquier consumidor por diseno.
 */
async function enviarADlq({ motivo, raw, meta, evento }) {
  const registro = {
    eventId: evento?.eventId ?? null,
    shipmentId: evento?.shipmentId ?? null,
    motivo,
    servicio: 'proyector',
    topic: meta.topic,
    partition: meta.partition,
    offset: meta.offset,
    payload: String(raw ?? '').slice(0, 4000),
    detectadoEn: new Date(),
  };

  await productor.send({
    topic: TOPICS.DLQ,
    messages: [
      {
        key: registro.shipmentId ?? 'dlq',
        value: JSON.stringify(registro),
      },
    ],
  });

  await db.collection('dlq').insertOne(registro);
  logger.warn('Evento derivado a DLQ', {
    ...metaLog(evento, meta),
    motivo,
  });
}

// ---------------------------------------------------------------- CU1: proyeccion de estados
async function manejarEstado(evento, meta) {
  const validacion = validarEstado(evento);
  if (!validacion.ok) {
    await enviarADlq({
      motivo: validacion.motivo,
      raw: JSON.stringify(evento),
      meta,
      evento,
    });
    return;
  }
  const evt = validacion.valor;

  const doc = await envios.findOne({ shipmentId: evt.shipmentId });
  if (doc) {
    if ((doc.historial ?? []).some((h) => h.eventId === evt.eventId)) {
      logger.info('Evento duplicado descartado', metaLog(evt, meta));
      return;
    }
    if (evt.secuencia <= (doc.ultimaSecuencia ?? -1)) {
      logger.info('Evento fuera de orden descartado', {
        ...metaLog(evt, meta),
        secuenciaRecibida: evt.secuencia,
        secuenciaAplicada: doc.ultimaSecuencia,
      });
      return;
    }
    if (doc.estado === 'ENTREGADO') {
      logger.info('Evento ignorado: el envio ya fue entregado', metaLog(evt, meta));
      return;
    }
  }

  const ocurridoEn = new Date(evt.ocurridoEn);
  const set = {
    estado: evt.tipo,
    ultimaSecuencia: evt.secuencia,
    ultimoEventoEn: ocurridoEn,
    actualizadoEn: new Date(),
    version: evt.version,
  };

  if (evt.hubId) set.hubId = evt.hubId;
  if (evt.zonaId) set.zonaId = evt.zonaId;
  if (evt.repartidorId) set.repartidorId = evt.repartidorId;
  if (evt.promesaVenceEn) set['promesa.venceEn'] = new Date(evt.promesaVenceEn);
  if (evt.intento) set.intentoActual = evt.intento;

  const inc = {};

  if (evt.tipo === 'ENTREGADO') {
    set.entregadoEn = ocurridoEn;
    set.entregadoPrimerIntento = (evt.intento ?? 1) === 1;
    set.terminal = true;
  }

  if (evt.tipo === 'ENTREGA_FALLIDA') {
    set.ultimoMotivo = evt.motivo;
    set.terminal = (evt.intento ?? 1) >= 3 || evt.motivo === 'RECHAZADO';
    inc.intentosFallidos = 1;
  }

  const entradaHistorial = {
    eventId: evt.eventId,
    tipo: evt.tipo,
    ocurridoEn,
    secuencia: evt.secuencia,
    motivo: evt.motivo ?? null,
    intento: evt.intento ?? null,
    hubId: evt.hubId ?? null,
    zonaId: evt.zonaId ?? null,
    repartidorId: evt.repartidorId ?? null,
  };

  const update = {
    $set: set,
    $push: { historial: entradaHistorial },
    $setOnInsert: {
      shipmentId: evt.shipmentId,
      creadoEn: new Date(),
    },
  };
  if (Object.keys(inc).length > 0) update.$inc = inc;

  await envios.updateOne({ shipmentId: evt.shipmentId }, update, { upsert: true });

  logger.info('Estado proyectado', {
    ...metaLog(evt, meta),
    tipo: evt.tipo,
    secuencia: evt.secuencia,
  });
}

// ---------------------------------------------------------------- Reprogramaciones
async function manejarReprogramacion(evento, meta) {
  const validacion = validarReprogramacion(evento);
  if (!validacion.ok) {
    logger.warn('Reprogramacion invalida descartada', {
      ...metaLog(evento, meta),
      motivo: validacion.motivo,
    });
    return;
  }
  const evt = validacion.valor;

  const entrada = {
    eventId: evt.eventId,
    ocurridoEn: new Date(evt.ocurridoEn),
    motivo: evt.motivo,
    intento: evt.intento,
    decision: evt.decision,
    proximoIntentoEn: evt.proximoIntentoEn
      ? new Date(evt.proximoIntentoEn)
      : null,
    franjaFallo: evt.franjaFallo ?? null,
  };

  const set = {
    actualizadoEn: new Date(),
    decisionReprogramacion: evt.decision,
    ultimaReprogramacionEn: new Date(evt.ocurridoEn),
  };
  if (evt.proximoIntentoEn) {
    set.proximoIntentoEn = new Date(evt.proximoIntentoEn);
  }
  if (evt.decision === 'DEVOLUCION') {
    set.devolucion = true;
    set.terminal = true;
  }

  try {
    await envios.updateOne(
      {
        shipmentId: evt.shipmentId,
        'reprogramaciones.eventId': { $ne: evt.eventId },
      },
      {
        $push: { reprogramaciones: entrada },
        $set: set,
        $setOnInsert: {
          shipmentId: evt.shipmentId,
          creadoEn: new Date(),
          estado: 'ENTREGA_FALLIDA',
          historial: [],
        },
      },
      { upsert: true },
    );
    logger.info('Reprogramacion proyectada', {
      ...metaLog(evt, meta),
      decision: evt.decision,
    });
  } catch (error) {
    if (error?.code === 11000) {
      logger.info('Reprogramacion duplicada descartada', metaLog(evt, meta));
      return;
    }
    throw error;
  }
}

// ---------------------------------------------------------------- Alertas de SLA
async function manejarAlerta(evento, meta) {
  const validacion = validarAlerta(evento);
  if (!validacion.ok) {
    logger.warn('Alerta invalida descartada', {
      ...metaLog(evento, meta),
      motivo: validacion.motivo,
    });
    return;
  }
  const evt = validacion.valor;

  const base = {
    shipmentId: evt.shipmentId,
    causa: evt.causa,
    prioridad: evt.prioridad,
    prioridadOrden: PRIORIDAD_ORDEN[evt.prioridad] ?? 9,
    venceEn: evt.venceEn ? new Date(evt.venceEn) : null,
    mensaje: evt.mensaje ?? null,
    hubId: evt.hubId ?? null,
    zonaId: evt.zonaId ?? null,
    actualizadoEn: new Date(),
    alertaId: evt.eventId,
  };

  if (evt.abierta) {
    await alertas.updateOne(
      { shipmentId: evt.shipmentId, causa: evt.causa },
      {
        $set: {
          ...base,
          estado: 'ABIERTA',
          abiertaEn: new Date(evt.ocurridoEn),
        },
        $setOnInsert: { creadoEn: new Date() },
        $unset: { cerradaEn: '' },
      },
      { upsert: true },
    );
  } else {
    await alertas.updateOne(
      { shipmentId: evt.shipmentId, causa: evt.causa },
      {
        $set: {
          ...base,
          estado: 'CERRADA',
          cerradaEn: new Date(evt.ocurridoEn),
        },
        $setOnInsert: {
          creadoEn: new Date(),
          abiertaEn: new Date(evt.ocurridoEn),
        },
      },
      { upsert: true },
    );
  }

  logger.info('Alerta proyectada', {
    ...metaLog(evt, meta),
    causa: evt.causa,
    prioridad: evt.prioridad,
    abierta: evt.abierta,
  });
}

// ---------------------------------------------------------------- Notificaciones
async function manejarNotificacion(evento, meta) {
  const validacion = validarNotificacion(evento);
  if (!validacion.ok) {
    logger.warn('Notificacion invalida descartada', {
      ...metaLog(evento, meta),
      motivo: validacion.motivo,
    });
    return;
  }
  const evt = validacion.valor;

  await notificaciones.updateOne(
    { notificacionId: evt.eventId },
    {
      $setOnInsert: {
        notificacionId: evt.eventId,
        shipmentId: evt.shipmentId,
        canal: evt.canal,
        tipo: evt.tipo,
        mensaje: evt.mensaje,
        opciones: evt.opciones ?? [],
        motivo: evt.motivo ?? null,
        prioridad: evt.prioridad ?? null,
        origenEventId: evt.origenEventId ?? null,
        enviadaEn: new Date(evt.ocurridoEn),
        creadoEn: new Date(),
      },
    },
    { upsert: true },
  );

  logger.info('Notificacion proyectada', metaLog(evt, meta));
}

// ---------------------------------------------------------------- Consumidor
const consumidor = await startConsumer({
  kafka,
  groupId: GROUPS.PROYECTOR,
  topics: [
    TOPICS.ESTADOS,
    TOPICS.REPROGRAMACIONES,
    TOPICS.ALERTAS,
    TOPICS.NOTIFICACIONES,
  ],
  logger,
  onInvalid: async ({ raw, motivo, ...meta }) => {
    await enviarADlq({ motivo, raw, meta });
  },
  onEvent: async (evento, meta) => {
    switch (meta.topic) {
      case TOPICS.ESTADOS:
        await manejarEstado(evento, meta);
        break;
      case TOPICS.REPROGRAMACIONES:
        await manejarReprogramacion(evento, meta);
        break;
      case TOPICS.ALERTAS:
        await manejarAlerta(evento, meta);
        break;
      case TOPICS.NOTIFICACIONES:
        await manejarNotificacion(evento, meta);
        break;
      default:
        logger.warn('Topic inesperado', meta);
    }
  },
});

logger.info('Proyector listo', { groupId: GROUPS.PROYECTOR });

process.on('SIGTERM', async () => {
  logger.info('Deteniendo proyector');
  await consumidor.disconnect().catch(() => {});
  await productor.disconnect().catch(() => {});
  process.exit(0);
});

// Evita que un error no controlado deje el proceso vivo sin consumir.
process.on('unhandledRejection', (error) => {
  logger.error('Promesa rechazada sin manejar', {
    error: error?.message ?? String(error),
    stack: error?.stack,
  });
});

export { randomUUID };
