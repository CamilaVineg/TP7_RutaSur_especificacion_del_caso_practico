import { randomUUID } from 'node:crypto';

import { GROUPS, TOPICS, cfg, fechaDe } from '../shared/config.js';
import { createLogger } from '../shared/logger.js';
import { connectMongo } from '../shared/mongo.js';
import {
  createKafka,
  connectProducer,
  startConsumer,
} from '../shared/kafka.js';
import { simAhora } from '../shared/reloj.js';
import { validarEstado, validarPromesa } from '../shared/events.js';

const logger = createLogger('monitor-sla');
const TICK_MS = Number(process.env.SLA_TICK_MS || 60000);
const SPEEDUP = cfg.speedup;
const HORA = 3_600_000;

const { db } = await connectMongo({ uri: cfg.mongoUri, dbName: cfg.dbName, logger });
const kafka = createKafka({ clientId: 'rutasur-monitor-sla', brokers: cfg.kafkaBrokers });
const productor = await connectProducer(kafka, { logger });
const estado = db.collection('sla_envios');

// Serializa la evaluacion por envio para evitar alertas duplicadas cuando
// coinciden un evento y el tick periodico.
const colas = new Map();
function enCola(shipmentId, tarea) {
  const previa = colas.get(shipmentId) ?? Promise.resolve();
  const siguiente = previa.then(tarea, tarea).finally(() => {
    if (colas.get(shipmentId) === siguiente) colas.delete(shipmentId);
  });
  colas.set(shipmentId, siguiente);
  return siguiente;
}

async function emitir(alerta) {
  await productor.send({
    topic: TOPICS.ALERTAS,
    messages: [
      {
        key: alerta.shipmentId,
        value: JSON.stringify({
          eventId: randomUUID(),
          version: 1,
          ocurridoEn: alerta.ocurridoEn,
          ...alerta,
        }),
      },
    ],
  });
}

async function cerrarAlertas(doc, mensaje) {
  const reglas = Object.entries(doc.reglas ?? {});
  for (const [causa, info] of reglas) {
    if (info.cerrada) continue;
    const ahora = (await simAhora(db, SPEEDUP)).ahora;
    await emitir({
      shipmentId: doc.shipmentId,
      ocurridoEn: ahora.toISOString(),
      causa,
      prioridad: info.prioridad,
      abierta: false,
      venceEn: doc.venceEn ? new Date(doc.venceEn).toISOString() : undefined,
      mensaje,
      hubId: doc.hubId ?? null,
      zonaId: doc.zonaId ?? null,
    });
    await estado.updateOne(
      { shipmentId: doc.shipmentId },
      {
        $set: {
          [`reglas.${causa}.cerrada`]: true,
          [`reglas.${causa}.cerradaEn`]: ahora.toISOString(),
          actualizadoEn: new Date(),
        },
      },
    );
    logger.info('Alerta cerrada', {
      shipmentId: doc.shipmentId,
      causa,
      motivoCierre: mensaje,
    });
  }
  await estado.updateOne(
    { shipmentId: doc.shipmentId },
    { $set: { alertasCerradas: true } },
  );
}

async function evaluar(shipmentId) {
  const doc = await estado.findOne({ shipmentId });
  if (!doc || !doc.venceEn) return;
  if (doc.terminal) {
    if (!doc.alertasCerradas) {
      await cerrarAlertas(
        doc,
        doc.ultimoEstado === 'ENTREGADO'
          ? 'Alerta cerrada: el envio fue entregado.'
          : 'Alerta cerrada: el envio salio del circuito por devolucion.',
      );
    }
    return;
  }

  const { ahora } = await simAhora(db, SPEEDUP);
  const restante = new Date(doc.venceEn).getTime() - ahora.getTime();
  const estadoActual = doc.ultimoEstado;

  let regla = null;
  if (restante < 0) {
    regla = {
      causa: 'VENCIDO',
      prioridad: 'CRITICA',
      mensaje: 'El envio supero la fecha prometida sin haber sido entregado.',
    };
  } else if (estadoActual === 'CREADO' && restante <= 10 * HORA) {
    regla = {
      causa: 'SIN_INGRESO',
      prioridad: 'MEDIA',
      mensaje:
        'Faltan menos de 10 horas para el vencimiento y el envio no ingreso al hub.',
    };
  } else if (
    ['INGRESADO_HUB', 'CLASIFICADO'].includes(estadoActual) &&
    restante <= 4 * HORA
  ) {
    regla = {
      causa: 'SIN_SALIDA',
      prioridad: 'ALTA',
      mensaje:
        'Faltan menos de 4 horas para el vencimiento y el envio no salio a reparto.',
    };
  } else if (
    estadoActual === 'ENTREGA_FALLIDA' &&
    fechaDe(doc.venceEn) === fechaDe(ahora)
  ) {
    regla = {
      causa: 'FALLO_DIA_LIMITE',
      prioridad: 'ALTA',
      mensaje:
        'La entrega fallo el mismo dia en que vence la promesa de entrega.',
    };
  }

  if (!regla) return;
  if (doc.reglas?.[regla.causa]) return;

  await emitir({
    shipmentId,
    ocurridoEn: ahora.toISOString(),
    causa: regla.causa,
    prioridad: regla.prioridad,
    abierta: true,
    venceEn: new Date(doc.venceEn).toISOString(),
    mensaje: regla.mensaje,
    hubId: doc.hubId ?? null,
    zonaId: doc.zonaId ?? null,
  });

  await estado.updateOne(
    { shipmentId },
    {
      $set: {
        [`reglas.${regla.causa}`]: {
          prioridad: regla.prioridad,
          disparadaEn: ahora.toISOString(),
          cerrada: false,
        },
        actualizadoEn: new Date(),
      },
    },
  );

  logger.info('Alerta de SLA emitida', {
    shipmentId,
    causa: regla.causa,
    prioridad: regla.prioridad,
    estadoActual,
    restanteHoras: Number((restante / HORA).toFixed(2)),
  });
}

async function manejarPromesa(evt, meta) {
  const set = {
    venceEn: new Date(evt.venceEn),
    actualizadoEn: new Date(),
  };
  if (evt.hubId) set.hubId = evt.hubId;
  if (evt.zonaId) set.zonaId = evt.zonaId;

  await estado.updateOne(
    { shipmentId: evt.shipmentId },
    {
      $set: set,
      $setOnInsert: { shipmentId: evt.shipmentId, reglas: {}, creadoEn: new Date() },
    },
    { upsert: true },
  );

  logger.info('Promesa registrada por el monitor', {
    eventId: evt.eventId,
    shipmentId: evt.shipmentId,
    venceEn: evt.venceEn,
    partition: meta.partition,
    offset: meta.offset,
  });

  await enCola(evt.shipmentId, () => evaluar(evt.shipmentId));
}

async function manejarEstado(evt, meta) {
  const doc = await estado.findOne({ shipmentId: evt.shipmentId });
  if (doc && evt.secuencia <= (doc.ultimaSecuencia ?? -1)) {
    logger.info('Evento fuera de orden ignorado por el monitor', {
      eventId: evt.eventId,
      shipmentId: evt.shipmentId,
      partition: meta.partition,
      offset: meta.offset,
    });
    return;
  }

  const set = {
    ultimoEstado: evt.tipo,
    ultimaSecuencia: evt.secuencia,
    ultimoEventoEn: new Date(evt.ocurridoEn),
    intentoActual: evt.intento ?? 1,
    motivo: evt.motivo ?? null,
    actualizadoEn: new Date(),
  };
  if (evt.promesaVenceEn) set.venceEn = new Date(evt.promesaVenceEn);
  if (evt.hubId) set.hubId = evt.hubId;
  if (evt.zonaId) set.zonaId = evt.zonaId;
  if (evt.tipo === 'ENTREGADO') set.terminal = true;
  if (
    evt.tipo === 'ENTREGA_FALLIDA' &&
    ((evt.intento ?? 1) >= 3 || evt.motivo === 'RECHAZADO')
  ) {
    set.terminal = true;
  }

  await estado.updateOne(
    { shipmentId: evt.shipmentId },
    {
      $set: set,
      $setOnInsert: { shipmentId: evt.shipmentId, reglas: {}, creadoEn: new Date() },
    },
    { upsert: true },
  );

  logger.info('Estado registrado por el monitor', {
    eventId: evt.eventId,
    shipmentId: evt.shipmentId,
    tipo: evt.tipo,
    partition: meta.partition,
    offset: meta.offset,
  });

  await enCola(evt.shipmentId, () => evaluar(evt.shipmentId));
}

// ---------------------------------------------------------------- Evaluacion periodica
let tickEnCurso = false;

async function tick() {
  if (tickEnCurso) return;
  tickEnCurso = true;
  try {
    const abiertos = await estado
      .find({ terminal: { $ne: true }, venceEn: { $exists: true } })
      .limit(2000)
      .toArray();
    for (const doc of abiertos) {
      await enCola(doc.shipmentId, () => evaluar(doc.shipmentId));
    }
  } catch (error) {
    logger.error('Error en la evaluacion periodica', { error: error.message });
  } finally {
    tickEnCurso = false;
  }
}

setInterval(tick, TICK_MS);

// ---------------------------------------------------------------- Consumidor
const consumidor = await startConsumer({
  kafka,
  groupId: GROUPS.MONITOR_SLA,
  topics: [TOPICS.ESTADOS, TOPICS.PROMESAS],
  logger,
  onInvalid: ({ raw, motivo, ...meta }) => {
    logger.warn('Mensaje invalido descartado', { ...meta, motivo });
  },
  onEvent: async (evento, meta) => {
    if (meta.topic === TOPICS.PROMESAS) {
      const validacion = validarPromesa(evento);
      if (!validacion.ok) return;
      await manejarPromesa(validacion.valor, meta);
      return;
    }

    const validacion = validarEstado(evento);
    if (!validacion.ok) return;
    await manejarEstado(validacion.valor, meta);
  },
});

logger.info('Monitor de SLA listo', {
  groupId: GROUPS.MONITOR_SLA,
  tickMs: TICK_MS,
});

process.on('SIGTERM', async () => {
  logger.info('Deteniendo monitor de SLA');
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
