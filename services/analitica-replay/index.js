import {
  GROUPS,
  TOPICS,
  cfg,
  fechaDe,
  hubDeZona,
} from '../shared/config.js';
import { createLogger } from '../shared/logger.js';
import { connectMongo } from '../shared/mongo.js';
import { createKafka, startConsumer } from '../shared/kafka.js';
import { registrarEventoProcesado } from '../shared/indexes.js';
import { validarEstado } from '../shared/events.js';

// Cuarto indicador: tiempo medio entre INGRESADO_HUB y EN_REPARTO por hub.
// Corre en su propio grupo de consumidores, desde el inicio del topic, sin
// tocar a los demas servicios. Reconstruye el historico completo.

const logger = createLogger('analitica-replay');

const { db } = await connectMongo({ uri: cfg.mongoUri, dbName: cfg.dbName, logger });
const kafka = createKafka({
  clientId: 'rutasur-analitica-replay',
  brokers: cfg.kafkaBrokers,
});
const replay = db.collection('replay_envios');
const metricasHub = db.collection('metricas_hub');

async function manejarEstado(evt, meta) {
  const duplicado = await registrarEventoProcesado(
    db,
    GROUPS.ANALITICA_REPLAY,
    evt.eventId,
  );
  if (duplicado) return;

  if (evt.tipo === 'INGRESADO_HUB') {
    await replay.updateOne(
      { shipmentId: evt.shipmentId },
      {
        $set: {
          ingresoEn: new Date(evt.ocurridoEn),
          ingresoSeq: evt.secuencia,
          hubId: evt.hubId ?? null,
          zonaId: evt.zonaId ?? null,
          actualizadoEn: new Date(),
        },
        $setOnInsert: { shipmentId: evt.shipmentId, creadoEn: new Date() },
      },
      { upsert: true },
    );
    logger.debug('Ingreso registrado para replay', {
      shipmentId: evt.shipmentId,
      partition: meta.partition,
      offset: meta.offset,
    });
    return;
  }

  if (evt.tipo !== 'EN_REPARTO') return;

  const doc = await replay.findOne({ shipmentId: evt.shipmentId });
  if (!doc?.ingresoEn) return;
  if (doc.repartoSeq && evt.secuencia <= doc.repartoSeq) return;

  const minutos =
    (new Date(evt.ocurridoEn).getTime() - new Date(doc.ingresoEn).getTime()) /
    60000;
  if (!Number.isFinite(minutos) || minutos < 0) return;

  const hubId = evt.hubId ?? doc.hubId ?? hubDeZona(evt.zonaId ?? doc.zonaId);
  const fecha = fechaDe(doc.ingresoEn);

  await metricasHub.updateOne(
    { hubId, fecha },
    {
      $inc: { cantidad: 1, sumaMinutos: minutos },
      $set: { actualizadoEn: new Date() },
      $setOnInsert: { hubId, fecha, creadoEn: new Date() },
    },
    { upsert: true },
  );

  await replay.updateOne(
    { shipmentId: evt.shipmentId },
    {
      $set: {
        repartoSeq: evt.secuencia,
        repartoEn: new Date(evt.ocurridoEn),
        minutosHastaReparto: minutos,
      },
    },
  );

  logger.info('Tiempo hub -> reparto acumulado', {
    eventId: evt.eventId,
    shipmentId: evt.shipmentId,
    hubId,
    fecha,
    minutos: Number(minutos.toFixed(2)),
    partition: meta.partition,
    offset: meta.offset,
  });
}

const consumidor = await startConsumer({
  kafka,
  groupId: GROUPS.ANALITICA_REPLAY,
  topics: [TOPICS.ESTADOS],
  fromBeginning: true,
  logger,
  onInvalid: ({ raw, motivo, ...meta }) => {
    logger.warn('Mensaje invalido descartado', { ...meta, motivo });
  },
  onEvent: async (evento, meta) => {
    const validacion = validarEstado(evento);
    if (!validacion.ok) return;
    await manejarEstado(validacion.valor, meta);
  },
});

logger.info('Replay de analitica listo (lee el topic desde el inicio)', {
  groupId: GROUPS.ANALITICA_REPLAY,
});

process.on('SIGTERM', async () => {
  logger.info('Deteniendo replay de analitica');
  await consumidor.disconnect().catch(() => {});
  process.exit(0);
});

process.on('unhandledRejection', (error) => {
  logger.error('Promesa rechazada sin manejar', {
    error: error?.message ?? String(error),
    stack: error?.stack,
  });
});
