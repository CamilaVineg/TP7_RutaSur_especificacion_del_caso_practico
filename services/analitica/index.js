import {
  GROUPS,
  TOPICS,
  cfg,
  fechaDe,
  franjaDe,
  hubDeZona,
} from '../shared/config.js';
import { createLogger } from '../shared/logger.js';
import { connectMongo } from '../shared/mongo.js';
import { createKafka, startConsumer } from '../shared/kafka.js';
import { registrarEventoProcesado } from '../shared/indexes.js';
import { validarEstado } from '../shared/events.js';

const logger = createLogger('analitica');

const { db } = await connectMongo({ uri: cfg.mongoUri, dbName: cfg.dbName, logger });
const kafka = createKafka({ clientId: 'rutasur-analitica', brokers: cfg.kafkaBrokers });
const metricas = db.collection('metricas_diarias');

async function manejarEstado(evt, meta) {
  const duplicado = await registrarEventoProcesado(
    db,
    GROUPS.ANALITICA,
    evt.eventId,
  );
  if (duplicado) {
    logger.debug('Evento ya contabilizado', {
      eventId: evt.eventId,
      shipmentId: evt.shipmentId,
    });
    return;
  }

  const fecha = fechaDe(evt.ocurridoEn);
  const franja = franjaDe(evt.ocurridoEn);
  const zonaId = evt.zonaId ?? 'SIN_ZONA';
  const hubId = evt.hubId ?? hubDeZona(zonaId);

  const inc = {};
  const set = { hubId, actualizadoEn: new Date() };

  switch (evt.tipo) {
    case 'CREADO':
      inc.creados = 1;
      break;
    case 'INGRESADO_HUB':
      inc.ingresosHub = 1;
      break;
    case 'CLASIFICADO':
      inc.clasificados = 1;
      break;
    case 'EN_REPARTO':
      inc.enReparto = 1;
      break;
    case 'ENTREGADO': {
      inc.entregados = 1;
      if ((evt.intento ?? 1) === 1) inc.primerIntentoOk = 1;
      if (evt.promesaVenceEn) {
        const aTiempo =
          new Date(evt.ocurridoEn).getTime() <=
          new Date(evt.promesaVenceEn).getTime();
        if (aTiempo) inc.slaCumplidos = 1;
        else inc.slaIncumplidos = 1;
      } else {
        inc.slaSinPromesa = 1;
      }
      break;
    }
    case 'ENTREGA_FALLIDA': {
      inc.fallos = 1;
      inc[`motivos.${evt.motivo}`] = 1;
      if ((evt.intento ?? 1) === 1) inc.fallosPrimerIntento = 1;
      else inc.fallosReintento = 1;
      break;
    }
    default:
      break;
  }

  if (Object.keys(inc).length === 0) return;

  await metricas.updateOne(
    { fecha, zonaId, franja },
    {
      $inc: inc,
      $set: set,
      $setOnInsert: { fecha, zonaId, franja, creadoEn: new Date() },
    },
    { upsert: true },
  );

  logger.info('Metrica actualizada', {
    eventId: evt.eventId,
    shipmentId: evt.shipmentId,
    tipo: evt.tipo,
    fecha,
    zonaId,
    franja,
    partition: meta.partition,
    offset: meta.offset,
  });
}

const consumidor = await startConsumer({
  kafka,
  groupId: GROUPS.ANALITICA,
  topics: [TOPICS.ESTADOS],
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

logger.info('Servicio de analitica listo', { groupId: GROUPS.ANALITICA });

process.on('SIGTERM', async () => {
  logger.info('Deteniendo analitica');
  await consumidor.disconnect().catch(() => {});
  process.exit(0);
});

process.on('unhandledRejection', (error) => {
  logger.error('Promesa rechazada sin manejar', {
    error: error?.message ?? String(error),
    stack: error?.stack,
  });
});
