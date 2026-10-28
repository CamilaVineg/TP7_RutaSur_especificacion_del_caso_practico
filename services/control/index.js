import express from 'express';
import Docker from 'dockerode';

import { GROUPS, TOPICS, cfg } from '../shared/config.js';
import { createLogger, dormir } from '../shared/logger.js';
import { connectMongo } from '../shared/mongo.js';
import { createKafka } from '../shared/kafka.js';

const logger = createLogger('control');
const PUERTO = Number(process.env.PORT || 4100);
const PROYECTO = process.env.COMPOSE_PROJECT || 'rutasur';
const SPEEDUP = cfg.speedup;

const { db } = await connectMongo({ uri: cfg.mongoUri, dbName: cfg.dbName, logger });
const kafka = createKafka({ clientId: 'rutasur-control', brokers: cfg.kafkaBrokers });

let docker = null;
try {
  docker = new Docker({ socketPath: '/var/run/docker.sock' });
} catch (error) {
  logger.warn('Docker no disponible', { error: error.message });
}

let admin = null;
async function getAdmin() {
  if (!admin) {
    admin = kafka.admin();
    await admin.connect();
  }
  return admin;
}

async function listarContenedores() {
  if (!docker) {
    throw new Error(
      'El socket de Docker no esta montado. Verificar el volumen /var/run/docker.sock.',
    );
  }
  const lista = await docker.listContainers({
    all: true,
    filters: { label: [`com.docker.compose.project=${PROYECTO}`] },
  });

  // Los contenedores de inicializacion (kafka-init, mongo-init) no son
  // servicios de la aplicacion y no deben aparecer en el panel.
  const interno = new Set(['kafka-init', 'mongo-init']);
  return lista.filter(
    (c) =>
      !interno.has(servicioDe(c)) &&
      String(c.Labels?.['com.docker.compose.oneoff'] ?? 'false').toLowerCase() !==
        'true',
  );
}

function servicioDe(contenedor) {
  return contenedor.Labels?.['com.docker.compose.service'] ?? 'desconocido';
}

async function contenedorDe(nombreServicio) {
  const lista = await listarContenedores();
  const encontrado = lista.find((c) => servicioDe(c) === nombreServicio);
  if (!encontrado) {
    throw new Error(`No se encontro el contenedor del servicio ${nombreServicio}`);
  }
  return docker.getContainer(encontrado.Id);
}

async function estadoServicios() {
  const lista = await listarContenedores();
  return lista
    .map((c) => ({
      servicio: servicioDe(c),
      estado: c.State,
      estadoDetallado: c.Status,
      id: c.Id.slice(0, 12),
    }))
    .sort((a, b) => a.servicio.localeCompare(b.servicio));
}

async function detener(servicio) {
  const contenedor = await contenedorDe(servicio);
  await contenedor.stop({ t: 5 }).catch((error) => {
    if (error?.statusCode !== 304) throw error;
  });
  logger.info('Servicio detenido', { servicio });
}

async function arrancar(servicio) {
  const contenedor = await contenedorDe(servicio);
  await contenedor.start().catch((error) => {
    if (error?.statusCode !== 304) throw error;
  });
  logger.info('Servicio arrancado', { servicio });
}

async function esperarGrupoVacio(groupId, timeoutMs = 25000) {
  const cliente = await getAdmin();
  const limite = Date.now() + timeoutMs;

  while (Date.now() < limite) {
    try {
      const { groups } = await cliente.describeGroups([groupId]);
      const grupo = groups?.[0];
      if (!grupo || (grupo.members?.length ?? 0) === 0) return true;
    } catch (error) {
      // Un grupo inexistente no tiene offsets que resetear.
      if (/not found|does not exist/i.test(error?.message ?? '')) return true;
    }
    await dormir(500);
  }

  logger.warn('El grupo sigue con miembros; se intenta el reset igual', {
    groupId,
  });
  return false;
}

/**
 * Fija los offsets de un grupo al inicio de cada particion.
 *
 * No se usa admin.resetOffsets porque en KafkaJS compromete el centinela -2
 * (EARLIEST_OFFSET). Al volver a arrancar, el consumidor interpreta ese valor
 * como "sin offset" y aplica fromBeginning:false, es decir, salta al final.
 * Aca se hace el mismo procedimiento pero comprometiendo el low real.
 */
async function reiniciarOffsets(groupId, topics) {
  const cliente = await getAdmin();
  const lows = new Map();
  for (const topic of topics) {
    const datos = await cliente.fetchTopicOffsets(topic);
    lows.set(topic, new Map(datos.map((p) => [p.partition, String(p.low ?? 0)])));
  }

  const consumidor = kafka.consumer({
    groupId,
    sessionTimeout: 15000,
    heartbeatInterval: 2000,
    allowAutoTopicCreation: false,
  });

  await consumidor.connect();
  await consumidor.subscribe({ topics, fromBeginning: true });

  let asignacion = null;
  consumidor.on(consumidor.events.GROUP_JOIN, (evento) => {
    asignacion = evento.payload.memberAssignment ?? null;
  });

  const enEjecucion = consumidor.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: async () => true,
  });

  // El evento GROUP_JOIN llega antes de la asignacion efectiva: se espera a
  // tener las particiones para poder reubicarlas y comprometerlas.
  const limite = Date.now() + 30000;
  while (!asignacion && Date.now() < limite) await dormir(200);
  if (!asignacion) {
    await consumidor.disconnect().catch(() => {});
    throw new Error(`No se pudo unir al grupo ${groupId} para reiniciar offsets`);
  }

  const pausa = [];
  const entradas = [];
  for (const topic of topics) {
    for (const partition of asignacion[topic] ?? []) {
      pausa.push({ topic, partition });
      entradas.push({
        topic,
        partition,
        offset: lows.get(topic)?.get(partition) ?? '0',
      });
    }
  }

  if (entradas.length === 0) {
    await consumidor.stop().catch(() => {});
    await consumidor.disconnect().catch(() => {});
    await enEjecucion.catch(() => {});
    throw new Error(`El grupo ${groupId} no recibio particiones para reiniciar`);
  }

  // Se pausa antes de reubicar para no procesar mensajes con este consumidor.
  consumidor.pause(pausa);
  for (const { topic, partition, offset } of entradas) {
    consumidor.seek({ topic, partition, offset });
  }
  await consumidor.commitOffsets(entradas);

  await consumidor.stop();
  await consumidor.disconnect();
  await enEjecucion.catch(() => {});

  logger.info('Offsets reiniciados', {
    groupId,
    topics,
    offsets: entradas.map((e) => `${e.topic}:P${e.partition}=${e.offset}`),
  });
}

async function lagPorGrupo() {
  const cliente = await getAdmin();
  const grupos = [
    GROUPS.PROYECTOR,
    GROUPS.NOTIFICACIONES,
    GROUPS.REPROGRAMACION,
    GROUPS.MONITOR_SLA,
    GROUPS.ANALITICA,
    GROUPS.ANALITICA_REPLAY,
  ];

  const cacheAltos = new Map();
  async function altos(topic) {
    if (!cacheAltos.has(topic)) {
      const datos = await cliente.fetchTopicOffsets(topic);
      cacheAltos.set(
        topic,
        new Map(
          datos.map((p) => [
            p.partition,
            Number(p.high ?? p.offset ?? 0),
          ]),
        ),
      );
    }
    return cacheAltos.get(topic);
  }

  const resultado = [];
  for (const groupId of grupos) {
    try {
      const offsets = await cliente.fetchOffsets({ groupId });
      const topics = [];
      for (const t of offsets) {
        const high = await altos(t.topic);
        let total = 0;
        const particiones = [];
        for (const p of t.partitions) {
          const comprometido = Number(p.offset ?? -1);
          const maximo = high.get(p.partition) ?? 0;
          const lag = comprometido < 0 ? maximo : Math.max(0, maximo - comprometido);
          total += lag;
          particiones.push({
            particion: p.partition,
            comprometido,
            maximo,
            lag,
          });
        }
        topics.push({ topic: t.topic, lag: total, particiones });
      }
      resultado.push({ groupId, lagTotal: topics.reduce((a, t) => a + t.lag, 0), topics });
    } catch (error) {
      resultado.push({ groupId, error: error.message, topics: [] });
    }
  }
  return resultado;
}

// ---------------------------------------------------------------- API
const app = express();
app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({ ok: true, servicio: 'control' });
});

app.get('/control/estado', async (_req, res) => {
  try {
    const servicios = await estadoServicios();
    res.json({ ok: true, servicios });
  } catch (error) {
    logger.warn('No se pudo consultar Docker', { error: error.message });
    res.status(503).json({
      ok: false,
      error: error.message,
      ayuda:
        'Verificar que Docker Desktop este corriendo y que el socket este montado en el contenedor de control.',
    });
  }
});

app.get('/control/lag', async (_req, res) => {
  try {
    const grupos = await lagPorGrupo();
    res.json({ ok: true, grupos, actualizadoEn: new Date().toISOString() });
  } catch (error) {
    logger.error('Error calculando lag', { error: error.message });
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.post('/control/servicio/:servicio/:accion', async (req, res) => {
  const { servicio, accion } = req.params;
  const permitidos = [
    'proyector',
    'notificaciones',
    'reprogramacion',
    'monitor-sla',
    'analitica',
    'analitica-replay',
    'simulador',
    'web',
  ];
  if (!permitidos.includes(servicio)) {
    return res.status(400).json({ ok: false, error: `Servicio no permitido: ${servicio}` });
  }
  try {
    if (accion === 'stop') await detener(servicio);
    else if (accion === 'start') await arrancar(servicio);
    else if (accion === 'restart') {
      await detener(servicio);
      await dormir(500);
      await arrancar(servicio);
    } else {
      return res
        .status(400)
        .json({ ok: false, error: `Accion no permitida: ${accion}` });
    }
    res.json({ ok: true, servicio, accion });
  } catch (error) {
    logger.error('Error operando servicio', {
      servicio,
      accion,
      error: error.message,
    });
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.post('/control/replay', async (_req, res) => {
  try {
    await detener('analitica-replay');
    await esperarGrupoVacio(GROUPS.ANALITICA_REPLAY);
    await db.collection('metricas_hub').deleteMany({});
    await db.collection('replay_envios').deleteMany({});
    await db
      .collection('eventos_procesados')
      .deleteMany({ consumidor: GROUPS.ANALITICA_REPLAY });
    await reiniciarOffsets(GROUPS.ANALITICA_REPLAY, [TOPICS.ESTADOS]);
    await arrancar('analitica-replay');
    logger.info('Replay de analitica reiniciado');
    res.json({
      ok: true,
      mensaje:
        'Se limpio el cuarto indicador y el grupo analitica-replay reprocesa el historico desde el inicio.',
    });
  } catch (error) {
    logger.error('Error reiniciando replay', { error: error.message });
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.post('/control/reconstruir', async (_req, res) => {
  try {
    await detener('proyector');
    await esperarGrupoVacio(GROUPS.PROYECTOR);

    // Se limpian todas las vistas que escribe el proyector para que la
    // reconstruccion sea completa y no queden alertas o envios huerfanos.
    const borrados = {
      envios: (await db.collection('envios').deleteMany({})).deletedCount,
      alertas: (await db.collection('alertas').deleteMany({})).deletedCount,
      notificaciones: (await db.collection('notificaciones').deleteMany({}))
        .deletedCount,
      dlq: (await db.collection('dlq').deleteMany({})).deletedCount,
    };

    await reiniciarOffsets(GROUPS.PROYECTOR, [
      TOPICS.ESTADOS,
      TOPICS.REPROGRAMACIONES,
      TOPICS.ALERTAS,
      TOPICS.NOTIFICACIONES,
    ]);
    await arrancar('proyector');
    logger.info('Proyeccion de envios reconstruida', borrados);
    res.json({
      ok: true,
      documentosBorrados: borrados,
      mensaje:
        'Se limpiaron las vistas del proyector y el grupo reprocesa los topics desde el inicio.',
    });
  } catch (error) {
    logger.error('Error reconstruyendo proyeccion', { error: error.message });
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get('/control/reloj', async (_req, res) => {
  const doc = await db.collection('reloj').findOne({ _id: 'global' });
  if (!doc) return res.json({ ok: true, simulado: false });
  const factor = Number(doc.factor) || SPEEDUP;
  const ahora = new Date(
    Number(doc.anclaSim) + (Date.now() - Number(doc.anclaReal)) * factor,
  );
  res.json({ ok: true, ahora: ahora.toISOString(), factor, simulado: true });
});

app.use((error, _req, res, _next) => {
  logger.error('Error HTTP no controlado', { error: error.message });
  res.status(500).json({ ok: false, error: error.message });
});

app.listen(PUERTO, () => {
  logger.info('Servicio de control escuchando', { puerto: PUERTO, proyecto: PROYECTO });
});
