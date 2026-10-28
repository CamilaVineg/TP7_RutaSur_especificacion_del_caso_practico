import express from 'express';
import { randomUUID } from 'node:crypto';

import {
  MOTIVO_PESOS,
  TOPICS,
  ZONAS,
  cfg,
  envNumber,
  franjaDe,
  proximoIntento,
  zonaPorId,
} from '../shared/config.js';
import { createLogger } from '../shared/logger.js';
import { connectMongo } from '../shared/mongo.js';
import { enviarEvento, createKafka, connectProducer } from '../shared/kafka.js';
import { reiniciarReloj } from '../shared/reloj.js';

const logger = createLogger('simulador');
const PUERTO = Number(process.env.PORT || 4000);

const SPEEDUP = cfg.speedup;
const ENVIOS_POR_MINUTO = envNumber('SIM_SHIPMENTS_PER_MINUTE', 20);
const SEMILLA = envNumber('SIM_SEED', 42);
const PROB_DEMORA_HUB = envNumber('SIM_HUB_DELAY_PROB', 0.1);
const PROB_DUPLICADO = envNumber('SIM_DUPLICADO_PROB', 0.02);
const PROB_DESORDEN = envNumber('SIM_DESORDEN_PROB', 0.02);
const PROB_MALFORMADO = envNumber('SIM_MALFORMADO_PROB', 0.01);
const PROB_EXPRESS = envNumber('SIM_EXPRESS_PROB', 0.15);
const HORAS_PROMESA = envNumber('SIM_PROMISE_HORAS', 24);

const HORA = 3_600_000;
const MINUTO = 60_000;

// ---------------------------------------------------------------- PRNG determinista
function crearAzar(semilla) {
  let estado = semilla >>> 0;
  return function azar() {
    estado |= 0;
    estado = (estado + 0x6d2b79f5) | 0;
    let t = Math.imul(estado ^ (estado >>> 15), 1 | estado);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const rnd = crearAzar(SEMILLA);
const entre = (min, max) => min + rnd() * (max - min);

function elegirMotivo() {
  let valor = rnd();
  for (const [motivo, peso] of MOTIVO_PESOS) {
    if (valor < peso) return motivo;
    valor -= peso;
  }
  return 'AUSENTE';
}

function elegirZona() {
  return ZONAS[Math.floor(rnd() * ZONAS.length)] ?? ZONAS[0];
}

// ---------------------------------------------------------------- Infraestructura
const { db } = await connectMongo({ uri: cfg.mongoUri, dbName: cfg.dbName, logger });

// Cada corrida del simulador reinicia el reloj simulado: un reinicio de la
// demo no arrastra horas simuladas de la corrida anterior.
const reloj = await reiniciarReloj(db, SPEEDUP);
const anclaSim = Number(reloj.anclaSim);
const anclaReal = Number(reloj.anclaReal);

const simAhora = () => anclaSim + (Date.now() - anclaReal) * SPEEDUP;

const kafka = createKafka({ clientId: 'rutasur-simulador', brokers: cfg.kafkaBrokers });
const productor = await connectProducer(kafka, { logger });

const contadores = {
  enviosCreados: 0,
  eventosPublicados: 0,
  entregados: 0,
  fallidos: 0,
  fallosPorMotivo: {},
  duplicados: 0,
  desordenados: 0,
  malformados: 0,
  escenarios: {},
  iniciadoEn: new Date().toISOString(),
};

// ---------------------------------------------------------------- Agenda en tiempo simulado
const agenda = [];

function programar(enSim, fn) {
  const item = { en: enSim, fn };
  let i = agenda.length;
  while (i > 0 && agenda[i - 1].en > enSim) i -= 1;
  agenda.splice(i, 0, item);
}

setInterval(() => {
  const ahora = simAhora();
  let procesados = 0;
  while (agenda.length > 0 && agenda[0].en <= ahora && procesados < 500) {
    const item = agenda.shift();
    procesados += 1;
    try {
      item.fn();
    } catch (error) {
      logger.error('Error ejecutando tarea simulada', { error: error.message });
    }
  }
}, 250);

// Serializa las publicaciones por envio para preservar el orden de secuencia.
const colas = new Map();
function encolar(shipmentId, tarea) {
  const previa = colas.get(shipmentId) ?? Promise.resolve();
  const siguiente = previa.then(tarea, tarea).finally(() => {
    if (colas.get(shipmentId) === siguiente) colas.delete(shipmentId);
  });
  colas.set(shipmentId, siguiente);
  return siguiente;
}

// ---------------------------------------------------------------- Publicacion de eventos
async function enviarEstado(evento) {
  await enviarEvento(productor, TOPICS.ESTADOS, evento.shipmentId, evento);
  contadores.eventosPublicados += 1;

  if (rnd() < PROB_DUPLICADO) {
    await enviarEvento(productor, TOPICS.ESTADOS, evento.shipmentId, evento);
    contadores.eventosPublicados += 1;
    contadores.duplicados += 1;
    logger.debug('Evento duplicado inyectado', { eventId: evento.eventId });
  }

  if (rnd() < PROB_MALFORMADO) {
    const malformado = {
      eventId: randomUUID(),
      shipmentId: evento.shipmentId,
      tipo: 'EN_REPARTO',
    };
    await enviarEvento(productor, TOPICS.ESTADOS, evento.shipmentId, malformado);
    contadores.eventosPublicados += 1;
    contadores.malformados += 1;
    logger.debug('Evento malformado inyectado', {
      shipmentId: evento.shipmentId,
    });
  }
}

function publicarEstado(evento) {
  return encolar(evento.shipmentId, () => enviarEstado(evento)).catch((error) =>
    logger.error('Error publicando estado', {
      eventId: evento.eventId,
      error: error.message,
    }),
  );
}

async function publicarPromesa({ shipmentId, venceEn, hubId, zonaId, ocurridoEn }) {
  await enviarEvento(productor, TOPICS.PROMESAS, shipmentId, {
    eventId: randomUUID(),
    shipmentId,
    ocurridoEn,
    version: 1,
    venceEn,
    hubId,
    zonaId,
  });
  contadores.eventosPublicados += 1;
}

// ---------------------------------------------------------------- Creacion de envios
function crearEnvio(opciones = {}) {
  const zona = opciones.zona ? zonaPorId(opciones.zona) : elegirZona();
  const shipmentId = `RS-${randomUUID().slice(0, 8).toUpperCase()}`;
  const ahora = simAhora();
  const t0 = opciones.t0 ?? ahora;
  const comprimido = opciones.comprimido === true;

  // En modo comprimido las horas simuladas se reducen a minutos para que el
  // ciclo completo se vea en segundos reales (documentado como compresion
  // exclusiva del modo demo).
  const escala = comprimido ? 0.15 : 1;
  const escalaCorta = comprimido ? 0.3 : 1;

  const horasPromesa =
    opciones.horasPromesa ?? (rnd() < PROB_EXPRESS ? 4 : HORAS_PROMESA);
  const venceEn = new Date(t0 + horasPromesa * HORA);

  const repartidorId = `R-${String(1 + Math.floor(rnd() * 120)).padStart(3, '0')}`;
  const base = {
    shipmentId,
    zonaId: zona.id,
    hubId: zona.hubId,
    repartidorId,
    promesaVenceEn: venceEn.toISOString(),
  };

  let secuencia = 0;

  const programarPaso = (eventoEn, tipo, extra = {}, publicarEn = eventoEn) => {
    const numeroSecuencia = (secuencia += 1);
    programar(publicarEn, () => {
      const evento = {
        eventId: randomUUID(),
        shipmentId,
        tipo,
        ocurridoEn: new Date(eventoEn).toISOString(),
        secuencia: numeroSecuencia,
        version: 1,
        ...base,
        ...extra,
      };

      const desordenar = rnd() < PROB_DESORDEN && numeroSecuencia > 1;
      if (desordenar) {
        const retraso = (10 + rnd() * 20) * MINUTO;
        contadores.desordenados += 1;
        logger.debug('Evento demorado (desorden)', {
          shipmentId,
          secuencia: numeroSecuencia,
        });
        programar(publicarEn + retraso, () => publicarEstado(evento));
        return;
      }

      publicarEstado(evento);
    });
  };

  contadores.enviosCreados += 1;

  publicarPromesa({
    shipmentId,
    venceEn: venceEn.toISOString(),
    hubId: zona.hubId,
    zonaId: zona.id,
    ocurridoEn: new Date(t0).toISOString(),
  }).catch((error) =>
    logger.error('Error publicando promesa', { error: error.message }),
  );

  programarPaso(t0, 'CREADO');

  const demoraHub = opciones.demoraHub ?? rnd() < PROB_DEMORA_HUB;
  const tIngreso =
    t0 + (demoraHub ? entre(11, 20) * HORA : entre(1, 3) * HORA) * escala;
  const tClasificado = tIngreso + entre(0.3, 1) * HORA * escalaCorta;
  const tReparto = tClasificado + entre(0.5, 2) * HORA * escalaCorta;

  programarPaso(tIngreso, 'INGRESADO_HUB');
  programarPaso(tClasificado, 'CLASIFICADO');

  if (opciones.detenerEn === 'CLASIFICADO') {
    return { shipmentId, zona, venceEn: venceEn.toISOString() };
  }

  const intentoInicial = opciones.intentoInicial ?? 1;
  const forzarPrimerFallo = opciones.forzarFallo === true;
  const exitoEnReintento = opciones.exitoEnReintento === true;
  const reintentoRapido = opciones.reintentoRapido === true;

  const programarIntento = (tSalida, intento) => {
    programarPaso(tSalida, 'EN_REPARTO', { intento });

    const tResultado = tSalida + entre(0.5, 3) * HORA * escalaCorta;

    programar(tResultado, () => {
      let falla;
      if (opciones.forzarExito === true) {
        falla = false;
      } else if (opciones.forzarTodosLosFallos === true) {
        falla = true;
      } else if (intento === intentoInicial && forzarPrimerFallo) {
        falla = true;
      } else if (intento > intentoInicial && exitoEnReintento) {
        falla = false;
      } else {
        falla = rnd() < zona.tasaFallo;
      }

      if (!falla) {
        contadores.entregados += 1;
        publicarEstado({
          eventId: randomUUID(),
          shipmentId,
          tipo: 'ENTREGADO',
          ocurridoEn: new Date(tResultado).toISOString(),
          secuencia: (secuencia += 1),
          version: 1,
          intento,
          ...base,
        });
        return;
      }

      const motivo = opciones.motivoForzado ?? elegirMotivo();
      contadores.fallidos += 1;
      contadores.fallosPorMotivo[motivo] =
        (contadores.fallosPorMotivo[motivo] ?? 0) + 1;

      publicarEstado({
        eventId: randomUUID(),
        shipmentId,
        tipo: 'ENTREGA_FALLIDA',
        ocurridoEn: new Date(tResultado).toISOString(),
        secuencia: (secuencia += 1),
        version: 1,
        motivo,
        intento,
        ...base,
      });

      if (intento >= 3) return;
      if (motivo === 'RECHAZADO') return;
      if (motivo === 'DIRECCION_INCORRECTA') return;

      const proximo = reintentoRapido
        ? new Date(tResultado + entre(2, 5) * MINUTO)
        : proximoIntento(new Date(tResultado), franjaDe(new Date(tResultado)));

      programarIntento(proximo.getTime(), intento + 1);
    });
  };

  programarIntento(tReparto, intentoInicial);

  return { shipmentId, zona, venceEn: venceEn.toISOString() };
}

// ---------------------------------------------------------------- Flujo continuo
const intervalo = Math.max(250, Math.round(60000 / Math.max(1, ENVIOS_POR_MINUTO)));
setInterval(() => {
  try {
    crearEnvio();
  } catch (error) {
    logger.error('Error creando envio continuo', { error: error.message });
  }
}, intervalo);

logger.info('Flujo continuo iniciado', {
  enviosPorMinuto: ENVIOS_POR_MINUTO,
  intervaloMs: intervalo,
  semilla: SEMILLA,
  speedup: SPEEDUP,
});

// ---------------------------------------------------------------- API HTTP
const app = express();
app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({ ok: true, servicio: 'simulador' });
});

app.get('/sim/estado', (_req, res) => {
  res.json({
    reloj: {
      ahora: new Date(simAhora()).toISOString(),
      factor: SPEEDUP,
    },
    contadores,
    agenda: agenda.length,
    configuracion: {
      enviosPorMinuto: ENVIOS_POR_MINUTO,
      semilla: SEMILLA,
      probDemoraHub: PROB_DEMORA_HUB,
      probDuplicado: PROB_DUPLICADO,
      probDesorden: PROB_DESORDEN,
      probMalformado: PROB_MALFORMADO,
      probExpress: PROB_EXPRESS,
      horasPromesa: HORAS_PROMESA,
    },
  });
});

app.get('/sim/resumen', (_req, res) => {
  res.json({ contadores, ahora: new Date(simAhora()).toISOString() });
});

const ESCENARIOS = {
  feliz: () =>
    crearEnvio({
      comprimido: true,
      horasPromesa: 24,
      demoraHub: false,
      forzarExito: true,
    }),
  ausente: () =>
    crearEnvio({
      comprimido: true,
      horasPromesa: 24,
      demoraHub: false,
      forzarFallo: true,
      motivoForzado: 'AUSENTE',
      reintentoRapido: true,
      exitoEnReintento: true,
    }),
  demorado: () =>
    crearEnvio({
      comprimido: true,
      horasPromesa: 3,
      demoraHub: false,
      detenerEn: 'CLASIFICADO',
    }),
  'tercer-fallo': () =>
    crearEnvio({
      comprimido: true,
      horasPromesa: 48,
      demoraHub: false,
      forzarTodosLosFallos: true,
      motivoForzado: 'AUSENTE',
      reintentoRapido: true,
    }),
  rafaga: () => {
    const creados = [];
    for (let i = 0; i < 500; i += 1) {
      creados.push(
        crearEnvio({
          comprimido: true,
          horasPromesa: 24,
          t0: simAhora() + (i % 120) * MINUTO,
        }),
      );
    }
    return { envios: creados.length, muestra: creados[0] };
  },
  invalido: async () => {
    const malformado = {
      eventId: randomUUID(),
      shipmentId: `RS-BAD-${randomUUID().slice(0, 6).toUpperCase()}`,
      tipo: 'EN_REPARTO',
    };
    await enviarEvento(
      productor,
      TOPICS.ESTADOS,
      malformado.shipmentId,
      malformado,
    );
    contadores.malformados += 1;
    contadores.eventosPublicados += 1;
    return { inyectado: malformado.shipmentId };
  },
};

app.post('/sim/escenarios/:nombre', async (req, res) => {
  const nombre = req.params.nombre;
  const escenario = ESCENARIOS[nombre];
  if (!escenario) {
    return res.status(404).json({
      ok: false,
      error: `Escenario desconocido. Opciones: ${Object.keys(ESCENARIOS).join(', ')}`,
    });
  }

  try {
    const resultado = await escenario();
    contadores.escenarios[nombre] = (contadores.escenarios[nombre] ?? 0) + 1;
    logger.info('Escenario disparado', { nombre });
    return res.json({
      ok: true,
      escenario: nombre,
      ...resultado,
      detalleUrl:
        resultado?.shipmentId ? `/envios/${resultado.shipmentId}` : null,
      trackingUrl:
        resultado?.shipmentId ? `/tracking/${resultado.shipmentId}` : null,
    });
  } catch (error) {
    logger.error('Error ejecutando escenario', {
      nombre,
      error: error.message,
      stack: error.stack,
    });
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.use((error, _req, res, _next) => {
  logger.error('Error HTTP no controlado', { error: error.message });
  res.status(500).json({ ok: false, error: error.message });
});

app.listen(PUERTO, () => {
  logger.info('Simulador escuchando', { puerto: PUERTO });
});

process.on('SIGTERM', () => {
  logger.info('Deteniendo simulador');
  process.exit(0);
});
