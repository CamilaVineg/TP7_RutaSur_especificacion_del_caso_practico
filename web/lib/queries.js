import { getDb } from './mongo.js';

const iso = (fecha) => (fecha ? new Date(fecha).toISOString() : null);

function serializarAlerta(a) {
  return {
    shipmentId: a.shipmentId,
    causa: a.causa,
    prioridad: a.prioridad,
    prioridadOrden: a.prioridadOrden ?? 9,
    estado: a.estado,
    abiertaEn: iso(a.abiertaEn),
    cerradaEn: iso(a.cerradaEn),
    venceEn: iso(a.venceEn),
    mensaje: a.mensaje ?? null,
    zonaId: a.zonaId ?? null,
    hubId: a.hubId ?? null,
  };
}

function serializarHistorial(h) {
  return {
    eventId: h.eventId,
    tipo: h.tipo,
    ocurridoEn: iso(h.ocurridoEn),
    secuencia: h.secuencia,
    motivo: h.motivo ?? null,
    intento: h.intento ?? null,
    hubId: h.hubId ?? null,
    zonaId: h.zonaId ?? null,
    repartidorId: h.repartidorId ?? null,
  };
}

export async function getTorre() {
  const db = await getDb();
  const [conteos, alertas, recientes] = await Promise.all([
    db
      .collection('envios')
      .aggregate([{ $group: { _id: '$estado', total: { $sum: 1 } } }])
      .toArray(),
    db
      .collection('alertas')
      .find({ estado: 'ABIERTA' })
      .sort({ prioridadOrden: 1, abiertaEn: 1 })
      .limit(12)
      .toArray(),
    db
      .collection('envios')
      .find(
        {},
        {
          projection: {
            shipmentId: 1,
            estado: 1,
            zonaId: 1,
            hubId: 1,
            actualizadoEn: 1,
            ultimoEventoEn: 1,
            historial: { $slice: -1 },
          },
        },
      )
      .sort({ actualizadoEn: -1 })
      .limit(15)
      .toArray(),
  ]);

  return {
    conteos: conteos
      .filter((c) => c._id)
      .map((c) => ({ estado: c._id, total: c.total }))
      .sort((a, b) => a.estado.localeCompare(b.estado)),
    total: conteos.reduce((acc, c) => acc + c.total, 0),
    alertas: alertas.map(serializarAlerta),
    recientes: recientes.map((e) => ({
      shipmentId: e.shipmentId,
      estado: e.estado,
      zonaId: e.zonaId ?? null,
      hubId: e.hubId ?? null,
      actualizadoEn: iso(e.actualizadoEn),
      ultimoEventoEn: iso(e.ultimoEventoEn),
      ultimoTipo: e.historial?.[0]?.tipo ?? e.estado ?? null,
    })),
    actualizadoEn: new Date().toISOString(),
  };
}

export async function getEnvio(shipmentId) {
  const db = await getDb();
  const envio = await db.collection('envios').findOne({ shipmentId });
  if (!envio) {
    return { envio: null, alertas: [], notificaciones: [], actualizadoEn: new Date().toISOString() };
  }

  const alertasDocs = await db
    .collection('alertas')
    .find({ shipmentId })
    .sort({ abiertaEn: -1 })
    .toArray();
  const notificacionesDocs = await db
    .collection('notificaciones')
    .find({ shipmentId })
    .sort({ enviadaEn: -1 })
    .toArray();

  const { _id, ...datosEnvio } = envio;

  return {
    envio: {
      ...datosEnvio,
      creadoEn: iso(envio.creadoEn),
      actualizadoEn: iso(envio.actualizadoEn),
      ultimoEventoEn: iso(envio.ultimoEventoEn),
      entregadoEn: iso(envio.entregadoEn),
      proximoIntentoEn: iso(envio.proximoIntentoEn),
      ultimaReprogramacionEn: iso(envio.ultimaReprogramacionEn),
      promesa: envio.promesa?.venceEn
        ? { venceEn: iso(envio.promesa.venceEn) }
        : null,
      historial: (envio.historial ?? []).map(serializarHistorial),
      reprogramaciones: (envio.reprogramaciones ?? []).map((r) => ({
        eventId: r.eventId,
        ocurridoEn: iso(r.ocurridoEn),
        motivo: r.motivo,
        intento: r.intento,
        decision: r.decision,
        proximoIntentoEn: iso(r.proximoIntentoEn),
        franjaFallo: r.franjaFallo ?? null,
      })),
    },
    alertas: alertasDocs.map(serializarAlerta),
    notificaciones: notificacionesDocs.map((n) => ({
      notificacionId: n.notificacionId,
      canal: n.canal,
      tipo: n.tipo,
      mensaje: n.mensaje,
      opciones: n.opciones ?? [],
      prioridad: n.prioridad ?? null,
      enviadaEn: iso(n.enviadaEn),
    })),
    actualizadoEn: new Date().toISOString(),
  };
}

// Vista publica: sin datos internos (hub, zona, repartidor, intentos, alertas).
export async function getTracking(shipmentId) {
  const db = await getDb();
  const envio = await db.collection('envios').findOne({ shipmentId });
  if (!envio) {
    return { envio: null, actualizadoEn: new Date().toISOString() };
  }

  return {
    envio: {
      shipmentId: envio.shipmentId,
      estado: envio.estado,
      promesaVenceEn: iso(envio.promesa?.venceEn),
      actualizadoEn: iso(envio.actualizadoEn),
      historial: (envio.historial ?? []).map((h) => ({
        tipo: h.tipo,
        ocurridoEn: iso(h.ocurridoEn),
      })),
    },
    actualizadoEn: new Date().toISOString(),
  };
}

// ---------------------------------------------------------------- Analitica

function agrupar(docs, clave) {
  const mapa = new Map();
  for (const doc of docs) {
    const k = clave(doc);
    if (!mapa.has(k)) mapa.set(k, []);
    mapa.get(k).push(doc);
  }
  return mapa;
}

function acumulado(docs) {
  const acc = {
    creados: 0,
    entregados: 0,
    fallos: 0,
    primerIntentoOk: 0,
    fallosPrimerIntento: 0,
    fallosReintento: 0,
    slaCumplidos: 0,
    slaIncumplidos: 0,
    slaSinPromesa: 0,
    ingresosHub: 0,
    enReparto: 0,
    motivos: {},
  };
  for (const doc of docs) {
    for (const clave of Object.keys(acc)) {
      if (typeof acc[clave] === 'number') {
        acc[clave] += Number(doc[clave] ?? 0);
      }
    }
    for (const [motivo, valor] of Object.entries(doc.motivos ?? {})) {
      acc.motivos[motivo] = (acc.motivos[motivo] ?? 0) + Number(valor);
    }
  }
  return acc;
}

function tasaPrimerIntento(acc) {
  const denominador = acc.primerIntentoOk + acc.fallosPrimerIntento;
  return denominador ? (acc.primerIntentoOk / denominador) * 100 : null;
}

function tasaSla(acc) {
  const denominador = acc.slaCumplidos + acc.slaIncumplidos;
  return denominador ? (acc.slaCumplidos / denominador) * 100 : null;
}

export async function getAnalitica(filtros = {}) {
  const db = await getDb();
  const { zona, hub, desde, hasta } = filtros;

  const match = {};
  if (zona) match.zonaId = zona;
  if (hub) match.hubId = hub;
  if (desde || hasta) {
    match.fecha = {};
    if (desde) match.fecha.$gte = desde;
    if (hasta) match.fecha.$lte = hasta;
  }

  const [docs, zonas, hubs, fechas, tiempos] = await Promise.all([
    db
      .collection('metricas_diarias')
      .find(match)
      .sort({ fecha: -1 })
      .limit(5000)
      .toArray(),
    db.collection('metricas_diarias').distinct('zonaId'),
    db.collection('metricas_diarias').distinct('hubId'),
    db.collection('metricas_diarias').distinct('fecha'),
    db
      .collection('metricas_hub')
      .find(
        hub || desde || hasta
          ? {
              ...(hub ? { hubId: hub } : {}),
              ...(desde || hasta
                ? {
                    fecha: {
                      ...(desde ? { $gte: desde } : {}),
                      ...(hasta ? { $lte: hasta } : {}),
                    },
                  }
                : {}),
            }
          : {},
      )
      .sort({ fecha: -1 })
      .limit(500)
      .toArray(),
  ]);

  const total = acumulado(docs);

  const porZona = [...agrupar(docs, (d) => d.zonaId)].map(([zonaId, ds]) => {
    const acc = acumulado(ds);
    return {
      zonaId,
      ...acc,
      tasaPrimerIntento: tasaPrimerIntento(acc),
      tasaSla: tasaSla(acc),
    };
  });

  const porFranja = [...agrupar(docs, (d) => d.franja)].map(([franja, ds]) => {
    const acc = acumulado(ds);
    return {
      franja,
      ...acc,
      tasaPrimerIntento: tasaPrimerIntento(acc),
    };
  });

  const porZonaFranja = [...agrupar(docs, (d) => `${d.zonaId}|${d.franja}`)].map(
    ([clave, ds]) => {
      const [zonaId, franja] = clave.split('|');
      const acc = acumulado(ds);
      return {
        zonaId,
        franja,
        ...acc,
        tasaPrimerIntento: tasaPrimerIntento(acc),
      };
    },
  );

  const porFecha = [...agrupar(docs, (d) => d.fecha)].map(([fecha, ds]) => {
    const acc = acumulado(ds);
    return {
      fecha,
      ...acc,
      tasaPrimerIntento: tasaPrimerIntento(acc),
      tasaSla: tasaSla(acc),
    };
  });

  const slaPorHubDia = [...agrupar(docs, (d) => `${d.hubId}|${d.fecha}`)]
    .map(([clave, ds]) => {
      const [hubId, fecha] = clave.split('|');
      const acc = acumulado(ds);
      return { hubId, fecha, ...acc, tasaSla: tasaSla(acc) };
    })
    .sort((a, b) => b.fecha.localeCompare(a.fecha));

  const motivos = Object.entries(total.motivos)
    .map(([motivo, cantidad]) => ({ motivo, cantidad }))
    .sort((a, b) => b.cantidad - a.cantidad);

  const totalFallos = motivos.reduce((acc, m) => acc + m.cantidad, 0);

  const tiemposPorHub = [...agrupar(tiempos, (t) => t.hubId)].map(
    ([hubId, ts]) => {
      const cantidad = ts.reduce((acc, t) => acc + Number(t.cantidad ?? 0), 0);
      const suma = ts.reduce((acc, t) => acc + Number(t.sumaMinutos ?? 0), 0);
      return {
        hubId,
        cantidad,
        promedioMinutos: cantidad ? suma / cantidad : null,
      };
    },
  );

  return {
    filtros: { zona: zona ?? '', hub: hub ?? '', desde: desde ?? '', hasta: hasta ?? '' },
    catalogo: {
      zonas: zonas.filter(Boolean).sort(),
      hubs: hubs.filter(Boolean).sort(),
      fechas: fechas.filter(Boolean).sort().reverse().slice(0, 90),
    },
    total: {
      ...total,
      tasaPrimerIntento: tasaPrimerIntento(total),
      tasaSla: tasaSla(total),
      motivos,
      totalFallos,
    },
    porZona,
    porFranja,
    porZonaFranja,
    porFecha,
    slaPorHubDia,
    tiemposPorHub,
    metricas: docs.length,
    actualizadoEn: new Date().toISOString(),
  };
}

// ---------------------------------------------------------------- Panel de demo

async function traerJson(url, opciones = {}) {
  try {
    const respuesta = await fetch(url, {
      cache: 'no-store',
      signal: AbortSignal.timeout(3000),
      ...opciones,
    });
    const datos = await respuesta.json();
    return { ok: respuesta.ok, datos };
  } catch (error) {
    return { ok: false, error: error.message };
  }
}

export async function getDemo() {
  const db = await getDb();
  const controlUrl = process.env.CONTROL_URL || 'http://control:4100';
  const simuladorUrl = process.env.SIMULADOR_URL || 'http://simulador:4000';

  const [estado, lag, sim, dlq, reloj] = await Promise.all([
    traerJson(`${controlUrl}/control/estado`),
    traerJson(`${controlUrl}/control/lag`),
    traerJson(`${simuladorUrl}/sim/estado`),
    db.collection('dlq').find({}).sort({ detectadoEn: -1 }).limit(8).toArray(),
    traerJson(`${controlUrl}/control/reloj`),
  ]);

  return {
    servicios: estado.ok ? estado.datos.servicios : [],
    serviciosError: estado.ok ? null : estado.error ?? estado.datos?.error,
    lag: lag.ok ? lag.datos.grupos : [],
    lagError: lag.ok ? null : lag.error ?? lag.datos?.error,
    simulador: sim.ok ? sim.datos : null,
    simuladorError: sim.ok ? null : sim.error,
    reloj: reloj.ok ? reloj.datos : null,
    dlq: dlq.map((d) => ({
      _id: String(d._id),
      eventId: d.eventId ?? null,
      shipmentId: d.shipmentId ?? null,
      motivo: d.motivo,
      servicio: d.servicio,
      topic: d.topic,
      partition: d.partition,
      offset: d.offset,
      detectadoEn: iso(d.detectadoEn),
      payload: d.payload,
    })),
    actualizadoEn: new Date().toISOString(),
  };
}
