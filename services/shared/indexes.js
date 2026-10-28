// Indices declarados en la especificacion del TP.
// Se crean de forma idempotente al arrancar cada servicio.

const INDICES = [
  // Vista de envios
  ['envios', { shipmentId: 1 }, { unique: true, name: 'shipmentId_unico' }],
  ['envios', { estado: 1, zonaId: 1 }, { name: 'estado_zona' }],
  ['envios', { 'promesa.venceEn': 1 }, { name: 'promesa_vence' }],
  ['envios', { actualizadoEn: -1 }, { name: 'envios_actualizado' }],

  // Cola de alertas
  ['alertas', { shipmentId: 1, causa: 1 }, { unique: true, name: 'alerta_unica' }],
  [
    'alertas',
    { estado: 1, prioridadOrden: 1, abiertaEn: 1 },
    { name: 'cola_alertas' },
  ],
  ['alertas', { shipmentId: 1 }, { name: 'alertas_por_envio' }],

  // Notificaciones
  [
    'notificaciones',
    { notificacionId: 1 },
    { unique: true, name: 'notificacion_unica' },
  ],
  [
    'notificaciones',
    { shipmentId: 1, enviadaEn: -1 },
    { name: 'notificaciones_por_envio' },
  ],

  // Metricas
  [
    'metricas_diarias',
    { fecha: 1, zonaId: 1, franja: 1 },
    { unique: true, name: 'metrica_diaria_unica' },
  ],
  ['metricas_diarias', { fecha: 1, hubId: 1 }, { name: 'metrica_hub_dia' }],
  [
    'metricas_hub',
    { hubId: 1, fecha: 1 },
    { unique: true, name: 'metrica_hub_unica' },
  ],

  // Idempotencia de consumidores
  [
    'eventos_procesados',
    { consumidor: 1, eventId: 1 },
    { unique: true, name: 'evento_consumidor_unico' },
  ],
  [
    'eventos_procesados',
    { creadoEn: 1 },
    { expireAfterSeconds: 604800, name: 'evento_ttl_7d' },
  ],

  // Estado interno de servicios
  ['sla_envios', { shipmentId: 1 }, { unique: true, name: 'sla_envio_unico' }],
  ['sla_envios', { terminal: 1, venceEn: 1 }, { name: 'sla_abiertos' }],
  [
    'reprog_envios',
    { shipmentId: 1 },
    { unique: true, name: 'reprog_envio_unico' },
  ],
  [
    'replay_envios',
    { shipmentId: 1 },
    { unique: true, name: 'replay_envio_unico' },
  ],

  // Revision manual de la DLQ desde la interfaz
  ['dlq', { detectadoEn: -1 }, { name: 'dlq_fecha' }],
];

export async function ensureIndexes(db, logger) {
  const fallos = [];
  for (const [coleccion, claves, opciones] of INDICES) {
    try {
      await db.collection(coleccion).createIndex(claves, opciones);
    } catch (error) {
      // Un indice con las mismas claves pero otras opciones devuelve error:
      // se registra y se continua para no bloquear el arranque.
      fallos.push({
        coleccion,
        indice: opciones?.name ?? JSON.stringify(claves),
        error: error.message,
      });
      logger?.warn('No se pudo crear indice', {
        coleccion,
        indice: opciones?.name,
        error: error.message,
      });
    }
  }
  return fallos;
}

// Marca un evento como procesado por un consumidor.
// Devuelve true si ya estaba (duplicado) y false si se registro ahora.
export async function registrarEventoProcesado(db, consumidor, eventId) {
  if (!eventId) return false;
  try {
    await db.collection('eventos_procesados').insertOne({
      consumidor,
      eventId,
      creadoEn: new Date(),
    });
    return false;
  } catch (error) {
    if (error?.code === 11000) return true;
    throw error;
  }
}
