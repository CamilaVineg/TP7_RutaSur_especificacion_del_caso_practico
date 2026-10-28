// Constantes y utilidades compartidas por todos los servicios de RutaSur.

export const TOPICS = Object.freeze({
  ESTADOS: 'envios.estados',
  PROMESAS: 'envios.promesas',
  REPROGRAMACIONES: 'envios.reprogramaciones',
  ALERTAS: 'alertas.sla',
  NOTIFICACIONES: 'notificaciones.enviadas',
  DLQ: 'envios.estados.dlq',
});

export const GROUPS = Object.freeze({
  PROYECTOR: 'proyector',
  NOTIFICACIONES: 'notificaciones',
  REPROGRAMACION: 'reprogramacion',
  MONITOR_SLA: 'monitor-sla',
  ANALITICA: 'analitica',
  ANALITICA_REPLAY: 'analitica-replay',
});

export const ESTADOS = [
  'CREADO',
  'INGRESADO_HUB',
  'CLASIFICADO',
  'EN_REPARTO',
  'ENTREGADO',
  'ENTREGA_FALLIDA',
];

export const MOTIVOS = [
  'AUSENTE',
  'DIRECCION_INCORRECTA',
  'RECHAZADO',
  'ZONA_INACCESIBLE',
];

export const CAUSAS_SLA = [
  'SIN_INGRESO',
  'SIN_SALIDA',
  'FALLO_DIA_LIMITE',
  'VENCIDO',
];

export const PRIORIDADES = ['MEDIA', 'ALTA', 'CRITICA'];

export const PRIORIDAD_ORDEN = Object.freeze({
  CRITICA: 1,
  ALTA: 2,
  MEDIA: 3,
});

export const DECISIONES = ['REPROGRAMADO', 'RETENIDO', 'DEVOLUCION'];

export const FRANJAS = ['MADRUGADA', 'MANANA', 'TARDE', 'NOCHE'];

export const FRANJA_OPUESTA = Object.freeze({
  MADRUGADA: 'TARDE',
  TARDE: 'MADRUGADA',
  MANANA: 'NOCHE',
  NOCHE: 'MANANA',
});

// Hora simulada (UTC) en la que arranca cada franja horaria.
export const FRANJA_HORA = Object.freeze({
  MADRUGADA: 3,
  MANANA: 9,
  TARDE: 15,
  NOCHE: 21,
});

// Tres hubs, cuatro zonas. La tasa de fallo por zona va de 5% a 30%.
export const ZONAS = Object.freeze([
  { id: 'Z-NORTE', hubId: 'H-NORTE', tasaFallo: 0.05 },
  { id: 'Z-CENTRO', hubId: 'H-CENTRO', tasaFallo: 0.12 },
  { id: 'Z-SUR', hubId: 'H-SUR', tasaFallo: 0.2 },
  { id: 'Z-OESTE', hubId: 'H-CENTRO', tasaFallo: 0.3 },
]);

export const HUBS = [...new Set(ZONAS.map((z) => z.hubId))];

// Distribucion de motivos de fallo pedida por la catedra.
export const MOTIVO_PESOS = Object.freeze([
  ['AUSENTE', 0.6],
  ['DIRECCION_INCORRECTA', 0.2],
  ['RECHAZADO', 0.15],
  ['ZONA_INACCESIBLE', 0.05],
]);

export function zonaPorId(id) {
  return ZONAS.find((z) => z.id === id) ?? ZONAS[0];
}

export function hubDeZona(zonaId) {
  return zonaPorId(zonaId).hubId;
}

export function envNumber(nombre, porDefecto) {
  const valor = Number(process.env[nombre]);
  return Number.isFinite(valor) ? valor : porDefecto;
}

export function envString(nombre, porDefecto) {
  const valor = process.env[nombre];
  return valor === undefined || valor === '' ? porDefecto : valor;
}

export const cfg = {
  get kafkaBrokers() {
    return envString('KAFKA_BROKERS', 'kafka:9092')
      .split(',')
      .map((b) => b.trim())
      .filter(Boolean);
  },
  get mongoUri() {
    return envString('MONGO_URI', 'mongodb://mongo:27017/rutasur?replicaSet=rs0');
  },
  get dbName() {
    return envString('MONGO_DB', 'rutasur');
  },
  get speedup() {
    return envNumber('SIM_SPEEDUP', 120);
  },
  get logLevel() {
    return envString('LOG_LEVEL', 'info');
  },
};

export function horaUtc(fecha) {
  return new Date(fecha).getUTCHours();
}

export function franjaDe(fecha) {
  const h = horaUtc(fecha);
  if (h < 6) return 'MADRUGADA';
  if (h < 12) return 'MANANA';
  if (h < 18) return 'TARDE';
  return 'NOCHE';
}

export function fechaDe(fecha) {
  return new Date(fecha).toISOString().slice(0, 10);
}

// Proximo dia habil (lunes a viernes) en la franja opuesta a la del fallo.
export function proximoIntento(fechaFallo, franjaFallo) {
  const d = new Date(fechaFallo);
  d.setUTCDate(d.getUTCDate() + 1);
  while (d.getUTCDay() === 0 || d.getUTCDay() === 6) {
    d.setUTCDate(d.getUTCDate() + 1);
  }
  const opuesta = FRANJA_OPUESTA[franjaFallo] ?? 'MANANA';
  d.setUTCHours(FRANJA_HORA[opuesta], 0, 0, 0);
  return d;
}

export function esDiaHabil(fecha) {
  const dia = new Date(fecha).getUTCDay();
  return dia !== 0 && dia !== 6;
}
