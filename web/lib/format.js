export const ETIQUETAS_ESTADO = {
  CREADO: 'Creado',
  INGRESADO_HUB: 'Ingresado al hub',
  CLASIFICADO: 'Clasificado',
  EN_REPARTO: 'En reparto',
  ENTREGADO: 'Entregado',
  ENTREGA_FALLIDA: 'Entrega fallida',
};

export const ETIQUETAS_CAUSA = {
  SIN_INGRESO: 'Sin ingreso al hub',
  SIN_SALIDA: 'Sin salida a reparto',
  FALLO_DIA_LIMITE: 'Fallo en el dia limite',
  VENCIDO: 'Vencido',
};

export const ETIQUETAS_MOTIVO = {
  AUSENTE: 'Ausente',
  DIRECCION_INCORRECTA: 'Direccion incorrecta',
  RECHAZADO: 'Rechazado',
  ZONA_INACCESIBLE: 'Zona inaccesible',
};

export const ETIQUETAS_PRIORIDAD = {
  CRITICA: 'Critica',
  ALTA: 'Alta',
  MEDIA: 'Media',
};

export const ETIQUETAS_DECISION = {
  REPROGRAMADO: 'Reprogramado',
  RETENIDO: 'Retenido',
  DEVOLUCION: 'Devolucion al comercio',
};

export const ETIQUETAS_FRANJA = {
  MADRUGADA: 'Madrugada (00-06)',
  MANANA: 'Manana (06-12)',
  TARDE: 'Tarde (12-18)',
  NOCHE: 'Noche (18-24)',
};

export function fechaHora(iso) {
  if (!iso) return '—';
  const fecha = new Date(iso);
  if (Number.isNaN(fecha.getTime())) return '—';
  return `${fecha.toISOString().slice(0, 16).replace('T', ' ')} UTC`;
}

export function soloFecha(iso) {
  if (!iso) return '—';
  const fecha = new Date(iso);
  if (Number.isNaN(fecha.getTime())) return '—';
  return fecha.toISOString().slice(0, 10);
}

export function horaCorta(iso) {
  if (!iso) return '—';
  const fecha = new Date(iso);
  if (Number.isNaN(fecha.getTime())) return '—';
  return `${fecha.toISOString().slice(11, 16)} UTC`;
}

export function porcentaje(numerador, denominador) {
  if (!denominador) return null;
  return Math.round((numerador / denominador) * 1000) / 10;
}

export function formatearPorcentaje(valor) {
  if (valor === null || valor === undefined) return '—';
  return `${valor.toFixed(1)}%`;
}

export function formatearMinutos(minutos) {
  if (!Number.isFinite(minutos)) return '—';
  if (minutos < 60) return `${minutos.toFixed(1)} min`;
  const horas = Math.floor(minutos / 60);
  const resto = Math.round(minutos % 60);
  return `${horas} h ${resto} min`;
}

export function claseEstado(estado) {
  if (estado === 'ENTREGADO') return 'ok';
  if (estado === 'ENTREGA_FALLIDA') return 'bad';
  if (estado === 'EN_REPARTO') return 'info';
  return 'neutral';
}
