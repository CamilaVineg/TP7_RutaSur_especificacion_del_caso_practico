import { z } from 'zod';
import {
  CAUSAS_SLA,
  DECISIONES,
  ESTADOS,
  MOTIVOS,
  PRIORIDADES,
} from './config.js';

const iso = z.string().datetime({ offset: true });

const base = {
  eventId: z.string().min(8),
  shipmentId: z.string().min(3),
  ocurridoEn: iso,
  version: z.number().int().positive(),
};

export const estadoSchema = z
  .object({
    ...base,
    tipo: z.enum(ESTADOS),
    secuencia: z.number().int().nonnegative(),
    hubId: z.string().optional(),
    zonaId: z.string().optional(),
    repartidorId: z.string().optional(),
    motivo: z.enum(MOTIVOS).optional(),
    intento: z.number().int().positive().optional(),
    promesaVenceEn: iso.optional(),
  })
  .superRefine((valor, ctx) => {
    if (valor.tipo === 'ENTREGA_FALLIDA' && !valor.motivo) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ['motivo'],
        message: 'ENTREGA_FALLIDA requiere motivo',
      });
    }
  });

export const promesaSchema = z.object({
  ...base,
  venceEn: iso,
  hubId: z.string().optional(),
  zonaId: z.string().optional(),
});

export const reprogramacionSchema = z.object({
  ...base,
  motivo: z.enum(MOTIVOS),
  intento: z.number().int().positive(),
  decision: z.enum(DECISIONES),
  proximoIntentoEn: iso.nullable().optional(),
  franjaFallo: z.string().optional(),
});

export const alertaSchema = z.object({
  ...base,
  causa: z.enum(CAUSAS_SLA),
  prioridad: z.enum(PRIORIDADES),
  abierta: z.boolean(),
  venceEn: iso.optional(),
  mensaje: z.string().optional(),
  hubId: z.string().optional(),
  zonaId: z.string().optional(),
});

export const notificacionSchema = z.object({
  ...base,
  canal: z.string(),
  tipo: z.string(),
  mensaje: z.string(),
  opciones: z.array(z.string()).optional(),
  origenEventId: z.string().optional(),
  motivo: z.enum(MOTIVOS).optional(),
  prioridad: z.enum(PRIORIDADES).optional(),
});

function validar(schema, payload) {
  const resultado = schema.safeParse(payload);
  if (resultado.success) {
    return { ok: true, valor: resultado.data };
  }
  const motivo = resultado.error.issues
    .map((issue) => `${issue.path.join('.') || '(raiz)'}: ${issue.message}`)
    .join('; ');
  return { ok: false, motivo };
}

export const validarEstado = (payload) => validar(estadoSchema, payload);
export const validarPromesa = (payload) => validar(promesaSchema, payload);
export const validarReprogramacion = (payload) =>
  validar(reprogramacionSchema, payload);
export const validarAlerta = (payload) => validar(alertaSchema, payload);
export const validarNotificacion = (payload) =>
  validar(notificacionSchema, payload);

// Los eventos de estado que cierran el ciclo de vida del envio.
export function esEstadoTerminal(tipo, motivo, intento) {
  if (tipo === 'ENTREGADO') return true;
  if (tipo === 'ENTREGA_FALLIDA') {
    if (motivo === 'RECHAZADO') return true;
    if ((intento ?? 1) >= 3) return true;
  }
  return false;
}
