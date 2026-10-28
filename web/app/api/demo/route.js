import { NextResponse } from 'next/server';
import { getDemo } from '@/lib/queries';

export const dynamic = 'force-dynamic';

const SIMULADOR_URL = process.env.SIMULADOR_URL || 'http://simulador:4000';
const CONTROL_URL = process.env.CONTROL_URL || 'http://control:4100';

const ESCENARIOS = [
  'feliz',
  'ausente',
  'demorado',
  'tercer-fallo',
  'rafaga',
  'invalido',
];

const SERVICIOS = [
  'proyector',
  'notificaciones',
  'reprogramacion',
  'monitor-sla',
  'analitica',
  'analitica-replay',
  'simulador',
  'web',
];

const ACCIONES = ['stop', 'start', 'restart'];

async function llamar(url, opciones = {}) {
  const respuesta = await fetch(url, {
    cache: 'no-store',
    signal: AbortSignal.timeout(30000),
    ...opciones,
  });
  let datos = null;
  try {
    datos = await respuesta.json();
  } catch {
    datos = { ok: respuesta.ok };
  }
  return NextResponse.json(datos, {
    status: respuesta.status,
    headers: { 'Cache-Control': 'no-store' },
  });
}

export async function GET() {
  try {
    const datos = await getDemo();
    return NextResponse.json(datos, {
      headers: { 'Cache-Control': 'no-store' },
    });
  } catch (error) {
    return NextResponse.json(
      { error: error.message },
      { status: 500, headers: { 'Cache-Control': 'no-store' } },
    );
  }
}

export async function POST(request) {
  let cuerpo;
  try {
    cuerpo = await request.json();
  } catch {
    return NextResponse.json(
      { ok: false, error: 'Cuerpo JSON invalido' },
      { status: 400 },
    );
  }

  try {
    switch (cuerpo?.tipo) {
      case 'escenario': {
        if (!ESCENARIOS.includes(cuerpo.nombre)) {
          return NextResponse.json(
            { ok: false, error: `Escenario desconocido: ${cuerpo.nombre}` },
            { status: 400 },
          );
        }
        return llamar(
          `${SIMULADOR_URL}/sim/escenarios/${encodeURIComponent(cuerpo.nombre)}`,
          { method: 'POST' },
        );
      }
      case 'servicio': {
        if (
          !SERVICIOS.includes(cuerpo.servicio) ||
          !ACCIONES.includes(cuerpo.accion)
        ) {
          return NextResponse.json(
            { ok: false, error: 'Servicio o accion no permitidos' },
            { status: 400 },
          );
        }
        return llamar(
          `${CONTROL_URL}/control/servicio/${encodeURIComponent(
            cuerpo.servicio,
          )}/${encodeURIComponent(cuerpo.accion)}`,
          { method: 'POST' },
        );
      }
      case 'replay':
        return llamar(`${CONTROL_URL}/control/replay`, { method: 'POST' });
      case 'reconstruir':
        return llamar(`${CONTROL_URL}/control/reconstruir`, {
          method: 'POST',
        });
      default:
        return NextResponse.json(
          { ok: false, error: `Tipo de accion desconocido: ${cuerpo?.tipo}` },
          { status: 400 },
        );
    }
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: error.message },
      { status: 502 },
    );
  }
}
