import { getDb } from '@/lib/mongo';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

// Colecciones cuya modificacion interesa a la interfaz.
// eventos_procesados queda afuera a proposito: se escribe en cada evento y
// generaria ruido sin aportar informacion visible.
const COLECCIONES = [
  'envios',
  'alertas',
  'notificaciones',
  'metricas_diarias',
  'metricas_hub',
  'dlq',
];

export async function GET(request) {
  const db = await getDb();
  const encoder = new TextEncoder();

  let cerrado = false;
  let heartbeat = null;
  const changeStreams = [];

  const enviar = (controller, bloque) => {
    if (cerrado) return;
    try {
      controller.enqueue(encoder.encode(bloque));
    } catch {
      cerrado = true;
    }
  };

  const cerrar = () => {
    if (cerrado) return;
    cerrado = true;
    if (heartbeat) clearInterval(heartbeat);
    for (const cs of changeStreams) {
      cs.close().catch(() => {});
    }
  };

  const stream = new ReadableStream({
    async start(controller) {
      request.signal.addEventListener('abort', cerrar);

      enviar(controller, 'retry: 3000\n\n');
      enviar(
        controller,
        `event: ping\ndata: ${JSON.stringify({ ts: Date.now() })}\n\n`,
      );

      for (const coleccion of COLECCIONES) {
        try {
          const cs = db
            .collection(coleccion)
            .watch([], { fullDocument: 'updateLookup' });

          cs.on('change', (cambio) => {
            enviar(
              controller,
              `event: cambio\ndata: ${JSON.stringify({
                coleccion,
                operacion: cambio.operationType,
                ts: Date.now(),
              })}\n\n`,
            );
          });
          cs.on('error', () => {
            // Una coleccion puede desaparecer si se reinicia la base.
            // El heartbeat mantiene viva la conexion y el cliente refresca.
          });
          changeStreams.push(cs);
        } catch {
          // Si la coleccion todavia no existe, se omite.
        }
      }

      heartbeat = setInterval(() => {
        enviar(
          controller,
          `event: ping\ndata: ${JSON.stringify({ ts: Date.now() })}\n\n`,
        );
      }, 20000);
    },
    cancel() {
      cerrar();
    },
  });

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    },
  });
}
