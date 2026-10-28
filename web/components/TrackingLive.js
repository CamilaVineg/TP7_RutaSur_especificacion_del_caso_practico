'use client';

import Link from 'next/link';
import { useCallback, useState } from 'react';

import { IndicadorConexion, useLive } from './useLive';
import { ETIQUETAS_ESTADO, claseEstado, fechaHora } from '@/lib/format';

// Vista publica: solo estado, promesa y fechas de los hitos.
// No muestra hub, zona, repartidor, intentos, alertas ni notificaciones.
export default function TrackingLive({ shipmentId, inicial }) {
  const [datos, setDatos] = useState(inicial);
  const [error, setError] = useState(null);

  const refrescar = useCallback(async () => {
    try {
      const respuesta = await fetch(
        `/api/tracking/${encodeURIComponent(shipmentId)}`,
        { cache: 'no-store' },
      );
      if (respuesta.status === 404) {
        setDatos({ envio: null });
        return;
      }
      if (!respuesta.ok) throw new Error(`HTTP ${respuesta.status}`);
      setDatos(await respuesta.json());
      setError(null);
    } catch (err) {
      setError(`No se pudo actualizar el seguimiento: ${err.message}`);
    }
  }, [shipmentId]);

  const { conectado, ultimaActualizacion } = useLive(refrescar, {
    intervaloMs: 5000,
  });

  const envio = datos?.envio;

  if (!envio) {
    return (
      <section className="panel centrado" style={{ maxWidth: 620, margin: '0 auto' }}>
        <h1>Seguimiento no disponible</h1>
        <p className="subtitulo">
          No encontramos el envio <span className="codigo">{shipmentId}</span>.
          Revisa el numero de seguimiento.
        </p>
        <Link className="boton" href="/torre">
          Volver a la torre
        </Link>
      </section>
    );
  }

  const historial = [...(envio.historial ?? [])].sort(
    (a, b) => new Date(a.ocurridoEn) - new Date(b.ocurridoEn),
  );

  return (
    <section style={{ maxWidth: 760, margin: '0 auto' }}>
      <div className="encabezado">
        <div>
          <p className="mudo" style={{ margin: 0 }}>
            Seguimiento publico
          </p>
          <h1>{shipmentId}</h1>
          <span className={`badge ${claseEstado(envio.estado)}`}>
            {ETIQUETAS_ESTADO[envio.estado] ?? envio.estado}
          </span>
        </div>
        <IndicadorConexion
          conectado={conectado}
          ultimaActualizacion={ultimaActualizacion}
        />
      </div>

      {error ? (
        <div className="aviso error" style={{ marginBottom: 16 }}>
          {error}
        </div>
      ) : null}

      <section className="panel" style={{ marginBottom: 18 }}>
        <div className="entre">
          <div>
            <div className="kpi-etiqueta">Entrega prometida</div>
            <div className="kpi-valor" style={{ fontSize: 24 }}>
              {fechaHora(envio.promesaVenceEn)}
            </div>
          </div>
          <div style={{ textAlign: 'right' }}>
            <div className="kpi-etiqueta">Ultima actualizacion</div>
            <div style={{ marginTop: 8 }}>{fechaHora(envio.actualizadoEn)}</div>
          </div>
        </div>
      </section>

      <section className="panel">
        <h2>Recorrido del envio</h2>
        {historial.length === 0 ? (
          <p className="mudo">
            Todavia no hay movimientos registrados para este envio.
          </p>
        ) : (
          <ul className="timeline">
            {historial.map((evento, indice) => (
              <li key={`${evento.tipo}-${evento.ocurridoEn}-${indice}`}>
                <div className="titulo">
                  {ETIQUETAS_ESTADO[evento.tipo] ?? evento.tipo}
                </div>
                <div className="detalle">{fechaHora(evento.ocurridoEn)}</div>
              </li>
            ))}
          </ul>
        )}
      </section>

      <p className="mudo" style={{ marginTop: 18 }}>
        Esta vista no expone datos internos de la operacion (hub, zona,
        repartidor, intentos ni alertas).
      </p>
    </section>
  );
}
