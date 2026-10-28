'use client';

import Link from 'next/link';
import { useCallback, useState } from 'react';

import { IndicadorConexion, useLive } from './useLive';
import {
  ETIQUETAS_CAUSA,
  ETIQUETAS_ESTADO,
  ETIQUETAS_PRIORIDAD,
  claseEstado,
  fechaHora,
} from '@/lib/format';

const ORDEN_ESTADOS = [
  'CREADO',
  'INGRESADO_HUB',
  'CLASIFICADO',
  'EN_REPARTO',
  'ENTREGADO',
  'ENTREGA_FALLIDA',
];

export default function TorreLive({ inicial }) {
  const [datos, setDatos] = useState(inicial);
  const [error, setError] = useState(null);

  const refrescar = useCallback(async () => {
    try {
      const respuesta = await fetch('/api/torre', { cache: 'no-store' });
      if (!respuesta.ok) throw new Error(`HTTP ${respuesta.status}`);
      setDatos(await respuesta.json());
      setError(null);
    } catch (err) {
      setError(`No se pudo actualizar la torre: ${err.message}`);
    }
  }, []);

  const { conectado, ultimaActualizacion } = useLive(refrescar, {
    intervaloMs: 5000,
  });

  const conteos = new Map(
    (datos?.conteos ?? []).map((c) => [c.estado, c.total]),
  );
  const total = datos?.total ?? 0;
  const maximo = Math.max(1, ...ORDEN_ESTADOS.map((e) => conteos.get(e) ?? 0));
  const alertas = datos?.alertas ?? [];
  const recientes = datos?.recientes ?? [];

  return (
    <>
      <div className="encabezado">
        <div>
          <h1>Torre de control</h1>
          <p className="subtitulo">
            Estado agregado de la operacion, cola de alertas SLA y ultimos
            movimientos. Se actualiza solo con cada evento.
          </p>
        </div>
        <div className="fila">
          <IndicadorConexion
            conectado={conectado}
            ultimaActualizacion={ultimaActualizacion}
          />
          <span className="pildora">Actualizado {fechaHora(datos?.actualizadoEn)}</span>
        </div>
      </div>

      {error ? (
        <div className="aviso error" style={{ marginBottom: 16 }}>
          {error}
        </div>
      ) : null}

      <section className="grid grid-4" style={{ marginBottom: 18 }}>
        <div className="kpi">
          <div className="kpi-valor">{total}</div>
          <div className="kpi-etiqueta">Envios en el sistema</div>
        </div>
        <div className="kpi">
          <div className="kpi-valor texto-ok">{conteos.get('ENTREGADO') ?? 0}</div>
          <div className="kpi-etiqueta">Entregados</div>
        </div>
        <div className="kpi">
          <div className="kpi-valor" style={{ color: 'var(--info)' }}>
            {conteos.get('EN_REPARTO') ?? 0}
          </div>
          <div className="kpi-etiqueta">En reparto</div>
        </div>
        <div className="kpi">
          <div className="kpi-valor texto-malo">
            {conteos.get('ENTREGA_FALLIDA') ?? 0}
          </div>
          <div className="kpi-etiqueta">Con entrega fallida</div>
        </div>
        <div className="kpi">
          <div className="kpi-valor texto-alerta">{alertas.length}</div>
          <div className="kpi-etiqueta">Alertas SLA abiertas</div>
        </div>
      </section>

      <div className="grid grid-2" style={{ marginBottom: 18 }}>
        <section className="panel">
          <h2>Distribucion por estado</h2>
          {total === 0 ? (
            <p className="mudo">
              Todavia no hay envios proyectados. Inicia el simulador o dispara
              un escenario desde el panel de demo.
            </p>
          ) : (
            ORDEN_ESTADOS.map((estado) => {
              const valor = conteos.get(estado) ?? 0;
              return (
                <div className="linea-estado" key={estado}>
                  <span className={`badge ${claseEstado(estado)}`}>
                    {ETIQUETAS_ESTADO[estado]}
                  </span>
                  <span className="barra">
                    <span
                      style={{
                        width: `${Math.max(valor === 0 ? 0 : 3, (valor / maximo) * 100)}%`,
                      }}
                    />
                  </span>
                  <span className="valor">{valor}</span>
                </div>
              );
            })
          )}
        </section>

        <section className="panel">
          <div className="entre">
            <h2>Cola de alertas SLA</h2>
            <span className="mudo">Prioridad y antiguedad</span>
          </div>
          {alertas.length === 0 ? (
            <p className="mudo">
              No hay alertas abiertas. Se abren solas cuando un envio entra en
              riesgo de incumplir la promesa o la supera.
            </p>
          ) : (
            <div className="tabla-envoltura">
              <table>
                <thead>
                  <tr>
                    <th>Envio</th>
                    <th>Causa</th>
                    <th>Prioridad</th>
                    <th>Abierta</th>
                  </tr>
                </thead>
                <tbody>
                  {alertas.map((alerta) => (
                    <tr key={`${alerta.shipmentId}-${alerta.causa}`}>
                      <td>
                        <Link
                          className="codigo"
                          href={`/envios/${alerta.shipmentId}`}
                        >
                          {alerta.shipmentId}
                        </Link>
                        <div className="mudo">{alerta.zonaId ?? 'sin zona'}</div>
                      </td>
                      <td>{ETIQUETAS_CAUSA[alerta.causa] ?? alerta.causa}</td>
                      <td>
                        <span
                          className={`badge ${(alerta.prioridad ?? '').toLowerCase()}`}
                        >
                          {ETIQUETAS_PRIORIDAD[alerta.prioridad] ??
                            alerta.prioridad}
                        </span>
                      </td>
                      <td className="mudo">{fechaHora(alerta.abiertaEn)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </div>

      <section className="panel ajustado">
        <div className="entre" style={{ padding: '16px 18px' }}>
          <h2 style={{ margin: 0 }}>Ultimos envios con movimiento</h2>
          <span className="mudo">
            {recientes.length} de {total} envios
          </span>
        </div>
        {recientes.length === 0 ? (
          <div className="centrado mudo">Sin movimientos registrados.</div>
        ) : (
          <div className="tabla-envoltura">
            <table>
              <thead>
                <tr>
                  <th>Envio</th>
                  <th>Estado</th>
                  <th>Zona / Hub</th>
                  <th>Ultimo evento</th>
                  <th>Actualizado</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {recientes.map((envio) => (
                  <tr key={envio.shipmentId}>
                    <td>
                      <Link
                        className="codigo"
                        href={`/envios/${envio.shipmentId}`}
                      >
                        {envio.shipmentId}
                      </Link>
                    </td>
                    <td>
                      <span className={`badge ${claseEstado(envio.estado)}`}>
                        {ETIQUETAS_ESTADO[envio.estado] ?? envio.estado}
                      </span>
                    </td>
                    <td className="mudo">
                      {envio.zonaId ?? '—'} / {envio.hubId ?? '—'}
                    </td>
                    <td className="mudo">
                      {ETIQUETAS_ESTADO[envio.ultimoTipo] ?? envio.ultimoTipo ?? '—'}
                      <div>{fechaHora(envio.ultimoEventoEn)}</div>
                    </td>
                    <td className="mudo">{fechaHora(envio.actualizadoEn)}</td>
                    <td>
                      <Link
                        className="boton chico"
                        href={`/tracking/${envio.shipmentId}`}
                      >
                        Vista publica
                      </Link>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </>
  );
}
