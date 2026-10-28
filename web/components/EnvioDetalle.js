'use client';

import Link from 'next/link';
import { useCallback, useState } from 'react';

import { IndicadorConexion, useLive } from './useLive';
import {
  ETIQUETAS_CAUSA,
  ETIQUETAS_DECISION,
  ETIQUETAS_ESTADO,
  ETIQUETAS_FRANJA,
  ETIQUETAS_MOTIVO,
  ETIQUETAS_PRIORIDAD,
  claseEstado,
  fechaHora,
} from '@/lib/format';

export default function EnvioDetalle({ shipmentId, inicial }) {
  const [datos, setDatos] = useState(inicial);
  const [error, setError] = useState(null);

  const refrescar = useCallback(async () => {
    try {
      const respuesta = await fetch(
        `/api/envios/${encodeURIComponent(shipmentId)}`,
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
      setError(`No se pudo actualizar el detalle: ${err.message}`);
    }
  }, [shipmentId]);

  const { conectado, ultimaActualizacion } = useLive(refrescar, {
    intervaloMs: 5000,
  });

  const envio = datos?.envio;

  if (!envio) {
    return (
      <section className="panel centrado">
        <h1>Envio no encontrado</h1>
        <p className="subtitulo">
          Todavia no se proyecto ningun evento para{' '}
          <span className="codigo">{shipmentId}</span>.
        </p>
        <Link className="boton" href="/torre">
          Volver a la torre
        </Link>
      </section>
    );
  }

  const historial = [...(envio.historial ?? [])].sort(
    (a, b) => (b.secuencia ?? 0) - (a.secuencia ?? 0),
  );
  const reprogramaciones = [...(envio.reprogramaciones ?? [])].sort(
    (a, b) => new Date(b.ocurridoEn) - new Date(a.ocurridoEn),
  );
  const alertas = datos.alertas ?? [];
  const notificaciones = datos.notificaciones ?? [];

  return (
    <>
      <div className="encabezado">
        <div>
          <div className="fila" style={{ marginBottom: 6 }}>
            <Link className="mudo" href="/torre">
              ← Torre de control
            </Link>
            <span className="mudo">/</span>
            <span className="codigo">{shipmentId}</span>
          </div>
          <h1>Detalle del envio</h1>
          <div className="fila" style={{ marginTop: 8 }}>
            <span className={`badge ${claseEstado(envio.estado)}`}>
              {ETIQUETAS_ESTADO[envio.estado] ?? envio.estado}
            </span>
            {envio.terminal ? (
              <span className="badge neutral">Ciclo cerrado</span>
            ) : (
              <span className="badge info">En curso</span>
            )}
            <span className="pildora">
              Secuencia aplicada: {envio.ultimaSecuencia ?? '—'}
            </span>
          </div>
        </div>
        <div className="fila">
          <IndicadorConexion
            conectado={conectado}
            ultimaActualizacion={ultimaActualizacion}
          />
          <Link className="boton" href={`/tracking/${shipmentId}`}>
            Ver vista publica
          </Link>
        </div>
      </div>

      {error ? (
        <div className="aviso error" style={{ marginBottom: 16 }}>
          {error}
        </div>
      ) : null}

      <section className="grid grid-4" style={{ marginBottom: 18 }}>
        <div className="kpi">
          <div className="kpi-valor" style={{ fontSize: 20 }}>
            {fechaHora(envio.promesa?.venceEn)}
          </div>
          <div className="kpi-etiqueta">Promesa de entrega</div>
        </div>
        <div className="kpi">
          <div className="kpi-valor" style={{ fontSize: 20 }}>
            {envio.entregadoEn ? fechaHora(envio.entregadoEn) : 'Pendiente'}
          </div>
          <div className="kpi-etiqueta">Entrega efectiva</div>
        </div>
        <div className="kpi">
          <div className="kpi-valor">{envio.intentosFallidos ?? 0}</div>
          <div className="kpi-etiqueta">Intentos fallidos</div>
        </div>
        <div className="kpi">
          <div className="kpi-valor" style={{ fontSize: 20 }}>
            {envio.entregadoPrimerIntento === true
              ? 'Si'
              : envio.entregadoPrimerIntento === false
                ? 'No'
                : '—'}
          </div>
          <div className="kpi-etiqueta">Entregado al primer intento</div>
        </div>
      </section>

      <div className="grid grid-2">
        <section className="panel">
          <h2>Historial de estados</h2>
          {historial.length === 0 ? (
            <p className="mudo">Sin eventos de estado proyectados.</p>
          ) : (
            <ul className="timeline">
              {historial.map((evento) => (
                <li key={evento.eventId}>
                  <div className="titulo">
                    {ETIQUETAS_ESTADO[evento.tipo] ?? evento.tipo}
                  </div>
                  <div className="detalle">
                    {fechaHora(evento.ocurridoEn)} · secuencia{' '}
                    {evento.secuencia ?? '—'}
                    {evento.intento ? ` · intento ${evento.intento}` : ''}
                  </div>
                  <div className="detalle">
                    {evento.hubId ? `Hub ${evento.hubId}` : ''}
                    {evento.zonaId ? ` · Zona ${evento.zonaId}` : ''}
                    {evento.repartidorId ? ` · ${evento.repartidorId}` : ''}
                  </div>
                  {evento.motivo ? (
                    <div className="detalle">
                      Motivo:{' '}
                      {ETIQUETAS_MOTIVO[evento.motivo] ?? evento.motivo}
                    </div>
                  ) : null}
                </li>
              ))}
            </ul>
          )}
        </section>

        <div className="grid" style={{ alignContent: 'start' }}>
          <section className="panel">
            <h2>Datos operativos</h2>
            <dl className="dos-columnas">
              <dt>Zona</dt>
              <dd>{envio.zonaId ?? '—'}</dd>
              <dt>Hub</dt>
              <dd>{envio.hubId ?? '—'}</dd>
              <dt>Repartidor</dt>
              <dd>{envio.repartidorId ?? '—'}</dd>
              <dt>Intento actual</dt>
              <dd>{envio.intentoActual ?? '—'}</dd>
              <dt>Ultimo motivo</dt>
              <dd>
                {envio.ultimoMotivo
                  ? (ETIQUETAS_MOTIVO[envio.ultimoMotivo] ?? envio.ultimoMotivo)
                  : '—'}
              </dd>
              <dt>Creado</dt>
              <dd>{fechaHora(envio.creadoEn)}</dd>
              <dt>Ultimo evento</dt>
              <dd>{fechaHora(envio.ultimoEventoEn)}</dd>
              <dt>Actualizado</dt>
              <dd>{fechaHora(envio.actualizadoEn)}</dd>
              <dt>Proximo intento</dt>
              <dd>{fechaHora(envio.proximoIntentoEn)}</dd>
            </dl>
          </section>

          <section className="panel">
            <h2>Reprogramaciones</h2>
            {reprogramaciones.length === 0 ? (
              <p className="mudo">Sin reprogramaciones registradas.</p>
            ) : (
              <ul className="lista-limpia">
                {reprogramaciones.map((r) => (
                  <li key={r.eventId}>
                    <div className="entre">
                      <span className="badge info">
                        {ETIQUETAS_DECISION[r.decision] ?? r.decision}
                      </span>
                      <span className="mudo">{fechaHora(r.ocurridoEn)}</span>
                    </div>
                    <div className="mudo" style={{ marginTop: 6 }}>
                      Motivo {ETIQUETAS_MOTIVO[r.motivo] ?? r.motivo} · intento{' '}
                      {r.intento} · franja{' '}
                      {ETIQUETAS_FRANJA[r.franjaFallo] ?? r.franjaFallo ?? '—'}
                    </div>
                    {r.proximoIntentoEn ? (
                      <div className="mudo">
                        Reintento previsto: {fechaHora(r.proximoIntentoEn)}
                      </div>
                    ) : null}
                  </li>
                ))}
              </ul>
            )}
          </section>
        </div>
      </div>

      <div className="grid grid-2" style={{ marginTop: 18 }}>
        <section className="panel">
          <h2>Alertas de SLA</h2>
          {alertas.length === 0 ? (
            <p className="mudo">El envio no genero alertas.</p>
          ) : (
            <ul className="lista-limpia">
              {alertas.map((alerta) => (
                <li key={`${alerta.causa}-${alerta.abiertaEn}`}>
                  <div className="entre">
                    <strong>
                      {ETIQUETAS_CAUSA[alerta.causa] ?? alerta.causa}
                    </strong>
                    <span
                      className={`badge ${(alerta.prioridad ?? '').toLowerCase()}`}
                    >
                      {ETIQUETAS_PRIORIDAD[alerta.prioridad] ?? alerta.prioridad}
                    </span>
                  </div>
                  <div className="mudo" style={{ marginTop: 4 }}>
                    {alerta.estado === 'ABIERTA' ? 'Abierta' : 'Cerrada'} ·{' '}
                    {fechaHora(alerta.abiertaEn)}
                  </div>
                  {alerta.mensaje ? (
                    <div className="mudo">{alerta.mensaje}</div>
                  ) : null}
                </li>
              ))}
            </ul>
          )}
        </section>

        <section className="panel">
          <h2>Notificaciones emitidas</h2>
          {notificaciones.length === 0 ? (
            <p className="mudo">
              Sin notificaciones. Se emiten ante entregas fallidas y riesgos de
              SLA.
            </p>
          ) : (
            <ul className="lista-limpia">
              {notificaciones.map((n) => (
                <li key={n.notificacionId}>
                  <div className="entre">
                    <span className="badge neutral">{n.tipo}</span>
                    <span className="mudo">{fechaHora(n.enviadaEn)}</span>
                  </div>
                  <div style={{ marginTop: 6 }}>{n.mensaje}</div>
                  <div className="fila" style={{ marginTop: 6 }}>
                    <span className="mudo">Canal {n.canal}</span>
                    {(n.opciones ?? []).map((opcion) => (
                      <span className="badge neutral" key={opcion}>
                        {opcion}
                      </span>
                    ))}
                  </div>
                </li>
              ))}
            </ul>
          )}
        </section>
      </div>
    </>
  );
}
