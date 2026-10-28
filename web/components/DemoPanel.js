'use client';

import Link from 'next/link';
import { useCallback, useState } from 'react';

import { IndicadorConexion, useLive } from './useLive';
import { fechaHora } from '@/lib/format';

const ESCENARIOS = [
  {
    nombre: 'feliz',
    titulo: 'Camino feliz',
    detalle: 'Creado, hub, clasificado y entregado sin fallos.',
  },
  {
    nombre: 'ausente',
    titulo: 'Destinatario ausente',
    detalle: 'El primer intento falla y el reintento termina entregando.',
  },
  {
    nombre: 'demorado',
    titulo: 'Demorado en hub',
    detalle: 'Se detiene en clasificado y dispara la alerta SIN_SALIDA.',
  },
  {
    nombre: 'tercer-fallo',
    titulo: 'Tercer fallo',
    detalle: 'Tres intentos fallidos: se decide devolucion al comercio.',
  },
  {
    nombre: 'rafaga',
    titulo: 'Rafaga de 500',
    detalle: 'Mide throughput del proyector y el lag de los grupos.',
  },
  {
    nombre: 'invalido',
    titulo: 'Evento invalido',
    detalle: 'Publica un evento malformado que debe caer en la DLQ.',
  },
];

const ACCIONES_SERVICIO = [
  ['stop', 'Detener'],
  ['start', 'Arrancar'],
  ['restart', 'Reiniciar'],
];

// Coincide con la lista blanca de /api/demo: Kafka, Mongo y el propio control
// no se operan desde la interfaz.
const OPERABLES = new Set([
  'proyector',
  'notificaciones',
  'reprogramacion',
  'monitor-sla',
  'analitica',
  'analitica-replay',
  'simulador',
  'web',
]);

function colorServicio(estado) {
  if (estado === 'running') return 'ok';
  if (estado === 'exited' || estado === 'dead') return 'bad';
  return 'warn';
}

export default function DemoPanel({ inicial }) {
  const [datos, setDatos] = useState(inicial);
  const [error, setError] = useState(null);
  const [ocupado, setOcupado] = useState(null);
  const [resultado, setResultado] = useState(null);

  const refrescar = useCallback(async () => {
    try {
      const respuesta = await fetch('/api/demo', { cache: 'no-store' });
      if (!respuesta.ok) throw new Error(`HTTP ${respuesta.status}`);
      setDatos(await respuesta.json());
      setError(null);
    } catch (err) {
      setError(`No se pudo actualizar el panel: ${err.message}`);
    }
  }, []);

  const { conectado, ultimaActualizacion } = useLive(refrescar, {
    intervaloMs: 4000,
  });

  const accion = async (cuerpo, etiqueta) => {
    setOcupado(etiqueta);
    setResultado(null);
    try {
      const respuesta = await fetch('/api/demo', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(cuerpo),
      });
      const datosRespuesta = await respuesta.json().catch(() => ({}));
      const ok = respuesta.ok && datosRespuesta.ok !== false;
      setResultado({
        ok,
        texto: ok
          ? datosRespuesta.mensaje || `Listo: ${etiqueta}`
          : datosRespuesta.error || `Fallo: ${etiqueta}`,
        shipmentId: datosRespuesta.shipmentId ?? null,
        trackingUrl: datosRespuesta.trackingUrl ?? null,
      });
      await refrescar();
    } catch (err) {
      setResultado({ ok: false, texto: `Error de red: ${err.message}` });
    } finally {
      setOcupado(null);
    }
  };

  const simulador = datos?.simulador ?? null;
  const reloj = datos?.reloj ?? null;
  const contadores = simulador?.contadores ?? {};
  const configuracion = simulador?.configuracion ?? {};
  const servicios = datos?.servicios ?? [];
  const grupos = datos?.lag ?? [];
  const dlq = datos?.dlq ?? [];

  return (
    <>
      <div className="encabezado">
        <div>
          <h1>Panel de demo</h1>
          <p className="subtitulo">
            Reloj simulado, escenarios, estado de los servicios, lag por grupo
            de consumidores y revision de la DLQ.
          </p>
        </div>
        <div className="fila">
          <IndicadorConexion
            conectado={conectado}
            ultimaActualizacion={ultimaActualizacion}
          />
          <button
            className="boton"
            type="button"
            onClick={() => refrescar()}
            disabled={ocupado !== null}
          >
            Actualizar
          </button>
        </div>
      </div>

      {error ? (
        <div className="aviso error" style={{ marginBottom: 16 }}>
          {error}
        </div>
      ) : null}

      {resultado ? (
        <div
          className={`aviso ${resultado.ok ? 'ok' : 'error'}`}
          style={{ marginBottom: 16 }}
        >
          <div>{resultado.texto}</div>
          {resultado.shipmentId ? (
            <div className="fila" style={{ marginTop: 8 }}>
              <Link
                className="boton chico"
                href={`/envios/${resultado.shipmentId}`}
              >
                Detalle {resultado.shipmentId}
              </Link>
              <Link
                className="boton chico"
                href={`/tracking/${resultado.shipmentId}`}
              >
                Tracking publico
              </Link>
            </div>
          ) : null}
        </div>
      ) : null}

      <div className="grid grid-2" style={{ marginBottom: 18 }}>
        <section className="panel">
          <div className="entre">
            <h2>Reloj simulado</h2>
            <span className="pildora">
              factor {reloj?.factor ?? simulador?.reloj?.factor ?? '—'}x
            </span>
          </div>
          <div className="kpi-valor" style={{ fontSize: 24 }}>
            {fechaHora(reloj?.ahora ?? simulador?.reloj?.ahora)}
          </div>
          <p className="mudo" style={{ marginTop: 8 }}>
            1 hora simulada equivale a 30 segundos reales. El monitor de SLA
            evalua con este reloj, no con el de la maquina.
          </p>
          <div className="separador" />
          <div className="grid grid-3">
            <div>
              <div className="kpi-valor" style={{ fontSize: 20 }}>
                {contadores.enviosCreados ?? 0}
              </div>
              <div className="kpi-etiqueta">Envios creados</div>
            </div>
            <div>
              <div className="kpi-valor" style={{ fontSize: 20 }}>
                {contadores.eventosPublicados ?? 0}
              </div>
              <div className="kpi-etiqueta">Eventos publicados</div>
            </div>
            <div>
              <div className="kpi-valor" style={{ fontSize: 20 }}>
                {contadores.entregados ?? 0}
              </div>
              <div className="kpi-etiqueta">Entregados</div>
            </div>
            <div>
              <div className="kpi-valor" style={{ fontSize: 20 }}>
                {contadores.fallidos ?? 0}
              </div>
              <div className="kpi-etiqueta">Fallidos</div>
            </div>
            <div>
              <div className="kpi-valor" style={{ fontSize: 20 }}>
                {contadores.duplicados ?? 0}
              </div>
              <div className="kpi-etiqueta">Duplicados inyectados</div>
            </div>
            <div>
              <div className="kpi-valor" style={{ fontSize: 20 }}>
                {contadores.malformados ?? 0}
              </div>
              <div className="kpi-etiqueta">Malformados inyectados</div>
            </div>
          </div>
          <div className="separador" />
          <div className="mudo">
            {configuracion.enviosPorMinuto ?? '—'} envios/min · semilla{' '}
            {configuracion.semilla ?? '—'} · agenda{' '}
            {simulador?.agenda ?? '—'} tareas pendientes
          </div>
          <div className="mudo">
            Demora hub {configuracion.probDemoraHub ?? '—'} · duplicado{' '}
            {configuracion.probDuplicado ?? '—'} · desorden{' '}
            {configuracion.probDesorden ?? '—'} · malformado{' '}
            {configuracion.probMalformado ?? '—'}
          </div>
        </section>

        <section className="panel">
          <h2>Escenarios</h2>
          <p className="mudo" style={{ marginTop: 0 }}>
            Cada boton publica eventos reales en Kafka. Los tiempos se
            comprimen para poder ver el ciclo completo en segundos.
          </p>
          <div className="acciones">
            {ESCENARIOS.map((escenario) => (
              <button
                key={escenario.nombre}
                type="button"
                className="boton"
                title={escenario.detalle}
                disabled={ocupado !== null}
                onClick={() =>
                  accion(
                    { tipo: 'escenario', nombre: escenario.nombre },
                    `escenario ${escenario.titulo}`,
                  )
                }
              >
                {ocupado === `escenario ${escenario.titulo}`
                  ? 'Ejecutando...'
                  : escenario.titulo}
              </button>
            ))}
          </div>
          <div className="separador" />
          <ul className="lista-limpia">
            {ESCENARIOS.map((escenario) => (
              <li key={`detalle-${escenario.nombre}`}>
                <strong style={{ fontSize: 13 }}>{escenario.titulo}</strong>
                <div className="mudo">{escenario.detalle}</div>
              </li>
            ))}
          </ul>
        </section>
      </div>

      <section className="panel ajustado" style={{ marginBottom: 18 }}>
        <div className="entre" style={{ padding: '16px 18px' }}>
          <h2 style={{ margin: 0 }}>Servicios</h2>
          <span className="mudo">
            Operacion real sobre los contenedores de Docker Compose
          </span>
        </div>
        {datos?.serviciosError ? (
          <div style={{ padding: 18 }}>
            <div className="aviso error">{datos.serviciosError}</div>
          </div>
        ) : (
          <div className="tabla-envoltura">
            <table>
              <thead>
                <tr>
                  <th>Servicio</th>
                  <th>Estado</th>
                  <th>Detalle</th>
                  <th>Acciones</th>
                </tr>
              </thead>
              <tbody>
                {servicios.map((servicio) => (
                  <tr key={servicio.servicio}>
                    <td className="codigo">{servicio.servicio}</td>
                    <td>
                      <span className={`badge ${colorServicio(servicio.estado)}`}>
                        {servicio.estado}
                      </span>
                    </td>
                    <td className="mudo">{servicio.estadoDetallado}</td>
                    <td>
                      {OPERABLES.has(servicio.servicio) ? (
                        <div className="acciones">
                          {ACCIONES_SERVICIO.map(([accionServicio, etiqueta]) => (
                            <button
                              key={accionServicio}
                              type="button"
                              className={`boton chico ${
                                accionServicio === 'stop' ? 'peligro' : ''
                              }`}
                              disabled={ocupado !== null}
                              onClick={() =>
                                accion(
                                  {
                                    tipo: 'servicio',
                                    servicio: servicio.servicio,
                                    accion: accionServicio,
                                  },
                                  `${etiqueta} ${servicio.servicio}`,
                                )
                              }
                            >
                              {etiqueta}
                            </button>
                          ))}
                        </div>
                      ) : (
                        <span className="mudo">Infraestructura</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <div className="grid grid-2" style={{ marginBottom: 18 }}>
        <section className="panel">
          <h2>Lag por grupo de consumidores</h2>
          {datos?.lagError ? (
            <div className="aviso error">{datos.lagError}</div>
          ) : grupos.length === 0 ? (
            <p className="mudo">
              Sin datos de lag. Los grupos aparecen cuando sus consumidores se
              conectan por primera vez.
            </p>
          ) : (
            <div className="tabla-envoltura">
              <table>
                <thead>
                  <tr>
                    <th>Grupo</th>
                    <th>Topic</th>
                    <th>Lag</th>
                    <th>Particiones (comprometido / maximo)</th>
                  </tr>
                </thead>
                <tbody>
                  {grupos.flatMap((grupo) => {
                    if ((grupo.topics ?? []).length === 0) {
                      return [
                        <tr key={`${grupo.groupId}-vacio`}>
                          <td className="codigo">{grupo.groupId}</td>
                          <td colSpan={3} className="mudo">
                            {grupo.error ?? 'Sin offsets comprometidos'}
                          </td>
                        </tr>,
                      ];
                    }
                    return grupo.topics.map((topic, indice) => (
                      <tr key={`${grupo.groupId}-${topic.topic}`}>
                        <td className="codigo">
                          {indice === 0 ? grupo.groupId : ''}
                          {indice === 0 ? (
                            <div>
                              <span
                                className={`badge ${
                                  grupo.lagTotal > 0 ? 'warn' : 'ok'
                                }`}
                              >
                                total {grupo.lagTotal}
                              </span>
                            </div>
                          ) : null}
                        </td>
                        <td className="mono">{topic.topic}</td>
                        <td
                          className={
                            topic.lag > 0 ? 'texto-alerta' : 'texto-ok'
                          }
                        >
                          {topic.lag}
                        </td>
                        <td className="mudo mono">
                          {topic.particiones
                            .map(
                              (p) =>
                                `P${p.particion}: ${p.comprometido}/${p.maximo}`,
                            )
                            .join(' · ')}
                        </td>
                      </tr>
                    ));
                  })}
                </tbody>
              </table>
            </div>
          )}
        </section>

        <section className="panel">
          <h2>Reconstruccion y reprocesamiento</h2>
          <p className="mudo" style={{ marginTop: 0 }}>
            Detiene el servicio, limpia su vista y reinicia los offsets del
            grupo para reprocesar los topics desde el inicio.
          </p>
          <div className="acciones">
            <button
              type="button"
              className="boton"
              disabled={ocupado !== null}
              onClick={() =>
                accion({ tipo: 'reconstruir' }, 'reconstruir proyeccion')
              }
            >
              {ocupado === 'reconstruir proyeccion'
                ? 'Reconstruyendo...'
                : 'Reconstruir proyeccion de envios'}
            </button>
            <button
              type="button"
              className="boton"
              disabled={ocupado !== null}
              onClick={() => accion({ tipo: 'replay' }, 'replay de analitica')}
            >
              {ocupado === 'replay de analitica'
                ? 'Reprocesando...'
                : 'Reprocesar analitica (replay)'}
            </button>
          </div>
          <div className="separador" />
          <div className="aviso">
            <strong>Recuperacion desde el ultimo offset.</strong>
            <div style={{ marginTop: 6 }}>
              Detener un servicio (por ejemplo el proyector) y volver a
              arrancarlo no pierde eventos: el grupo retoma desde el ultimo
              offset comprometido. La reconstruccion completa borra la vista y
              vuelve al inicio del topic.
            </div>
          </div>
        </section>
      </div>

      <section className="panel ajustado">
        <div className="entre" style={{ padding: '16px 18px' }}>
          <h2 style={{ margin: 0 }}>Dead Letter Queue</h2>
          <span className="mudo">
            Ultimos {dlq.length} eventos invalidos derivados por el proyector
          </span>
        </div>
        {dlq.length === 0 ? (
          <div className="centrado mudo">
            La DLQ esta vacia. Usa el escenario &quot;Evento invalido&quot; para
            poblarla.
          </div>
        ) : (
          <div className="tabla-envoltura">
            <table>
              <thead>
                <tr>
                  <th>Detectado</th>
                  <th>Envio</th>
                  <th>Motivo</th>
                  <th>Ubicacion</th>
                  <th>Payload</th>
                </tr>
              </thead>
              <tbody>
                {dlq.map((registro) => (
                  <tr key={registro._id}>
                    <td className="mudo">{fechaHora(registro.detectadoEn)}</td>
                    <td className="codigo">{registro.shipmentId ?? '—'}</td>
                    <td>{registro.motivo}</td>
                    <td className="mono mudo">
                      {registro.topic} P{registro.partition} O
                      {registro.offset}
                    </td>
                    <td className="mono" style={{ maxWidth: 360 }}>
                      <details>
                        <summary className="mudo">ver payload</summary>
                        <div style={{ marginTop: 6, wordBreak: 'break-all' }}>
                          {registro.payload}
                        </div>
                      </details>
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
