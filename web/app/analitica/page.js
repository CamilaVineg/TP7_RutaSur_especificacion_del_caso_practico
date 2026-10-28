import Link from 'next/link';

import { getAnalitica } from '@/lib/queries';
import Aviso from '@/components/Aviso';
import {
  ETIQUETAS_FRANJA,
  ETIQUETAS_MOTIVO,
  formatearMinutos,
  formatearPorcentaje,
  soloFecha,
} from '@/lib/format';

export const dynamic = 'force-dynamic';

function valor(parametro) {
  if (Array.isArray(parametro)) return parametro[0] ?? '';
  return parametro ?? '';
}

export default async function AnaliticaPage({ searchParams }) {
  const filtros = {
    zona: valor(searchParams?.zona),
    hub: valor(searchParams?.hub),
    desde: valor(searchParams?.desde),
    hasta: valor(searchParams?.hasta),
  };

  let datos = null;
  let error = null;
  try {
    datos = await getAnalitica(filtros);
  } catch (err) {
    error = err.message;
  }

  return (
    <>
      <div className="encabezado">
        <div>
          <h1>Analitica operativa</h1>
          <p className="subtitulo">
            Tres indicadores calculados por el servicio de analitica y un cuarto
            indicador reconstruido por el grupo de replay. La pagina consulta
            las vistas al cargar.
          </p>
        </div>
        <Link className="boton" href="/demo">
          Ir al panel de reprocesamiento
        </Link>
      </div>

      <form className="panel filtros" method="get" action="/analitica">
        <label className="campo">
          Zona
          <select name="zona" defaultValue={filtros.zona}>
            <option value="">Todas</option>
            {(datos?.catalogo?.zonas ?? []).map((zona) => (
              <option key={zona} value={zona}>
                {zona}
              </option>
            ))}
          </select>
        </label>
        <label className="campo">
          Hub
          <select name="hub" defaultValue={filtros.hub}>
            <option value="">Todos</option>
            {(datos?.catalogo?.hubs ?? []).map((hub) => (
              <option key={hub} value={hub}>
                {hub}
              </option>
            ))}
          </select>
        </label>
        <label className="campo">
          Desde
          <input type="date" name="desde" defaultValue={filtros.desde} />
        </label>
        <label className="campo">
          Hasta
          <input type="date" name="hasta" defaultValue={filtros.hasta} />
        </label>
        <button className="boton primario" type="submit">
          Aplicar filtros
        </button>
        <Link className="boton" href="/analitica">
          Limpiar
        </Link>
      </form>

      <div style={{ height: 18 }} />

      {error ? (
        <Aviso
          tipo="error"
          titulo="No se pudo calcular la analitica"
          detalle={`${error}. Verifica que MongoDB y el servicio de analitica esten corriendo.`}
        />
      ) : null}

      {datos && datos.metricas === 0 ? (
        <Aviso
          titulo="Todavia no hay metricas"
          detalle="El servicio de analitica escribe una metrica por evento de estado. Inicia el simulador o dispara un escenario para generar datos."
        />
      ) : null}

      {datos && datos.metricas > 0 ? (
        <>
          <section className="grid grid-3" style={{ marginBottom: 18 }}>
            <div className="kpi">
              <div className="kpi-valor">{datos.total.creados}</div>
              <div className="kpi-etiqueta">Envios creados</div>
            </div>
            <div className="kpi">
              <div className="kpi-valor texto-ok">{datos.total.entregados}</div>
              <div className="kpi-etiqueta">Entregados</div>
            </div>
            <div className="kpi">
              <div className="kpi-valor texto-malo">{datos.total.fallos}</div>
              <div className="kpi-etiqueta">Entregas fallidas</div>
            </div>
            <div className="kpi">
              <div className="kpi-valor">
                {formatearPorcentaje(datos.total.tasaPrimerIntento)}
              </div>
              <div className="kpi-etiqueta">Tasa de primer intento</div>
            </div>
            <div className="kpi">
              <div className="kpi-valor">
                {formatearPorcentaje(datos.total.tasaSla)}
              </div>
              <div className="kpi-etiqueta">Cumplimiento de promesa</div>
            </div>
            <div className="kpi">
              <div className="kpi-valor" style={{ fontSize: 23 }}>
                {formatearMinutos(
                  datos.tiemposPorHub.length > 0
                    ? datos.tiemposPorHub.reduce(
                        (acc, h) => acc + (h.promedioMinutos ?? 0),
                        0,
                      ) / datos.tiemposPorHub.length
                    : null,
                )}
              </div>
              <div className="kpi-etiqueta">Promedio hub a reparto</div>
            </div>
          </section>

          <div className="grid grid-2" style={{ marginBottom: 18 }}>
            <section className="panel ajustado">
              <div className="entre" style={{ padding: '16px 18px' }}>
                <h2 style={{ margin: 0 }}>
                  Indicador 1 · Exito al primer intento por zona
                </h2>
                <span className="mudo">Zonas ordenadas por volumen</span>
              </div>
              <div className="tabla-envoltura">
                <table>
                  <thead>
                    <tr>
                      <th>Zona</th>
                      <th>Entregados</th>
                      <th>Fallos</th>
                      <th>Tasa 1er intento</th>
                      <th>Cumplimiento SLA</th>
                    </tr>
                  </thead>
                  <tbody>
                    {[...datos.porZona]
                      .sort((a, b) => b.creados - a.creados)
                      .map((zona) => (
                        <tr key={zona.zonaId}>
                          <td className="codigo">{zona.zonaId}</td>
                          <td>{zona.entregados}</td>
                          <td>{zona.fallos}</td>
                          <td>
                            <strong>
                              {formatearPorcentaje(zona.tasaPrimerIntento)}
                            </strong>
                            <div className="barra" style={{ marginTop: 5 }}>
                              <span
                                style={{
                                  width: `${zona.tasaPrimerIntento ?? 0}%`,
                                }}
                              />
                            </div>
                          </td>
                          <td>{formatearPorcentaje(zona.tasaSla)}</td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </div>
            </section>

            <section className="panel ajustado">
              <div className="entre" style={{ padding: '16px 18px' }}>
                <h2 style={{ margin: 0 }}>
                  Indicador 2 · Comportamiento por franja horaria
                </h2>
                <span className="mudo">Hora del evento (UTC)</span>
              </div>
              <div className="tabla-envoltura">
                <table>
                  <thead>
                    <tr>
                      <th>Franja</th>
                      <th>Creados</th>
                      <th>Entregados</th>
                      <th>Fallos</th>
                      <th>Tasa 1er intento</th>
                    </tr>
                  </thead>
                  <tbody>
                    {[...datos.porFranja]
                      .sort((a, b) => (b.creados ?? 0) - (a.creados ?? 0))
                      .map((franja) => (
                        <tr key={franja.franja}>
                          <td>
                            {ETIQUETAS_FRANJA[franja.franja] ?? franja.franja}
                          </td>
                          <td>{franja.creados}</td>
                          <td>{franja.entregados}</td>
                          <td>{franja.fallos}</td>
                          <td className="texto-ok">
                            {formatearPorcentaje(franja.tasaPrimerIntento)}
                          </td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </div>
            </section>
          </div>

          <div className="grid grid-2" style={{ marginBottom: 18 }}>
            <section className="panel">
              <h2>Indicador 3 · Motivos de fallo</h2>
              {datos.total.motivos.length === 0 ? (
                <p className="mudo">No se registraron fallos en el periodo.</p>
              ) : (
                <>
                  <table>
                    <thead>
                      <tr>
                        <th>Motivo</th>
                        <th>Cantidad</th>
                        <th>Participacion</th>
                      </tr>
                    </thead>
                    <tbody>
                      {datos.total.motivos.map((motivo) => {
                        const participacion = datos.total.totalFallos
                          ? (motivo.cantidad / datos.total.totalFallos) * 100
                          : 0;
                        return (
                          <tr key={motivo.motivo}>
                            <td>
                              {ETIQUETAS_MOTIVO[motivo.motivo] ?? motivo.motivo}
                            </td>
                            <td>{motivo.cantidad}</td>
                            <td>
                              <div className="entre">
                                <span>{formatearPorcentaje(participacion)}</span>
                              </div>
                              <div className="barra" style={{ marginTop: 5 }}>
                                <span style={{ width: `${participacion}%` }} />
                              </div>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                  <p className="mudo" style={{ marginBottom: 0, marginTop: 12 }}>
                    Total de fallos: {datos.total.totalFallos}. La distribucion
                    objetivo del simulador es 60% ausente, 20% direccion
                    incorrecta, 15% rechazado y 5% zona inaccesible.
                  </p>
                </>
              )}
            </section>

            <section className="panel ajustado">
              <div style={{ padding: '16px 18px 10px' }}>
                <h2 style={{ margin: 0 }}>Indicador 3 · Zona por franja</h2>
                <p className="mudo" style={{ margin: '6px 0 0' }}>
                  Cruce para detectar franjas criticas por zona.
                </p>
              </div>
              <div className="tabla-envoltura">
                <table>
                  <thead>
                    <tr>
                      <th>Zona</th>
                      <th>Franja</th>
                      <th>Entregados</th>
                      <th>Fallos</th>
                      <th>Tasa 1er intento</th>
                    </tr>
                  </thead>
                  <tbody>
                    {[...datos.porZonaFranja]
                      .sort(
                        (a, b) =>
                          a.zonaId.localeCompare(b.zonaId) ||
                          a.franja.localeCompare(b.franja),
                      )
                      .map((fila) => (
                        <tr key={`${fila.zonaId}-${fila.franja}`}>
                          <td className="codigo">{fila.zonaId}</td>
                          <td>
                            {ETIQUETAS_FRANJA[fila.franja] ?? fila.franja}
                          </td>
                          <td>{fila.entregados}</td>
                          <td>{fila.fallos}</td>
                          <td>{formatearPorcentaje(fila.tasaPrimerIntento)}</td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </div>
            </section>
          </div>

          <div className="grid grid-2" style={{ marginBottom: 18 }}>
            <section className="panel">
              <h2>Indicador 4 · Tiempo medio de hub a reparto</h2>
              <p className="mudo" style={{ marginTop: 0 }}>
                Reconstruido por el grupo <span className="codigo">analitica-replay</span>{' '}
                leyendo <span className="codigo">envios.estados</span> desde el
                inicio. Se puede reprocesar desde el panel de demo.
              </p>
              {datos.tiemposPorHub.length === 0 ? (
                <p className="mudo">
                  Todavia no hay pares INGRESADO_HUB / EN_REPARTO suficientes.
                </p>
              ) : (
                <table>
                  <thead>
                    <tr>
                      <th>Hub</th>
                      <th>Envios medidos</th>
                      <th>Tiempo promedio</th>
                    </tr>
                  </thead>
                  <tbody>
                    {datos.tiemposPorHub.map((hub) => (
                      <tr key={hub.hubId}>
                        <td className="codigo">{hub.hubId}</td>
                        <td>{hub.cantidad}</td>
                        <td>{formatearMinutos(hub.promedioMinutos)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </section>

            <section className="panel ajustado">
              <div style={{ padding: '16px 18px 10px' }}>
                <h2 style={{ margin: 0 }}>Cumplimiento de SLA por hub y dia</h2>
                <p className="mudo" style={{ margin: '6px 0 0' }}>
                  Entregas dentro de la promesa sobre entregas con promesa.
                </p>
              </div>
              <div className="tabla-envoltura">
                <table>
                  <thead>
                    <tr>
                      <th>Fecha</th>
                      <th>Hub</th>
                      <th>Entregados</th>
                      <th>Cumplimiento</th>
                    </tr>
                  </thead>
                  <tbody>
                    {datos.slaPorHubDia.slice(0, 20).map((fila) => (
                      <tr key={`${fila.fecha}-${fila.hubId}`}>
                        <td>{soloFecha(fila.fecha)}</td>
                        <td className="codigo">{fila.hubId}</td>
                        <td>{fila.entregados}</td>
                        <td>{formatearPorcentaje(fila.tasaSla)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          </div>

          <section className="panel ajustado">
            <div className="entre" style={{ padding: '16px 18px' }}>
              <h2 style={{ margin: 0 }}>Evolucion diaria</h2>
              <span className="mudo">
                {datos.metricas} documentos de metrica considerados
              </span>
            </div>
            <div className="tabla-envoltura">
              <table>
                <thead>
                  <tr>
                    <th>Fecha</th>
                    <th>Creados</th>
                    <th>Entregados</th>
                    <th>Fallos</th>
                    <th>Tasa 1er intento</th>
                    <th>Cumplimiento SLA</th>
                  </tr>
                </thead>
                <tbody>
                  {datos.porFecha.slice(0, 20).map((fila) => (
                    <tr key={fila.fecha}>
                      <td>{soloFecha(fila.fecha)}</td>
                      <td>{fila.creados}</td>
                      <td>{fila.entregados}</td>
                      <td>{fila.fallos}</td>
                      <td>{formatearPorcentaje(fila.tasaPrimerIntento)}</td>
                      <td>{formatearPorcentaje(fila.tasaSla)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </>
      ) : null}
    </>
  );
}
