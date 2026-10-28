'use client';

import { useEffect, useRef, useState } from 'react';

/**
 * Mantiene una vista sincronizada con MongoDB.
 * - Abre el stream SSE (/api/stream) que publica cambios de colecciones.
 * - Refresca al recibir un cambio, con debounce para no saturar la API.
 * - Tiene un polling de respaldo por si el stream se corta.
 */
export function useLive(refrescar, opciones = {}) {
  const { intervaloMs = 5000, debounceMs = 350 } = opciones;
  const [conectado, setConectado] = useState(false);
  const [ultimaActualizacion, setUltimaActualizacion] = useState(null);

  const refrescarRef = useRef(refrescar);
  refrescarRef.current = refrescar;

  useEffect(() => {
    let vivo = true;
    let temporizador = null;
    let enVuelo = false;
    let pendiente = false;

    const ejecutar = async () => {
      if (!vivo || !refrescarRef.current) return;
      if (enVuelo) {
        pendiente = true;
        return;
      }
      enVuelo = true;
      try {
        await refrescarRef.current();
        if (vivo) setUltimaActualizacion(new Date().toISOString());
      } finally {
        enVuelo = false;
        if (pendiente && vivo) {
          pendiente = false;
          programar(0);
        }
      }
    };

    const programar = (ms = debounceMs) => {
      if (!vivo) return;
      if (temporizador) clearTimeout(temporizador);
      temporizador = setTimeout(() => {
        temporizador = null;
        ejecutar();
      }, ms);
    };

    const fuente = new EventSource('/api/stream');
    fuente.addEventListener('open', () => setConectado(true));
    fuente.addEventListener('ping', () => setConectado(true));
    fuente.addEventListener('cambio', () => programar());
    fuente.addEventListener('error', () => setConectado(false));

    const intervalo = setInterval(() => programar(0), intervaloMs);

    programar(0);

    return () => {
      vivo = false;
      fuente.close();
      clearInterval(intervalo);
      if (temporizador) clearTimeout(temporizador);
    };
  }, [intervaloMs, debounceMs]);

  return { conectado, ultimaActualizacion };
}

export function IndicadorConexion({ conectado, ultimaActualizacion }) {
  return (
    <span className="pildora">
      <span className={`punto ${conectado ? 'vivo' : 'muerto'}`} />
      {conectado ? 'En vivo' : 'Reconectando'}
      {ultimaActualizacion ? (
        <span className="mudo">
          · {ultimaActualizacion.slice(11, 19)} UTC
        </span>
      ) : null}
    </span>
  );
}
