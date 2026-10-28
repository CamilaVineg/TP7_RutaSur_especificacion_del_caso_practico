const NIVELES = { debug: 10, info: 20, warn: 30, error: 40 };

function nivelConfigurado() {
  const nivel = (process.env.LOG_LEVEL || 'info').toLowerCase();
  return NIVELES[nivel] ?? NIVELES.info;
}

export function createLogger(servicio) {
  const minimo = nivelConfigurado();

  const escribir = (nivel, mensaje, contexto = {}) => {
    if (NIVELES[nivel] < minimo) return;
    const linea = {
      ts: new Date().toISOString(),
      nivel,
      servicio,
      mensaje,
      ...contexto,
    };
    process.stdout.write(`${JSON.stringify(linea)}\n`);
  };

  return {
    debug: (mensaje, contexto) => escribir('debug', mensaje, contexto),
    info: (mensaje, contexto) => escribir('info', mensaje, contexto),
    warn: (mensaje, contexto) => escribir('warn', mensaje, contexto),
    error: (mensaje, contexto) => escribir('error', mensaje, contexto),
  };
}

export function dormir(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
