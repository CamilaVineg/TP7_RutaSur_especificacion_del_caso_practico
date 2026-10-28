// Reloj simulado compartido.
// El simulador guarda un ancla (tiempo simulado + tiempo real). Cualquier
// servicio deriva el "ahora" simulado de esa ancla, de modo que el monitor de
// SLA usa el reloj que viaja en los eventos y no el reloj de la maquina.

export const COLECCION_RELOJ = 'reloj';
export const DOC_RELOJ = 'global';

export async function inicializarReloj(db, factor) {
  const ahora = Date.now();
  await db.collection(COLECCION_RELOJ).updateOne(
    { _id: DOC_RELOJ },
    {
      $setOnInsert: { anclaSim: ahora, anclaReal: ahora, factor },
    },
    { upsert: true },
  );
  await db
    .collection(COLECCION_RELOJ)
    .updateOne({ _id: DOC_RELOJ }, { $set: { factor } });
  return db.collection(COLECCION_RELOJ).findOne({ _id: DOC_RELOJ });
}

export async function reiniciarReloj(db, factor) {
  const ahora = Date.now();
  await db.collection(COLECCION_RELOJ).updateOne(
    { _id: DOC_RELOJ },
    { $set: { anclaSim: ahora, anclaReal: ahora, factor } },
    { upsert: true },
  );
  return db.collection(COLECCION_RELOJ).findOne({ _id: DOC_RELOJ });
}

export async function simAhora(db, factorPorDefecto = 1) {
  const doc = await db
    .collection(COLECCION_RELOJ)
    .findOne({ _id: DOC_RELOJ });

  if (!doc || !Number.isFinite(Number(doc.anclaSim))) {
    return { ahora: new Date(), factor: factorPorDefecto, simulado: false };
  }

  const factor = Number(doc.factor) || factorPorDefecto;
  const ms =
    Number(doc.anclaSim) + (Date.now() - Number(doc.anclaReal)) * factor;

  return { ahora: new Date(ms), factor, simulado: true };
}
