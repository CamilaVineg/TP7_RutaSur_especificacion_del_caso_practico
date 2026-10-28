export default function Aviso({ titulo = 'Sin datos', detalle, tipo = '' }) {
  return (
    <div className={`aviso ${tipo}`}>
      <strong>{titulo}</strong>
      {detalle ? <div style={{ marginTop: 6 }}>{detalle}</div> : null}
    </div>
  );
}
