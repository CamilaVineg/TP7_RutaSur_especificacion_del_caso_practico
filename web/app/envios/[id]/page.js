import { getEnvio } from '@/lib/queries';
import EnvioDetalle from '@/components/EnvioDetalle';
import Aviso from '@/components/Aviso';

export const dynamic = 'force-dynamic';

export default async function EnvioPage({ params }) {
  const shipmentId = decodeURIComponent(params.id);

  try {
    const inicial = await getEnvio(shipmentId);
    return <EnvioDetalle shipmentId={shipmentId} inicial={inicial} />;
  } catch (error) {
    return (
      <>
        <h1>Detalle del envio</h1>
        <Aviso
          tipo="error"
          titulo={`No se pudo leer el envio ${shipmentId}`}
          detalle={`${error.message}. Verifica que MongoDB y el proyector esten corriendo.`}
        />
      </>
    );
  }
}
