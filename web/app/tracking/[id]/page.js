import { getTracking } from '@/lib/queries';
import TrackingLive from '@/components/TrackingLive';
import Aviso from '@/components/Aviso';

export const dynamic = 'force-dynamic';

export default async function TrackingPage({ params }) {
  const shipmentId = decodeURIComponent(params.id);

  try {
    const inicial = await getTracking(shipmentId);
    return <TrackingLive shipmentId={shipmentId} inicial={inicial} />;
  } catch (error) {
    return (
      <>
        <h1>Seguimiento del envio</h1>
        <Aviso
          tipo="error"
          titulo={`No se pudo leer el envio ${shipmentId}`}
          detalle={`${error.message}. Verifica que MongoDB y el proyector esten corriendo.`}
        />
      </>
    );
  }
}
