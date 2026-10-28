import { getDemo } from '@/lib/queries';
import DemoPanel from '@/components/DemoPanel';
import Aviso from '@/components/Aviso';

export const dynamic = 'force-dynamic';

export default async function DemoPage() {
  try {
    const inicial = await getDemo();
    return <DemoPanel inicial={inicial} />;
  } catch (error) {
    return (
      <>
        <h1>Panel de demo</h1>
        <Aviso
          tipo="error"
          titulo="No se pudo cargar el panel de demo"
          detalle={`${error.message}. Verifica que MongoDB, el simulador y el servicio de control esten corriendo.`}
        />
      </>
    );
  }
}
