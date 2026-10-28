import { getTorre } from '@/lib/queries';
import TorreLive from '@/components/TorreLive';
import Aviso from '@/components/Aviso';

export const dynamic = 'force-dynamic';

export default async function TorrePage() {
  try {
    const inicial = await getTorre();
    return <TorreLive inicial={inicial} />;
  } catch (error) {
    return (
      <>
        <h1>Torre de control</h1>
        <Aviso
          tipo="error"
          titulo="No se pudo leer la base de proyeccion"
          detalle={`${error.message}. Verifica que MongoDB y el proyector esten corriendo (docker compose ps).`}
        />
      </>
    );
  }
}
