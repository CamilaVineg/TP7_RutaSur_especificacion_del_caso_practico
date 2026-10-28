import { NextResponse } from 'next/server';
import { getEnvio } from '@/lib/queries';

export const dynamic = 'force-dynamic';

export async function GET(_request, { params }) {
  try {
    const datos = await getEnvio(params.id);
    if (!datos.envio) {
      return NextResponse.json(
        { error: 'Envio no encontrado', shipmentId: params.id },
        { status: 404 },
      );
    }
    return NextResponse.json(datos, {
      headers: { 'Cache-Control': 'no-store' },
    });
  } catch (error) {
    return NextResponse.json(
      { error: error.message },
      { status: 500, headers: { 'Cache-Control': 'no-store' } },
    );
  }
}
