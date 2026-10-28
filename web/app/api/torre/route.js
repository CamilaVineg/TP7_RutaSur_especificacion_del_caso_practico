import { NextResponse } from 'next/server';
import { getTorre } from '@/lib/queries';

export const dynamic = 'force-dynamic';

export async function GET() {
  try {
    const datos = await getTorre();
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
