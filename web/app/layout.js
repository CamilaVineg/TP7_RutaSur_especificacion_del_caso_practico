import './globals.css';
import Link from 'next/link';

export const metadata = {
  title: 'RutaSur | Torre de control',
  description:
    'Plataforma de seguimiento de envios en tiempo real orientada a eventos.',
};

const NAVEGACION = [
  { href: '/torre', etiqueta: 'Torre de control' },
  { href: '/analitica', etiqueta: 'Analitica' },
  { href: '/demo', etiqueta: 'Panel de demo' },
];

export default function RootLayout({ children }) {
  return (
    <html lang="es">
      <body>
        <header className="topbar">
          <div className="topbar-interno">
            <Link className="marca" href="/torre">
              <span className="marca-logo">RS</span>
              <span>
                RutaSur
                <small>Operacion en tiempo real</small>
              </span>
            </Link>
            <nav className="nav">
              {NAVEGACION.map((item) => (
                <Link key={item.href} href={item.href}>
                  {item.etiqueta}
                </Link>
              ))}
            </nav>
          </div>
        </header>
        <main className="contenedor">{children}</main>
        <footer className="pie">
          TP7 Programacion Avanzada - Kafka, MongoDB y Next.js
        </footer>
      </body>
    </html>
  );
}
