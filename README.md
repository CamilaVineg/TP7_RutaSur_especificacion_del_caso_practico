# RutaSur — seguimiento de envíos en tiempo real orientado a eventos

TP7 de Programación Avanzada. Plataforma local que simula la operación logística de
RutaSur: los eventos de negocio viajan por **Apache Kafka**, un **proyector** los
materializa en **MongoDB** como modelo de lectura y una interfaz **Next.js** muestra la
torre de control, el detalle de cada envío, el tracking público y la analítica.

Todo corre con Docker Compose y se levanta con **un solo comando**.

```
Kafka es el único punto de contacto entre servicios.
Ningún servicio llama por HTTP a otro servicio de negocio.
```

---

## 1. Arranque rápido

Requisitos: Docker Desktop en ejecución (Docker 24+ y Compose v2).

```bash
docker compose up --build -d
```

o, con el script del proyecto:

```bash
npm run up
```

La primera vez tarda unos minutos (imágenes de Kafka, Mongo, Node y build de Next.js).
Cuando los contenedores están arriba:

| Recurso | URL |
| --- | --- |
| Torre de control | http://localhost:3000/torre |
| Detalle de envío | http://localhost:3000/envios/RS-XXXXXXXX |
| Tracking público | http://localhost:3000/tracking/RS-XXXXXXXX |
| Analítica | http://localhost:3000/analitica |
| Panel de demo | http://localhost:3000/demo |
| API del simulador | http://localhost:4000 |
| API del control | http://localhost:4100 |
| MongoDB (host) | `mongodb://localhost:27018/rutasur?replicaSet=rs0` |
| Kafka (host) | `localhost:29092` |

Verificación rápida:

```bash
docker compose ps
curl http://localhost:4000/health
curl http://localhost:4100/health
```

Para bajar todo (incluidos volúmenes de Kafka y Mongo):

```bash
npm run down
```

---

## 2. Arquitectura

```
                        ┌──────────────────────────┐
                        │        Simulador         │
                        │  20 envíos/min · semilla │
                        │  reloj simulado (120x)   │
                        └────────────┬─────────────┘
                                     │ publica
        ┌────────────────────────────┼────────────────────────────────┐
        │                            ▼                                │
        │   ┌─────────────────────────────────────────────────────┐   │
        │   │                     Apache Kafka                    │   │
        │   │ envios.estados · envios.promesas                    │   │
        │   │ envios.reprogramaciones · alertas.sla               │   │
        │   │ notificaciones.enviadas · envios.estados.dlq        │   │
        │   └───┬───────────────┬───────────────┬─────────────┬───┘   │
        │       │               │               │             │       │
        │  grupo│proyector  grupo│notificaciones │grupo        │grupo  │
        │       ▼               ▼               ▼             ▼       │
        │  ┌─────────┐   ┌──────────────┐  ┌──────────┐  ┌─────────┐  │
        │  │Proyector│   │Notificaciones│  │Reprogram.│  │Monitor  │  │
        │  │  (CU1)  │   │    (CU2)     │  │  (CU2)   │  │ SLA(CU3)│  │
        │  └────┬────┘   └──────┬───────┘  └────┬─────┘  └────┬────┘  │
        │       │               │               │             │       │
        │       │ escribe       │ publica       │ publica     │ publica
        │       ▼               ▼               ▼             ▼       │
        │   ┌────────────────────────── MongoDB ─────────────────────┐ │
        │   │ envios · alertas · notificaciones · metricas_diarias   │ │
        │   │ metricas_hub · dlq · reloj · colecciones de estado     │ │
        │   └────────────────────────────┬───────────────────────────┘ │
        │                                │ change streams             │
        │  ┌──────────────┐   ┌──────────▼──────────┐                  │
        │  │  Analítica   │   │   Next.js (web)     │                  │
        │  │analitica(CU4)│   │ SSE /api/stream     │                  │
        │  │analitica-    │   │ torre · detalle ·   │                  │
        │  │replay (CU4)  │   │ tracking · demo     │                  │
        │  └──────────────┘   └─────────────────────┘                  │
        └──────────────────────────────────────────────────────────────┘
```

### Por qué hay tres "fuentes de escritura"

- **Simulador**: publica el hecho de negocio (`envios.estados`, `envios.promesas`).
- **Servicios de reacción**: `notificaciones`, `reprogramacion` y `monitor-sla`
  consumen esos hechos y publican **sus propios** eventos en otros topics.
- **Proyector**: único servicio que escribe las vistas de lectura (`envios`, `alertas`,
  `notificaciones`, `dlq`). Si mañana cambia el modelo de lectura, se reconstruye sin
  tocar a los demás.

Cada servicio tiene su propio grupo de consumidores y avanza a su ritmo: detener uno no
frena a los demás.

---

## 3. Topics

| Topic | Particiones | Retención | Política | Productor | Consumidores |
| --- | --- | --- | --- | --- | --- |
| `envios.estados` | 3 | 30 días | delete | simulador | proyector, notificaciones, reprogramacion, monitor-sla, analitica, analitica-replay |
| `envios.promesas` | 3 | — | **compact** | simulador | monitor-sla |
| `envios.reprogramaciones` | 3 | 30 días | delete | reprogramacion | proyector |
| `alertas.sla` | 3 | 7 días | delete | monitor-sla | proyector, notificaciones |
| `notificaciones.enviadas` | 3 | 7 días | delete | notificaciones | proyector |
| `envios.estados.dlq` | 3 | 30 días | delete | proyector | (revisión manual desde la UI) |

Los topics se crean en el arranque con `kafka-init`, con `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`
para que no aparezcan topics accidentales.

### Grupos de consumidores

`proyector`, `notificaciones`, `reprogramacion`, `monitor-sla`, `analitica` y
`analitica-replay`. Son independientes: el lag de un grupo no afecta a los otros.

### Eventos

Todos los eventos comparten sobre: `eventId` (UUID), `version`, `ocurridoEn` (ISO-8601
del **reloj simulado**) y `shipmentId` (clave del mensaje en Kafka, garantiza orden por
envío dentro de una partición).

- **Estado** (`envios.estados`): `tipo` (`CREADO`, `INGRESADO_HUB`, `CLASIFICADO`,
  `EN_REPARTO`, `ENTREGADO`, `ENTREGA_FALLIDA`), `secuencia`, `zonaId`, `hubId`,
  `repartidorId`, `intento`, `motivo`, `promesaVenceEn`.
- **Promesa** (`envios.promesas`): `venceEn`, `zonaId`, `hubId`. Compactado por envío.
- **Reprogramación** (`envios.reprogramaciones`): `motivo`, `intento`, `decision`
  (`REPROGRAMADO` / `RETENIDO` / `DEVOLUCION`), `franjaFallo`, `proximoIntentoEn`.
- **Alerta SLA** (`alertas.sla`): `causa` (`SIN_INGRESO`, `SIN_SALIDA`,
  `FALLO_DIA_LIMITE`, `VENCIDO`), `prioridad` (`MEDIA`/`ALTA`/`CRITICA`), `abierta`.
- **Notificación** (`notificaciones.enviadas`): `canal`, `tipo`, `mensaje`, `opciones`,
  `origenEventId`.

Los cinco esquemas se validan con **zod** en `services/shared/events.js`. Un mensaje que
no cumple el contrato no rompe el consumidor: se deriva a la DLQ (o se descarta con log,
según el servicio).

---

## 4. Servicios

| Servicio | Responsabilidad |
| --- | --- |
| `simulador` | Genera tráfico, reloj simulado, escenarios y API HTTP (`:4000`). |
| `proyector` | CU1: proyecta estados y reprogramaciones, escribe vistas y centraliza la DLQ. |
| `notificaciones` | CU2: reacciona a entregas fallidas y alertas de SLA publicando notificaciones. |
| `reprogramacion` | CU2: decide reprogramación, retención o devolución y publica el evento. |
| `monitor-sla` | CU3: evalúa 4 reglas de SLA con el reloj simulado, por evento y periódicamente. |
| `analitica` | CU4: indicadores 1–3 sobre `metricas_diarias`. |
| `analitica-replay` | CU4: 4.º indicador (hub → reparto) con grupo propio y lectura desde el inicio. |
| `control` | Opera contenedores (stop/start/restart), calcula lag, reconstruye vistas y dispara replay. |
| `web` | Next.js: 4 pantallas + panel de demo, con actualización en vivo por SSE. |

### Modelo de lectura (MongoDB)

| Colección | Contenido |
| --- | --- |
| `envios` | Documento por envío: estado actual, historial, reprogramaciones, promesa, terminal. |
| `alertas` | Cola de alertas SLA (una por envío y causa), abierta/cerrada y prioridad. |
| `notificaciones` | Notificaciones emitidas, para mostrarlas en el detalle. |
| `metricas_diarias` | Agregados por fecha, zona y franja horaria (indicadores 1–3). |
| `metricas_hub` | Agregado de minutos hub → reparto por hub y fecha (indicador 4). |
| `dlq` | Copia legible de los eventos derivados a la DLQ, con topic/partición/offset. |
| `reloj` | Ancla del reloj simulado (`anclaSim`, `anclaReal`, `factor`). |
| `sla_envios`, `reprog_envios`, `replay_envios` | Estado interno de cada servicio (no son vistas de la UI). |
| `eventos_procesados` | Idempotencia por consumidor + `eventId`, con TTL de 7 días. |

MongoDB corre como **replica set de un nodo** (`rs0`) porque los change streams —que
alimentan el SSE del frontend— lo requieren.

---

## 5. Casos de uso

### CU1 — Ciclo de vida del envío

1. El simulador publica `CREADO` con `secuencia: 1` y una promesa.
2. Cada transición (`INGRESADO_HUB` → `CLASIFICADO` → `EN_REPARTO` → `ENTREGADO` /
   `ENTREGA_FALLIDA`) incrementa la secuencia.
3. El proyector aplica **tres guardas** antes de escribir:
   - `eventId` ya presente en el historial → duplicado, se descarta.
   - `secuencia <= ultimaSecuencia` → fuera de orden, se descarta con log.
   - El envío ya está `ENTREGADO` → se ignora cualquier evento posterior.
4. `ENTREGADO` marca `terminal: true` y calcula `entregadoPrimerIntento`.
5. Un evento que no valida contra el esquema se publica en `envios.estados.dlq` y se
   registra en la colección `dlq` para revisarlo desde el panel.

### CU2 — Entrega fallida: tres reacciones independientes

Un `ENTREGA_FALLIDA` dispara, en paralelo y sin conocerse entre sí:

| Servicio | Reacción |
| --- | --- |
| `notificaciones` | Publica una notificación con el motivo y opciones (reprogramar, corregir dirección, contactar). |
| `reprogramacion` | Decide `REPROGRAMADO` (día hábil siguiente, franja opuesta), `RETENIDO` (dirección incorrecta) o `DEVOLUCION` (rechazo o tercer intento) y publica `envios.reprogramaciones`. |
| `monitor-sla` | Actualiza su estado y evalúa si corresponde `FALLO_DIA_LIMITE`, y además notifica riesgo a `notificaciones` vía `alertas.sla`. |

El proyector consume `envios.reprogramaciones` y `notificaciones.enviadas` para
enriquecer el detalle del envío. Si un servicio está caído, los otros dos siguen
reaccionando y, al volver, el caído se pone al día desde su último offset.

### CU3 — Riesgo de SLA

El monitor evalúa con el **reloj simulado que viaja en los eventos** (`ocurridoEn`), no
con el reloj de la máquina. Reglas, en orden de prioridad:

| Regla | Condición | Prioridad |
| --- | --- | --- |
| `VENCIDO` | la promesa ya venció y el envío no es terminal | CRITICA |
| `SIN_INGRESO` | estado `CREADO` y faltan ≤ 10 h simuladas | MEDIA |
| `SIN_SALIDA` | estado `INGRESADO_HUB`/`CLASIFICADO` y faltan ≤ 4 h | ALTA |
| `FALLO_DIA_LIMITE` | último estado `ENTREGA_FALLIDA` y vence el mismo día | ALTA |

La evaluación ocurre **por evento** y además **periódicamente** (`SLA_TICK_MS`, 60 s
reales) para los envíos abiertos. Cada regla se emite una sola vez por envío; al
entregarse o salir del circuito, las alertas abiertas se cierran.

### CU4 — Analítica

`analitica` mantiene `metricas_diarias` (fecha, zona, franja) y publica:

1. **Éxito al primer intento por zona**: entregados / fallos / tasa.
2. **Comportamiento por franja horaria**: creados, entregados, fallos y tasa por franja.
3. **Motivos de fallo** con participación, más el cruce zona × franja.

`analitica-replay` corre en un grupo aparte, lee `envios.estados` **desde el inicio** y
construye el 4.º indicador: **tiempo medio de hub a reparto por hub**. Se puede
reprocesar completo desde el panel de demo sin afectar al resto.

La página `/analitica` consulta las vistas al cargar (no se refresca por SSE) y permite
filtrar por zona, hub y rango de fechas.

---

## 6. Simulador

- **20 envíos por minuto**, semilla fija (`SIM_SEED=42`) para que la demo sea
  reproducible.
- **Reloj simulado**: 1 hora simulada = 30 segundos reales (`SIM_SPEEDUP=120`).
  Todos los `ocurridoEn` usan ese reloj y el monitor de SLA evalúa con él.
- Zonas y tasas de fallo: `Z-NORTE` 5 %, `Z-CENTRO` 12 %, `Z-SUR` 20 %, `Z-OESTE` 30 %.
- Distribución de motivos: 60 % `AUSENTE`, 20 % `DIRECCION_INCORRECTA`, 15 %
  `RECHAZADO`, 5 % `ZONA_INACCESIBLE`.
- Anomalías inyectadas: demora en hub 10 %, **duplicado 2 %**, **desorden 2 %**,
  **malformado 1 %**.
- Tres hubs (`H-NORTE`, `H-CENTRO`, `H-SUR`) y cuatro zonas.

### Escenarios (`POST /sim/escenarios/:nombre`)

| Nombre | Qué demuestra |
| --- | --- |
| `feliz` | Ciclo completo hasta `ENTREGADO` sin fallos. |
| `ausente` | Primer intento fallido y reintento exitoso. |
| `demorado` | Se detiene en `CLASIFICADO` y dispara `SIN_INGRESO`/`SIN_SALIDA`. |
| `tercer-fallo` | Tres intentos fallidos → `DEVOLUCION` y ciclo terminal. |
| `rafaga` | 500 envíos de golpe para medir throughput y lag. |
| `invalido` | Publica un evento que no cumple el esquema y cae en la DLQ. |

Los escenarios usan **tiempos comprimidos** (horas simuladas reducidas a segundos) para
que el ciclo completo se vea durante la demo. Es una compresión exclusiva del modo
escenario: el flujo continuo respeta el reloj de 120x. Está indicado en la UI del panel.

También hay endpoints de lectura: `GET /sim/estado`, `GET /sim/resumen`, `GET /health`.

---

## 7. Panel de demo

`/demo` reúne todo lo necesario para la presentación:

- **Reloj simulado** y contadores (creados, publicados, entregados, fallidos,
  duplicados, malformados).
- **Escenarios** con un clic.
- **Servicios reales de Docker Compose** con stop / start / restart.
- **Lag por grupo** y por partición (comprometido / máximo).
- **Reconstrucción de la proyección** y **replay de analítica**.
- **DLQ** con motivo, topic/partición/offset y payload.

### Guion sugerido (5–7 minutos)

1. Abrir `/torre`: mostrar el flujo continuo y el indicador "En vivo".
2. `/demo` → **Camino feliz**: abrir el detalle y ver el historial completo.
3. **Destinatario ausente**: mostrar notificación + reprogramación + reintento.
4. **Demorado en hub**: ir a la torre y ver las alertas SLA (`SIN_INGRESO`, `SIN_SALIDA`).
5. **Tercer fallo**: tres reprogramaciones y `DEVOLUCION` con el ciclo cerrado.
6. **Evento inválido**: verlo aparecer en la DLQ con partición y offset.
7. **Ráfaga de 500**: mirar cómo sube el lag del proyector y vuelve a 0.
8. **Aislamiento**: detener `notificaciones`, disparar `ausente` y comprobar que el
   proyector y la reprogramación siguen funcionando. Volver a arrancarlo.
9. **Recuperación**: detener `proyector`, disparar un escenario (el envío da 404),
   arrancarlo y ver que el lag baja a 0 y el envío se reconstruye.
10. **Reconstrucción**: `Reconstruir proyección de envios`, verificar que el lag vuelve
    a 0 y que los envíos conservan su historial desde `CREADO`.
11. **Replay de analítica**: reprocesar el 4.º indicador y ver el lag del grupo
    `analitica-replay` volver a 0.

---

## 8. Criterios de aceptación

| Criterio | Cómo se cumple / se verifica | Resultado |
| --- | --- | --- |
| Latencia < 2 s evento → vista | Medición de un escenario hasta verlo en `/api/envios/:id` | **564 ms** |
| Recuperación desde el último offset | Se detuvo el proyector: lag 107 y 404; al arrancarlo: lag 0 y envío `ENTREGADO` con 5 eventos | OK |
| Reconstrucción de vistas | `POST /control/reconstruir`: 309 envíos borrados y reprocesados; 309/309 con `CREADO`, lag 0 | OK |
| Replay del 4.º indicador | `POST /control/replay`: `metricas_hub` y `replay_envios` reconstruidos, lag 0 | OK |
| Cierre de alertas al entregar | Un envío con alerta `VENCIDO` ABIERTA se entregó: la alerta quedó `CERRADA` y el envío `terminal` | OK |
| Aislamiento entre consumidores | Detener un servicio no afecta a los otros grupos | OK |
| Idempotencia y orden | Guardas por `eventId` y `secuencia`; `eventos_procesados` con TTL en analítica | OK |
| DLQ | Escenario inválido + eventos malformados quedan en `envios.estados.dlq` y en `dlq` | OK |
| Observabilidad | Logs JSON con `eventId`, `topic`, `partition`, `offset`; lag por grupo en `/demo` | OK |
| UI | `/torre`, `/envios/:id`, `/tracking/:id`, `/analitica`, `/demo` responden 200 | OK |
| Un solo comando | `docker compose up --build -d` levanta los 11 contenedores | OK |
| Tracking sin datos internos | `/tracking/:id` sólo expone estado, promesa y fechas | OK |

Comandos útiles para repetir la verificación:

```bash
# lag por grupo
curl -s http://localhost:4100/control/lag

# escenario y detalle
curl -s -X POST http://localhost:4000/sim/escenarios/feliz
curl -s http://localhost:3000/api/envios/RS-XXXXXXXX

# reconstrucción de la proyección
curl -s -X POST http://localhost:4100/control/reconstruir

# replay de analítica
curl -s -X POST http://localhost:4100/control/replay

# servicios del stack
curl -s http://localhost:4100/control/estado
```

---

## 9. Decisiones de diseño

- **Una sola imagen para los servicios Node**: `rutasur-services:latest` contiene el
  código y las dependencias; cada contenedor ejecuta un proceso distinto
  (`node proyector/index.js`, etc.). Menos builds, mismo aislamiento de procesos.
- **Proyector como único escritor de las vistas**: concentra la idempotencia y la DLQ,
  y permite reconstruir todo desde los topics.
- **Reloj simulado compartido** en la colección `reloj`: el simulador lo reinicia en
  cada arranque y el monitor de SLA lo consulta. Nunca se usa `Date.now()` para decidir
  vencimientos.
- **Reinicio de offsets propio**: `admin.resetOffsets` de KafkaJS compromete `-2`
  (`EARLIEST_OFFSET`); al reincorporarse, el consumidor lo interpreta como "sin offset"
  y salta al final. El servicio de control se une brevemente al grupo y compromete el
  `low` real de cada partición antes de arrancar el servicio.
- **Reconstrucción completa**: además de `envios`, se limpian `alertas`,
  `notificaciones` y `dlq`, porque también son proyecciones del mismo consumidor. Si no
  se limpiaran, quedarían alertas de envíos inexistentes.
- **Eventos fuera de orden**: se descartan cuando `secuencia <= ultimaSecuencia`. Es una
  política simple y determinista; el 2 % de desorden inyectado no pierde el estado final
  porque la secuencia mayor siempre gana.
- **`ZONA_INACCESIBLE` se trata como `AUSENTE`** para la reprogramación (no hay una
  decisión específica en el enunciado): se reintenta el día hábil siguiente en la franja
  opuesta.
- **Sin llamadas HTTP entre servicios de negocio**: el único HTTP interno es el del
  panel de demo hacia las APIs del simulador y del control, que son herramientas de
  operación, no parte del flujo de eventos.
- **Fechas en UTC** de punta a punta (`TZ=UTC`), formateadas en el cliente para evitar
  desajustes de hidratación.

---

## 10. Solución de problemas

| Síntoma | Causa / solución |
| --- | --- |
| `failed to connect to the docker API` | Docker Desktop no está corriendo. Iniciarlo y esperar al motor. |
| `kafka-init` o `mongo-init` quedan en `exited` | Es esperable: son contenedores de inicialización de un solo uso. El panel de demo no los muestra. |
| La torre no muestra datos | Verificar `docker compose ps` y que `proyector` esté `running`. |
| El panel muestra error de Docker | El socket `/var/run/docker.sock` debe estar montado en `control` (ya viene en el compose). |
| MongoDB sin change streams / SSE sin eventos | El replica set no se inició: revisar `docker compose logs mongo-init`. |
| Un envío da 404 | Puede estar en el lag del proyector: mirar `/demo`. |
| El reloj simulado quedó viejo | Reiniciar `simulador` (reinicia el ancla del reloj). |
| Puertos ocupados | Cambiar `WEB_PORT`, `SIMULADOR_PORT`, `CONTROL_PORT`, `MONGO_PORT` o `KAFKA_EXTERNAL_PORT` en `.env`. |

Logs:

```bash
docker compose logs -f proyector
docker compose logs -f monitor-sla
docker compose logs -f control
```

Los logs son JSON con `ts`, `nivel`, `servicio`, `mensaje` y, cuando aplica, `eventId`,
`shipmentId`, `topic`, `partition` y `offset`.

---

## 11. Estructura del repositorio

```
.
├── docker-compose.yml        # Kafka KRaft, topics, Mongo replica set, servicios y web
├── .env.example              # parámetros configurables
├── services/
│   ├── Dockerfile            # imagen única para todos los servicios Node
│   ├── shared/               # config, eventos (zod), Kafka, Mongo, reloj, índices, logs
│   ├── simulator/            # generador de tráfico, escenarios y reloj
│   ├── proyector/            # CU1 + vistas + DLQ
│   ├── notificaciones/       # CU2
│   ├── reprogramacion/       # CU2
│   ├── monitor-sla/          # CU3
│   ├── analitica/            # CU4 (indicadores 1–3)
│   ├── analitica-replay/     # CU4 (indicador 4)
│   └── control/              # operación de la demo y offsets
└── web/                      # Next.js (App Router)
    ├── app/torre               # torre de control
    ├── app/envios/[id]         # detalle interno
    ├── app/tracking/[id]       # tracking público
    ├── app/analitica           # analítica
    ├── app/demo                # panel de demo
    └── app/api/stream          # SSE con change streams
```

---

## 12. Configuración

Copiar `.env.example` a `.env` para cambiar valores. Los más relevantes:

| Variable | Default | Descripción |
| --- | --- | --- |
| `SIM_SPEEDUP` | `120` | 1 h simulada = 30 s reales. |
| `SIM_SHIPMENTS_PER_MINUTE` | `20` | Ritmo del flujo continuo. |
| `SIM_SEED` | `42` | Semilla del generador. |
| `SIM_HUB_DELAY_PROB` | `0.10` | Probabilidad de demora en hub. |
| `SIM_DUPLICADO_PROB` | `0.02` | Duplicados inyectados. |
| `SIM_DESORDEN_PROB` | `0.02` | Eventos fuera de orden. |
| `SIM_MALFORMADO_PROB` | `0.01` | Eventos malformados. |
| `SLA_TICK_MS` | `60000` | Evaluación periódica de SLA (reloj real). |
| `WEB_PORT` | `3000` | Puerto de la interfaz. |
| `MONGO_PORT` | `27018` | Puerto de Mongo en el host. |
