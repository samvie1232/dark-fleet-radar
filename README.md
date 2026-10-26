# Dark Fleet Radar

Radar de "flotas fantasma" en tiempo real: recibe en vivo las señales AIS de los barcos, detecta
comportamientos sospechosos (apagones de señal, spoofing, transferencias en alta mar, cambios de
identidad, barcos sancionados) y los muestra en un mapa, con métricas que prueban que las alertas
funcionan.

## En números

| Qué | Resultado |
|---|---|
| Exactly-once | 0 duplicados y 0 huecos por offset de Kafka en bronze, también después de matar Flink a mitad de camino ([prueba de caos](#exactly-once-y-prueba-de-caos)) |
| Detecciones | 6 reglas en streaming: spoofing, apagones, encuentros en alta mar, merodeo sobre cables, barcos sancionados y cambios de identidad |
| Precision / recall | 100 % / 100 % en las 6 reglas sobre 150 barcos etiquetados (positivos y negativos difíciles) ([evaluación](#evaluación-precision-y-recall)) |
| Datos reales | Báltico y Mar del Norte en vivo: unos 3.000 mensajes AIS por minuto en un PC de 16 GB |

![Prueba de caos: se mata el TaskManager, Flink retoma desde el checkpoint y la tabla bronze queda sin duplicados ni huecos](docs/chaos_test.gif)

## Arquitectura

```
aisstream.io (WebSocket, Báltico + Mar del Norte)
        │
  producer/  ── valida, reconecta con backoff + jitter, key = MMSI
        │            ├─► data/raw/ais-AAAAMMDD-HH.jsonl   (grabación para replay)
        │            └─► :8000/metrics                    (Prometheus)
        ▼
  Redpanda ── ais.raw (6 particiones, 7 días)   ais.dlq (mensajes inválidos + motivo)
        │
        ▼
  Flink SQL (flink/sql/pipeline.sql) ── tiempo de evento, watermark de 1 min,
        │                                checkpoints exactly-once cada 60 s
        ▼
  Iceberg (catálogo REST) sobre S3 local (SeaweedFS)
     ├─ ais.ais_raw                 bronze: cada registro de Kafka una vez, con partición y offset
     ├─ ais.positions               silver: lat/lon/SOG/COG tipados, banderas de calidad, sin duplicados
     ├─ ais.vessel_static_reports   silver: IMO, nombre, indicativo, tipo, destino, calado
     │
     │  Segundo job de Flink (flink/sql/detections.sql), mismo topic, otro consumer group
     │        └─ consulta en Postgres OpenSanctions, puertos, cables y cobertura (lookup joins)
     ├─ gold.coverage_h3            mensajes y barcos por celda H3 (resolución 7) y hora
     └─ gold.alerts                 una fila por alerta, con puntaje y evidencia en JSON
                │
                ▼
  Postgres (alertas y cobertura) + Prometheus (métricas) ──► Grafana: mapa y salud del pipeline
```

aisstream no tiene SLA ni replay, por eso todo se persiste en Kafka y en disco antes de procesarlo.

## Cómo correrlo

Requisitos: Docker con unos 6 GB de RAM asignados (PC de 16 GB recomendado) y una API key gratuita de
[aisstream.io](https://aisstream.io).

```bash
cp .env.example .env          # y pega tu AISSTREAM_API_KEY dentro de .env
docker compose up -d --build
docker compose logs -f producer
```

- **Tablero (Grafana): http://localhost:3000** — mapa de alertas, cobertura, alertas por tipo y salud
  del pipeline. Se entra sin usuario; para editar, `admin` / `admin`.
- Flink (job, checkpoints, backpressure): http://localhost:8081
- Resumen del lago: `docker compose --profile tools run --rm tools summary`
- Consola de Redpanda (topics y mensajes): http://localhost:8090 (cámbialo con `CONSOLE_PORT` en `.env`)
- Métricas del productor: http://localhost:8000/metrics
- Grabaciones: `data/raw/`

Explorar lo grabado con [DuckDB](https://duckdb.org):

```bash
duckdb -c ".read scripts/explore.sql"
```

Cambiar de región: edita `AIS_BOUNDING_BOXES` en `.env`, por ejemplo el Caribe colombiano y la
entrada del Canal de Panamá: `[[[7.0, -82.0], [13.5, -74.0]]]`.

## Exactly-once y prueba de caos

Flink guarda los offsets de Kafka y el estado en cada checkpoint (cada 60 s), e Iceberg hace un
commit por checkpoint. Si Flink se cae, retoma desde el último checkpoint: lo que no se confirmó
se vuelve a leer y lo que sí se confirmó no se repite.

La tabla bronze guarda la partición y el offset de cada registro, así que se puede probar:

```bash
docker compose --profile tools run --rm tools verify
```

revisa que en cada partición no haya offsets repetidos (duplicados) ni saltos (pérdidas).

Prueba de caos: `./scripts/chaos_test.sh` (o `.\scripts\chaos_test.ps1` en Windows) mata el
TaskManager a mitad de camino, muestra cómo los dos jobs pasan a `RESTARTING` y vuelven a `RUNNING`
desde el último checkpoint, espera dos checkpoints nuevos y vuelve a verificar. Es el GIF del
inicio (grabado en el entorno de desarrollo con un feed AIS simulado; las esperas largas van aceleradas).
Entre las dos verificaciones entraron 14.099 filas más y siguen en 0 duplicados y 0 huecos en las
6 particiones.

Si apagas el PC, al volver corre `docker compose up -d`: el servicio `flink-job` encuentra el
último checkpoint y retoma desde ahí, y si el job ya está corriendo no envía otro.

**Decisión de diseño, S3 local:** el brief original usaba MinIO, pero MinIO dejó de publicar
imágenes en Docker Hub. Iceberg solo necesita una API S3, así que se usa SeaweedFS (Apache 2.0).

## Tablero

Grafana (`infra/grafana/`) lee de dos fuentes:
- **Postgres**: el job de detecciones copia cada alerta de `gold.alerts` a la tabla `alerts` (lectura
  continua de la tabla Iceberg) y escribe la cobertura por celda y hora en `coverage_hourly`.
- **Prometheus**: métricas del productor (mensajes por minuto, % a la DLQ, retraso del evento) y de
  Flink (checkpoints completados, fallidos y su duración).

El tablero se genera con `python infra/grafana/build_dashboard.py`; se edita ahí y no a mano en el JSON.

## Detecciones

Corren en un job de Flink aparte del de ingesta: se pueden cambiar las reglas y reenviar ese job
sin tocar bronze/silver. Cada alerta tiene un `alert_id` determinista (hash de tipo, barco y hora),
así que si una alerta se escribiera dos veces se reconoce como la misma.

| Alerta | Regla | Puntaje |
|---|---|---|
| `spoofing_jump` | Entre dos posiciones seguidas del mismo MMSI haría falta ir a más de 50 nudos, con un salto de más de 2 km (ningún mercante pasa de unos 30 nudos; los 2 km evitan confundir ruido de GPS con un salto). Una alerta por episodio: solo el primer salto tras 6 h sin saltos de ese barco | velocidad / 100, máximo 1 |
| `sanctioned_vessel` | El IMO (con dígito de control válido) o el MMSI de un reporte estático aparece en la lista marítima de [OpenSanctions](https://www.opensanctions.org/datasets/maritime/). Una alerta por barco, con todas las listas que lo nombran | 1,0 si está sancionado, 0,7 si es otro riesgo |
| `identity_change` | Mismo IMO con un nombre o MMSI nunca visto antes para ese IMO, en menos de 30 días (los barcos que alternan entre dos formas de escribir su nombre no alertan cada vez) | 0,9 si cambia la bandera (MID), 0,7 si cambia el MMSI, 0,5 si solo el nombre |
| `dark_gap` | Un barco calla 2 horas o más fuera de puerto **y** en la hora central del silencio se oía a otros 2 barcos o más en su misma zona (celda H3 de resolución 5, unos 250 km²; una celda chica casi nunca tiene a otro barco en la misma hora en mar abierto). Si no se oía a nadie, el silencio puede ser de cobertura (o del propio receptor apagado) y no alerta | horas de silencio / 12, máximo 1 |
| `sts_encounter` | Dos barcos a menos de 2 nudos (sin estar amarrados), fuera de puerto, a menos de 1 km entre sí durante al menos 90 minutos de una ventana de 2 h: posible transferencia barco a barco. Los pares se buscan entre celdas H3 vecinas, así que dos barcos a la deriva no se pierden por caer a ambos lados de un borde. No cuenta si en la celda (~36 km²) hay más de 4 barcos quietos: eso es un fondeadero o una zona de espera. Alerta en cada ventana mientras dure | 0,5 + minutos juntos / 240, máximo 1 |
| `cable_loitering` | Un barco lento (< 3 nudos) o fondeado pasa 30 minutos o más de una hora (ventanas de 1 h que avanzan cada 30 min) dentro de la franja (~3 km) de un cable o ducto submarino, fuera de puerto. Es el patrón previo a los daños del Balticconnector (2023) y del Eagle S (2024) | 0,4 + minutos / 100, máximo 1 |

Zonas de referencia (`tools/src/radar_tools/geo.py`, como celdas H3 en Postgres): 49 puertos del Mar
del Norte y el Báltico (unos 10 km alrededor) y 9 cables y ductos (Balticconnector, Estlink 1 y 2,
C-Lion1, Nord Stream, BCS, NordBalt, NorNed, North Sea Link). **Los trazados son aproximados**, unos
pocos puntos por cable; para algo serio habría que cargar el trazado real (por ejemplo de EMODnet).

Cuando se cambian las reglas, el job de detecciones sube de versión (hoy `dark-fleet-radar-detections-v4`):
`flink-job` cancela la versión anterior y la nueva sigue desde los offsets que esa dejó en Kafka.

La lista de OpenSanctions se descarga y se carga en Postgres al arrancar (servicio `ref-loader`).
Para refrescarla: `docker compose --profile tools run --rm tools load-sanctions`. El mismo servicio carga
las zonas de puertos y de cables submarinos (`tools/src/radar_tools/geo.py`).

Funciones propias en Java (`flink/udf/`, se compilan y se prueban al construir la imagen):
`h3_cell` (celda H3), `h3_disk` (una celda y sus vecinas), `haversine_km`, `implied_speed_kn`, `imo_valid` y `epoch_ms`.

Ver las alertas (conteo por tipo y las 20 más recientes):

```bash
docker compose --profile tools run --rm tools alerts
```

**Notas técnicas (Flink 1.20):**
- El sql-client falla ("belongs to a different planner") con vistas temporales que llaman funciones
  propias o hacen lookup joins, así que los campos del JSON son columnas calculadas de la tabla
  fuente y cada consulta va completa en su `INSERT`.
- La TVF `TUMBLE` no acepta una subconsulta con lookup join, y aplicarla antes del join convierte la
  agregación en una que emite correcciones (que Iceberg en modo append no acepta). Para encuentros y
  cables se usan las ventanas de grupo clásicas (`GROUP BY TUMBLE(...)`), y el par de barcos se arma
  con un join por intervalo sobre el tiempo de la ventana.

## Evaluación: precision y recall

En datos reales no hay etiquetas, así que no se puede medir directamente cuántas alertas son
correctas. Lo que sí se puede medir es si cada regla hace lo que dice: `tools evaluate`
(`tools/src/radar_tools/evaluate.py`) arma 12 horas simuladas de tráfico con casos etiquetados, los
manda a un topic aparte (`ais.eval`) y los procesa un job de Flink con **el mismo SQL de
detecciones** (`flink/evaluate.sh` solo cambia el topic y las tablas de salida, que terminan en
`_eval`). Al final compara las alertas con las etiquetas.

Por cada tipo hay 12 casos positivos y 12 negativos difíciles, que se parecen al positivo pero no
deben alertar:

| Alerta | Positivos | Negativos difíciles |
|---|---|---|
| `spoofing_jump` | Un reporte desplazado 90 a 330 km durante un minuto | Ferry a 35 nudos, ruido de GPS de ~1 km, 30 min sin reportar |
| `dark_gap` | Silencio de 2,5 a 6 h con otros barcos oyéndose cerca | Silencio de 1,5 h, silencio donde no se oye a nadie, silencio en puerto |
| `sts_encounter` | Dos barcos a 0,1-0,8 km, a menos de 1,5 nudos, 3 a 5 h | A 2,5 km, en puerto, solo 60 min, convoy a 9 nudos, fondeadero con 9 barcos |
| `cable_loitering` | Barco lento sobre un cable 45 a 120 min | Cruza a 10 nudos, solo 20 min, lento pero lejos del cable |
| `sanctioned_vessel` | IMO de la lista cargada (hay 2 en la lista de ejemplo) | IMO válido que no está en la lista |
| `identity_change` | Mismo IMO con otro MMSI y bandera | Misma identidad todo el tiempo, mismo nombre con otras mayúsculas y espacios |

Resultado (`reports/evaluation.md`):

| Alerta | Positivos | Negativos difíciles | Precision | Recall | Alertas por caso |
|---|---|---|---|---|---|
| `spoofing_jump` | 12 | 12 | 100 % | 100 % | 1.0 |
| `dark_gap` | 12 | 12 | 100 % | 100 % | 1.0 |
| `sts_encounter` | 12 | 28 | 100 % | 100 % | 1.6 |
| `cable_loitering` | 12 | 12 | 100 % | 100 % | 2.5 |
| `sanctioned_vessel` | 2 | 12 | 100 % | 100 % | 1.0 |
| `identity_change` | 12 | 12 | 100 % | 100 % | 1.0 |

En encuentros, los negativos se cuentan por barco: cada fondeadero aporta 9.

La evaluación ya sirvió: la primera corrida dio 0 % de recall en apagones y 17 % en encuentros.
Esto es lo que se corrigió:
- **Apagones:** la cobertura se miraba en la misma celda H3 de resolución 7 (~5 km²), y en mar
  abierto casi nunca hay otro barco en una celda tan chica en la misma hora. Ahora se mira por
  zona (resolución 5, ~250 km²).
- **Encuentros:** el par se armaba solo si los dos barcos caían en la misma celda, y dos barcos a la
  deriva cruzan bordes todo el tiempo. Ahora se buscan en la celda y sus vecinas (`h3_disk`).
- **Fondeaderos:** con datos reales salían miles de "encuentros" en zonas de espera frente a la
  costa, donde decenas de barcos fondean juntos. Ahora, si en la celda hay más de 4 barcos quietos,
  no se alerta. Esto tiene un costo: un trasvase real dentro de un fondeadero tampoco alerta.
- **Cables:** con ventanas fijas de 1 h, un barco que merodeaba 54 minutos repartidos en dos horas
  no alertaba. Ahora las ventanas avanzan cada 30 minutos.
- **Generador:** un negativo salía como falsa alarma porque dos casos compartían el mismo IMO por
  azar. Ahora los IMO del generador son únicos.

"Alertas por caso" mayor que 1 es esperable en encuentros y cables: alertan una vez por ventana
mientras dure el patrón (encuentros cada 2 h; cables en ventanas de 1 h que se solapan cada 30 min).
Un 100 % aquí quiere decir que cada regla hace lo que dice sobre estos casos, no que acierte siempre
con datos reales.

Para correrla (con el stack levantado; tarda unos 15 minutos y no toca los datos reales):

```bash
./scripts/evaluate.sh        # Linux/macOS
.\scripts\evaluate.ps1     # Windows (PowerShell)
```

## Qué hace el productor

| Tema | Decisión |
|---|---|
| Reconexión | Backoff exponencial con jitter completo (1 s a 60 s); se reinicia tras 60 s estables |
| Lectura rápida | `send()` solo encola en el buffer de Kafka; no frena el WebSocket (aisstream descarta si lees lento) |
| Duplicados | Productor idempotente con `acks=all` |
| Partición | Key = MMSI, así todos los mensajes de un barco quedan en orden en la misma partición |
| Validación | A la DLQ van JSON roto, campos faltantes, hora ilegible y MMSI que no es de barco (fuera de 2xx–7xx o de relleno como 123456789) |
| Calidad de posición | Lat 91 / lon 181 y saltos imposibles **no** se descartan aquí; se marcan en Flink |
| Grabación | Cada frame crudo, también los inválidos, en JSONL por hora UTC |

## Desarrollo

```bash
cd producer            # o cd tools
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q && ruff check .
```

## Limitaciones conocidas

- **Cobertura AIS:** aisstream junta receptores en tierra. Lejos de la costa hay huecos, y un
  apagón ahí no se puede distinguir de falta de cobertura (por eso la regla lo exige).
- **Zonas aproximadas:** los trazados de cables y las zonas de puerto son de pocos puntos. Un
  trasvase dentro de un fondeadero concurrido tampoco alerta, por el filtro de densidad.
- **Evaluación sintética:** el 100 % mide que cada regla hace lo que dice sobre casos construidos,
  no su precisión en el mundo real, donde no hay etiquetas.
- **Un solo nodo:** todo corre en Docker en un PC. Flink, Redpanda y el catálogo (SQLite) tienen
  paralelismo 1 y no hay alta disponibilidad.

## Ética y licencias

Uso no comercial. Las alertas son señales para priorizar una revisión, nunca prueba de ilegalidad.
No se enriquece con datos personales. La API key vive solo en `.env`, que está en `.gitignore`.
Código bajo licencia MIT.
