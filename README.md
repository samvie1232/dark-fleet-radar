# Dark Fleet Radar

Radar de "flotas fantasma" en tiempo real: recibe en vivo las señales AIS de los barcos, detecta
comportamientos sospechosos (apagones de señal, spoofing, transferencias en alta mar, cambios de
identidad, barcos sancionados) y los muestra en un mapa, con métricas que prueban que las alertas
funcionan.

> Estado: **paso 3 de 4, primera mitad**. Ingesta exactly-once a Iceberg, mapa de cobertura H3 y
> tres detecciones (spoofing, barcos sancionados, cambios de identidad). Siguen apagones de señal,
> transferencias entre barcos y merodeo cerca de cables, y luego la validación con precision/recall.

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
     │        └─ consulta OpenSanctions en Postgres (lookup join con caché de 1 h)
     ├─ gold.coverage_h3            mensajes y barcos por celda H3 (resolución 7) y hora
     └─ gold.alerts                 una fila por alerta, con puntaje y evidencia en JSON
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
TaskManager a mitad de camino, lo levanta y vuelve a verificar. Resultado en desarrollo:

| Momento | Filas en bronze | Duplicados | Huecos |
|---|---|---|---|
| Antes de matar el TaskManager | 34.000 | 0 | 0 |
| Después de recuperarse | 52.000 | 0 | 0 |
| Tras reiniciar todo Flink (retoma desde el checkpoint) | 64.560 | 0 | 0 |

Si apagas el PC, al volver corre `docker compose up -d`: el servicio `flink-job` encuentra el
último checkpoint y retoma desde ahí, y si el job ya está corriendo no envía otro.

**Decisión de diseño, S3 local:** el brief original usaba MinIO, pero MinIO dejó de publicar
imágenes en Docker Hub. Iceberg solo necesita una API S3, así que se usa SeaweedFS (Apache 2.0).

## Detecciones

Corren en un job de Flink aparte del de ingesta: se pueden cambiar las reglas y reenviar ese job
sin tocar bronze/silver. Cada alerta tiene un `alert_id` determinista (hash de tipo, barco y hora),
así que si una alerta se escribiera dos veces se reconoce como la misma.

| Alerta | Regla | Puntaje |
|---|---|---|
| `spoofing_jump` | Entre dos posiciones seguidas del mismo MMSI haría falta ir a más de 50 nudos, con un salto de más de 2 km (ningún mercante pasa de unos 30 nudos; los 2 km evitan confundir ruido de GPS con un salto) | velocidad / 100, máximo 1 |
| `sanctioned_vessel` | El IMO (con dígito de control válido) o el MMSI de un reporte estático aparece en la lista marítima de [OpenSanctions](https://www.opensanctions.org/datasets/maritime/). Una alerta por barco y entrada de la lista | 1,0 si está sancionado, 0,7 si es otro riesgo |
| `identity_change` | Mismo IMO con otro nombre u otro MMSI en menos de 30 días | 0,9 si cambia la bandera (MID), 0,7 si cambia el MMSI, 0,5 si solo el nombre |

La lista de OpenSanctions se descarga y se carga en Postgres al arrancar (servicio `ref-loader`).
Para refrescarla: `docker compose --profile tools run --rm tools load-sanctions`.

Funciones propias en Java (`flink/udf/`, se compilan y se prueban al construir la imagen):
`h3_cell` (celda H3), `haversine_km`, `implied_speed_kn`, `imo_valid` y `epoch_ms`.

Ver las alertas (conteo por tipo y las 20 más recientes):

```bash
docker compose --profile tools run --rm tools alerts
```

**Nota técnica:** en Flink 1.20 el sql-client falla ("belongs to a different planner") si se usan
vistas temporales dentro de un `STATEMENT SET`, así que los campos del JSON se sacan como columnas
calculadas de la tabla fuente y cada consulta va completa en su `INSERT`.

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

## Ética y licencias

Uso no comercial. Las alertas son señales para priorizar una revisión, nunca prueba de ilegalidad.
No se enriquece con datos personales. La API key vive solo en `.env`, que está en `.gitignore`.
Código bajo licencia MIT.
