# Dark Fleet Radar

Radar de "flotas fantasma" en tiempo real: recibe en vivo las señales AIS de los barcos, detecta
comportamientos sospechosos (apagones de señal, spoofing, transferencias en alta mar, cambios de
identidad, barcos sancionados) y los muestra en un mapa, con métricas que prueban que las alertas
funcionan.

> Estado: **paso 1 de 4, ingesta**. aisstream.io → productor Python → Redpanda.
> Siguen Flink + Iceberg, cobertura H3 y detecciones, y validación con precision/recall.

## Arquitectura (paso 1)

```
aisstream.io (WebSocket, Báltico + Mar del Norte)
        │
  producer/  ── valida, reconecta con backoff + jitter, key = MMSI
        │            ├─► data/raw/ais-AAAAMMDD-HH.jsonl   (grabación para replay)
        │            └─► :8000/metrics                    (Prometheus)
        ▼
  Redpanda ── ais.raw (6 particiones, 7 días)   ais.dlq (mensajes inválidos + motivo)
```

aisstream no tiene SLA ni replay, por eso todo se persiste en Kafka y en disco antes de procesarlo.

## Cómo correrlo

Requisitos: Docker, 8 GB de RAM como mínimo (16 GB recomendados) y una API key gratuita de
[aisstream.io](https://aisstream.io).

```bash
cp .env.example .env          # y pega tu AISSTREAM_API_KEY dentro de .env
docker compose up -d --build
docker compose logs -f producer
```

- Consola de Redpanda (topics y mensajes): http://localhost:8080
- Métricas del productor: http://localhost:8000/metrics
- Grabaciones: `data/raw/`

Explorar lo grabado con [DuckDB](https://duckdb.org):

```bash
duckdb -c ".read scripts/explore.sql"
```

Cambiar de región: edita `AIS_BOUNDING_BOXES` en `.env`, por ejemplo el Caribe colombiano y la
entrada del Canal de Panamá: `[[[7.0, -82.0], [13.5, -74.0]]]`.

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
cd producer
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q && ruff check .
```

## Ética y licencias

Uso no comercial. Las alertas son señales para priorizar una revisión, nunca prueba de ilegalidad.
No se enriquece con datos personales. La API key vive solo en `.env`, que está en `.gitignore`.
Código bajo licencia MIT.
