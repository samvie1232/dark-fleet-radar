#!/usr/bin/env bash
# Precision/recall de las detecciones con escenarios etiquetados. Uso: ./scripts/evaluate.sh
# (con el stack corriendo). Tarda unos 15 minutos y deja el reporte en reports/evaluation.md.
set -euo pipefail

echo "1) Preparando topic y tablas de evaluación (no toca los datos reales)"
docker compose --profile tools run --rm --build tools eval-reset

echo "2) Lanzando el job de evaluación (mismo SQL de detecciones, sobre ais.eval)"
docker compose run --rm --entrypoint /docker-entrypoint.sh flink-job /opt/evaluate.sh

echo "3) Inyectando 12 h simuladas y comparando las alertas con las etiquetas"
docker compose --profile tools run --rm tools evaluate
