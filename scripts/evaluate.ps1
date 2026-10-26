# Precision/recall de las detecciones para Windows (PowerShell). Uso: .\scripts\evaluate.ps1
# (con el stack corriendo). Tarda unos 15 minutos y deja el reporte en reports\evaluation.md.
$ErrorActionPreference = "Stop"

Write-Host "1) Preparando topic y tablas de evaluacion (no toca los datos reales)"
docker compose --profile tools run --rm --build tools eval-reset

Write-Host "2) Lanzando el job de evaluacion (mismo SQL de detecciones, sobre ais.eval)"
docker compose run --rm --entrypoint /docker-entrypoint.sh flink-job /opt/evaluate.sh

Write-Host "3) Inyectando 12 h simuladas y comparando las alertas con las etiquetas"
docker compose --profile tools run --rm tools evaluate
