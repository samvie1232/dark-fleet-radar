#!/usr/bin/env bash
# Envía el job de evaluación: el mismo detections.sql, pero leyendo el topic ais.eval (escenarios con
# etiqueta que inyecta `tools evaluate`) y escribiendo en tablas *_eval, sin tocar las de producción.
set -euo pipefail

JM="http://flink-jobmanager:8081"
NAME="dark-fleet-radar-eval"

until curl -sf "$JM/overview" >/dev/null; do sleep 2; done

# Una evaluación a la vez: cancela la anterior si sigue corriendo.
curl -sf "$JM/jobs/overview" | tr '{' '\n' | grep "\"name\":\"$NAME\"" \
  | grep -E '"state":"(INITIALIZING|CREATED|RUNNING|RESTARTING)"' \
  | sed -E 's/.*"jid":"([0-9a-f]+)".*/\1/' | while read -r jid; do
    echo "Cancelando la evaluación anterior ($jid)"
    curl -sf -X PATCH "$JM/jobs/$jid?mode=cancel" >/dev/null || true
  done || true

job=/tmp/eval.sql
{
  echo "SET 'execution.checkpointing.interval' = '10 s';"
  sed -e "s|'dark-fleet-radar-detections-v[0-9]*'|'$NAME'|" \
      -e "s|checkpoints-detections-v[0-9]*|checkpoints-eval-$(date +%s)|" \
      -e "s|'topic' = 'ais.raw'|'topic' = 'ais.eval'|" \
      -e "s|'flink-dark-fleet-radar-detections'|'flink-dark-fleet-radar-eval'|" \
      -e "s|'scan.startup.mode' = 'group-offsets'|'scan.startup.mode' = 'earliest-offset'|" \
      -e "s|lake\.gold\.alerts\b|lake.gold.alerts_eval|g" \
      -e "s|lake\.gold\.coverage_h3\b|lake.gold.coverage_h3_eval|g" \
      -e "s|'table-name' = 'coverage_hourly'|'table-name' = 'coverage_hourly_eval'|" \
      -e "s|'table-name' = 'coverage_zone_hourly'|'table-name' = 'coverage_zone_hourly_eval'|" \
      -e "s|'table-name' = 'alerts'|'table-name' = 'alerts_eval'|" \
      /opt/sql/detections.sql
} > "$job"

/opt/flink/bin/sql-client.sh -f "$job"
sleep 5
if curl -sf "$JM/jobs/overview" | tr '{' '\n' | grep "\"name\":\"$NAME\"" | grep -q '"state":"RUNNING"'; then
  echo "Job de evaluación corriendo."
else
  echo "ERROR: el job de evaluación no quedó corriendo; revisa el log de arriba." >&2
  exit 1
fi
