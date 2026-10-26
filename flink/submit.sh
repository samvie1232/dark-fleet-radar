#!/usr/bin/env bash
# Envía el job de Flink SQL una sola vez. Si ya hay un checkpoint previo (por ejemplo tras
# apagar el PC), el job retoma desde él: mismos offsets de Kafka y mismo estado, sin duplicar.
set -euo pipefail

JM="http://flink-jobmanager:8081"
JOB_NAME="dark-fleet-radar"
CHECKPOINTS="/flink-data/checkpoints"

echo "Esperando al JobManager…"
until curl -sf "$JM/overview" >/dev/null; do sleep 2; done

# Cada job del overview queda en su propia línea al cortar por '{'.
if curl -sf "$JM/jobs/overview" | tr '{' '\n' | grep "\"name\":\"$JOB_NAME\"" \
    | grep -Eq '"state":"(INITIALIZING|CREATED|RUNNING|RESTARTING)"'; then
  echo "El job '$JOB_NAME' ya está corriendo; no se envía otro."
  exit 0
fi

latest=$(ls -td "$CHECKPOINTS"/*/chk-*/ 2>/dev/null | while read -r d; do
  [ -f "$d/_metadata" ] && { echo "$d"; break; }
done || true)

job=/tmp/job.sql
: > "$job"
if [ -n "$latest" ]; then
  echo "Retomando desde el checkpoint $latest"
  echo "SET 'execution.savepoint.path' = '$latest';" >> "$job"
  echo "SET 'execution.savepoint.ignore-unclaimed-state' = 'true';" >> "$job"
else
  echo "Sin checkpoints previos: arranque desde cero"
fi
cat /opt/sql/pipeline.sql >> "$job"

/opt/flink/bin/sql-client.sh -f "$job"

# sql-client sale con 0 aunque falle una sentencia: confirmamos que el job quedó corriendo.
sleep 5
if curl -sf "$JM/jobs/overview" | tr '{' '\n' | grep "\"name\":\"$JOB_NAME\"" \
    | grep -Eq '"state":"(INITIALIZING|CREATED|RUNNING)"'; then
  echo "Job '$JOB_NAME' enviado y corriendo."
else
  echo "ERROR: el job '$JOB_NAME' no quedó corriendo; revisa el log de arriba." >&2
  exit 1
fi
