#!/usr/bin/env bash
# Envía los jobs de Flink SQL una sola vez cada uno. Si un job ya tiene checkpoints (por ejemplo
# tras apagar el PC), retoma desde el último: mismos offsets de Kafka y mismo estado, sin duplicar.
set -euo pipefail

JM="http://flink-jobmanager:8081"

echo "Esperando al JobManager…"
until curl -sf "$JM/overview" >/dev/null; do sleep 2; done

job_state_ok() {  # $1 = nombre del job; $2 = regex de estados aceptados
  # Cada job del overview queda en su propia línea al cortar por '{'.
  curl -sf "$JM/jobs/overview" | tr '{' '\n' | grep "\"name\":\"$1\"" | grep -Eq "\"state\":\"($2)\""
}

submit_job() {  # $1 = nombre, $2 = archivo SQL, $3 = carpeta de checkpoints del job
  local name="$1" sql="$2" ckpts="$3"

  if job_state_ok "$name" 'INITIALIZING|CREATED|RUNNING|RESTARTING'; then
    echo "El job '$name' ya está corriendo; no se envía otro."
    return 0
  fi

  local latest
  latest=$(ls -td "$ckpts"/*/chk-*/ 2>/dev/null | while read -r d; do
    [ -f "$d/_metadata" ] && { echo "$d"; break; }
  done || true)

  local job="/tmp/$name.sql"
  : > "$job"
  if [ -n "$latest" ]; then
    echo "[$name] Retomando desde el checkpoint $latest"
    echo "SET 'execution.savepoint.path' = '$latest';" >> "$job"
    echo "SET 'execution.savepoint.ignore-unclaimed-state' = 'true';" >> "$job"
  else
    echo "[$name] Sin checkpoints previos: arranque desde cero"
  fi
  cat "$sql" >> "$job"

  /opt/flink/bin/sql-client.sh -f "$job"

  # sql-client sale con 0 aunque falle una sentencia: confirmamos que el job quedó corriendo.
  sleep 5
  if job_state_ok "$name" 'INITIALIZING|CREATED|RUNNING'; then
    echo "Job '$name' enviado y corriendo."
  else
    echo "ERROR: el job '$name' no quedó corriendo; revisa el log de arriba." >&2
    return 1
  fi
}

# Ingesta (bronze + silver). Usa la carpeta de checkpoints por defecto del cluster.
submit_job "dark-fleet-radar" /opt/sql/pipeline.sql /flink-data/checkpoints
# Detecciones (gold). Tiene su propia carpeta, fijada en detections.sql.
submit_job "dark-fleet-radar-detections" /opt/sql/detections.sql /flink-data/checkpoints-detections
