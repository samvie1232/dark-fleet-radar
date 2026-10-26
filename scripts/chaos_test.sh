#!/usr/bin/env bash
# Prueba de caos: mata el TaskManager de Flink a mitad de camino y verifica que la tabla bronze
# no tenga duplicados ni huecos. Uso: ./scripts/chaos_test.sh  (con el stack ya corriendo)
set -euo pipefail
C="docker compose"

echo "1) Estado antes del fallo"
$C --profile tools run --rm --build tools verify

echo "2) Matando el TaskManager (como si se cayera la máquina)…"
$C kill flink-taskmanager
$C --profile tools run --rm tools watch-flink 20
echo "3) Levantándolo de nuevo; Flink retoma desde el último checkpoint"
$C start flink-taskmanager

echo "4) Esperando 2 checkpoints nuevos (~2 min)…"
$C --profile tools run --rm tools watch-flink 150

echo "5) Verificación final"
$C --profile tools run --rm tools verify
