# Prueba de caos para Windows (PowerShell). Uso: .\scripts\chaos_test.ps1  (con el stack corriendo)
$ErrorActionPreference = "Stop"

Write-Host "1) Estado antes del fallo"
docker compose --profile tools run --rm tools verify

Write-Host "2) Matando el TaskManager (como si se cayera la maquina)..."
docker compose kill flink-taskmanager
Start-Sleep -Seconds 20
Write-Host "3) Levantandolo de nuevo; Flink retoma desde el ultimo checkpoint"
docker compose start flink-taskmanager

Write-Host "4) Esperando 2 checkpoints nuevos (~2 min)..."
Start-Sleep -Seconds 150

Write-Host "5) Verificacion final"
docker compose --profile tools run --rm tools verify
