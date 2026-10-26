# Evaluación de las detecciones

Corrida: 2026-10-26 22:13 UTC · 12 h simuladas · 58771 mensajes · semilla 42

| Alerta | Positivos | Negativos difíciles | Precision | Recall | Alertas por caso |
|---|---|---|---|---|---|
| `spoofing_jump` | 12 | 12 | 100 % | 100 % | 1.0 |
| `dark_gap` | 12 | 12 | 100 % | 100 % | 1.0 |
| `sts_encounter` | 12 | 28 | 100 % | 100 % | 1.3 |
| `cable_loitering` | 12 | 12 | 100 % | 100 % | 2.5 |
| `sanctioned_vessel` | 2 | 12 | 100 % | 100 % | 1.0 |
| `identity_change` | 12 | 12 | 100 % | 100 % | 1.0 |

## Fallos

Ninguno.

Casos construidos a mano con `tools evaluate` (ver `tools/src/radar_tools/evaluate.py`): miden si cada regla hace lo que dice, no su precisión con datos reales, que no traen etiquetas.
