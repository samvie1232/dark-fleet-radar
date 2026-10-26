"""Métricas Prometheus del productor (se exponen en :METRICS_PORT/metrics)."""

from prometheus_client import Counter, Gauge, Histogram

RECEIVED = Counter("ais_messages_received_total", "Frames recibidos de aisstream")
PUBLISHED = Counter("ais_messages_published_total", "Mensajes válidos enviados a Kafka", ["type"])
INVALID = Counter("ais_messages_invalid_total", "Mensajes enviados a la DLQ", ["reason"])
PUBLISH_ERRORS = Counter("ais_publish_errors_total", "Fallos al escribir en Kafka")
RECONNECTS = Counter("ais_reconnects_total", "Reconexiones al WebSocket")
CONNECTED = Gauge("ais_connected", "1 si el WebSocket está conectado")
LAST_MESSAGE = Gauge("ais_last_message_timestamp_seconds", "Hora de llegada del último frame")
EVENT_DELAY = Histogram(
    "ais_event_delay_seconds",
    "Retraso entre la hora del evento AIS y su llegada al productor",
    buckets=(0.5, 1, 2, 5, 10, 30, 60, 120, 300, 900, 3600),
)
