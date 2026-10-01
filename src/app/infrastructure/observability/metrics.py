from prometheus_client import (
    REGISTRY,
    Counter,
    Histogram,
    generate_latest,
)


# 1. Métricas de Transação (HTTP API)
TRANSACTIONS_CREATED_TOTAL = Counter(
    "transactions_created_total",
    "Total de transações criadas via API",
    ["status"],
)

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "Latência de requisições HTTP na API",
    ["method", "endpoint"],
    buckets=(0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total de requisições HTTP recebidas",
    ["method", "endpoint", "status_code"],
)

# 2. Métricas de Mensageria e Workers
OUTBOX_EVENTS_PUBLISHED_TOTAL = Counter(
    "outbox_events_published_total",
    "Total de eventos da outbox publicados no Kafka",
    ["topic", "status"],
)

KAFKA_MESSAGES_CONSUMED_TOTAL = Counter(
    "kafka_messages_consumed_total",
    "Total de mensagens consumidas nos tópicos do Kafka",
    ["topic", "status"],  # status: processed, duplicate_skipped, retried, dlq, failed
)

CONSUMER_PROCESSING_DURATION_SECONDS = Histogram(
    "kafka_consumer_processing_duration_seconds",
    "Duração total do processamento de uma mensagem no consumer",
    ["topic"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

# 3. Métricas de Análise de Risco
RISK_ANALYSIS_REQUESTS_TOTAL = Counter(
    "risk_analysis_requests_total",
    "Total de requisições enviadas ao serviço externo de risco",
    ["decision", "result"],  # result: success, transient_error, permanent_error
)

RISK_ANALYSIS_DURATION_SECONDS = Histogram(
    "risk_analysis_duration_seconds",
    "Tempo de resposta da chamada externa de análise de risco",
    ["result"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0),
)

# 4. Métricas de DLQ
DLQ_MESSAGES_TOTAL = Counter(
    "dlq_messages_total",
    "Total de mensagens encaminhadas para a Dead Letter Queue",
    ["original_topic", "reason"],
)


def get_prometheus_metrics_payload() -> bytes:
    """Gera o payload em formato de texto Prometheus."""
    return generate_latest(REGISTRY)
