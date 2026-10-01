---
title: "Skill: Observabilidade com OpenTelemetry, Logs Estruturados, Métricas Prometheus e Escala"
type: "skill"
category: "observability"
status: "approved"
related_specs:
  - "specs/api_v1.yaml"
  - "specs/events/transaction_created.json"
  - "specs/events/transaction_status_changed.json"
related_plans:
  - "docs/plans/etapa_6_observabilidade_escala_plan.md"
created_at: "2026-10-01"
updated_at: "2026-10-01"
---

# Skill: Observabilidade com OpenTelemetry, Logs Estruturados, Métricas Prometheus e Escala

Este documento formaliza as práticas e padrões arquiteturais para **OpenTelemetry (W3C TraceContext & Spans)**, **Logs Estruturados em JSON correlacionados com Traces**, **Métricas Prometheus**, testes de **Simulação de Carga (Locust 10k eventos/min)** e **Escalabilidade Horizontal** no Apache Kafka.

---

## 1. Padrão OpenTelemetry & W3C TraceContext

### 1.1. Propagação de Contexto Distribuído (W3C TraceContext)
Para rastreabilidade completa ponta a ponta através de fronteiras assíncronas (HTTP -> MySQL Outbox -> Kafka Topic -> Consumer Worker -> HTTP Risk API), utilizamos o padrão **W3C TraceContext**:
- **Header HTTP / Kafka Header**: `traceparent` (formato: `00-<trace_id_32hex>-<parent_span_id_16hex>-<trace_flags_2hex>`).
- Suporte a cabeçalho legado `X-Correlation-ID` mapeado de forma compatível.
- **Injeção (Inject)**: Realizada ao enviar requisições HTTP ou produzir mensagens nos tópicos do Kafka.
- **Extração (Extract)**: Realizada no middleware FastAPI e na leitura de mensagens pelos workers do Kafka (`TransactionConsumerWorker`, `RetryConsumerWorker`).

### 1.2. Ciclo de Spans Semânticos
```
[HTTP POST /transactions] (Server Span)
  │
  ├──> [DB Insert Transaction + Outbox] (Internal/DB Span)
  │
[OutboxPublisherWorker] (Background Job)
  └──> [Kafka Produce: transactions.created.v1] (Producer Span - injects traceparent)
         │
[TransactionConsumerWorker] (Consumer Span - extracts traceparent)
  ├──> [Inbox try_acquire & DB lock] (Internal/DB Span)
  ├──> [HTTP POST /risk-analysis] (Client/HTTP Span - injects traceparent)
  └──> [DB Update Transaction Status & Outbox Event] (Internal/DB Span)
```

---

## 2. Logs Estruturados em JSON Correlacionados (Trace-Log Correlation)

Todos os logs em JSON contêm automaticamente `trace_id` e `span_id` extraídos do contexto ativo do OpenTelemetry:
```json
{
  "timestamp": "2026-10-01T12:00:00.123Z",
  "level": "INFO",
  "logger": "app.entrypoints.workers.transaction_consumer_worker",
  "message": "Transação avaliada com sucesso",
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7",
  "transaction_id": "a93e5066-6db8-4033-a607-bb7cbda984a9",
  "event_id": "evt-771891",
  "customer_id": "cust-***8812",
  "duration_ms": 42.5
}
```

### 2.1. Mascaramento e Proteção de Dados Sensíveis (PII / LGPD)
Para garantir conformidade com normas de segurança e privacidade (LGPD/GDPR):
- **Campos Sensíveis Mascarados**: CPF/CNPJ, e-mail, senhas, dados de cartão/tokens e identificadores pessoais (`customer_id` parcial).
  - Exemplos: `123.456.789-00` -> `***.456.***-00`, `cust-12345` -> `cust-***2345`, `user@domain.com` -> `u***@domain.com`.
- **Filtro Sanitizador Automático (`DataMaskingFilter`)**: Implementado no pipeline de logging (`src/app/infrastructure/observability/logging.py`) inspecionando e mascarando chaves sensíveis em dicionários, records e mensagens antes da serialização JSON.
- **Proibição Estrita de Dump de Payloads Brutos**: Proibido registrar payloads completos contendo credenciais ou informações cadastrais confidenciais em nível `INFO` ou superior.

---

## 3. Métricas Prometheus e Health Probes

A aplicação expõe `/metrics` com métricas consolidadas:
- `transactions_created_total`: Counter `[status]`
- `http_requests_total`: Counter `[method, endpoint, status_code]`
- `http_request_duration_seconds`: Histogram `[method, endpoint]`
- `kafka_messages_consumed_total`: Counter `[topic, status]` (`processed`, `duplicate_skipped`, `retried`, `dlq`)
- `kafka_consumer_processing_duration_seconds`: Histogram `[topic]`
- `risk_analysis_requests_total`: Counter `[decision, result]`
- `risk_analysis_duration_seconds`: Histogram `[result]`
- `dlq_messages_total`: Counter `[reason, original_topic]`

Health Probes:
- `/health/live`: Liveness probe (HTTP 200).
- `/health/ready`: Readiness probe verificando conexões com MySQL e Kafka.

---

## 4. Teste de Performance e Carga (Locust — Carga Sustentada e Spike Test)

- Script em `src/tests/performance/locustfile.py`.
- **Modos de Execução**:
  1. **Carga Sustentada (10k eventos/min)**:
     - Simulação de taxa contínua de ~167 req/s com injeção de TraceContext e duplicatas concorrentes.
  2. **Aumento Abrupto de Carga (`SpikeLoadShape`)**:
     - Estágio 1 (0-30s): Aquecimento e carga baixa (~100 a 300 req/min -> 5 usuários).
     - Estágio 2 (30-90s): **Spike abrupto** (100 -> 10.000+ req/min -> 150 usuários gerados a 100 usuários/s).
     - Estágio 3 (90-120s): Estabilização e encerramento (100 usuários).
- **Validações de Integridade**:
  - Isolamento de partições e balanceamento de Consumer Groups no Kafka.
  - Zero deadlocks e locks a nível de linha no MySQL durante o pico abrupto.
  - Deduplicação estrita de transações pelo Inbox Pattern mesmo sob concorrência maciça.
