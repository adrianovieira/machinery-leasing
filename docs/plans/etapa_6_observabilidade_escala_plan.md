---
title: "Plano de Implementação: Etapa 6 — Observabilidade com OpenTelemetry, Métricas Prometheus, Teste de Carga e Documentação"
type: "plan"
phase: "completed"
status: "done"
skills_required:
  - "docs/skills/observability_and_scaling.md"
  - "docs/skills/idempotency.md"
  - "docs/skills/retry_dlq.md"
  - "docs/skills/outbox_pattern.md"
tools_required:
  - "opentelemetry-api"
  - "opentelemetry-sdk"
  - "prometheus-client"
  - "python-json-logger"
  - "fastapi"
  - "confluent-kafka"
  - "locust"
  - "pytest"
  - "ruff"
dependencies:
  - "docs/plans/etapa_1_contratos_modelagem_plan.md"
  - "docs/plans/etapa_2_api_basica_persistencia_plan.md"
  - "docs/plans/etapa_3_transactional_outbox_plan.md"
  - "docs/plans/etapa_4_consumidor_idempotente_plan.md"
  - "docs/plans/etapa_5_resiliencia_retries_dlq_plan.md"
acceptance_criteria:
  - "Rastreamento distribuído com OpenTelemetry (W3C TraceContext) configurado, injetando e propagando `traceparent` (Trace ID e Span ID) entre requisições HTTP, eventos Kafka e chamadas de análise de risco."
  - "Logs estruturados em JSON implementados com `python-json-logger`, correlacionando automaticamente `trace_id` e `span_id` do OpenTelemetry com `transaction_id` e `event_id`."
  - "Filtro sanitizador de mascaramento de dados sensíveis (`DataMaskingFilter`) para proteção de PII/LGPD (CPF/CNPJ, tokens, senhas e IDs cadastrais) ativo no pipeline de logs."
  - "Módulo central de métricas com `prometheus-client` e endpoint `/metrics` na API FastAPI disponibilizando contagem de transações, contagem de erros, mensagens de outbox/inbox/retries/dlq e histogramas de tempo de processamento."
  - "Health check probes `/health/live` e `/health/ready` (com checagem ativa de conectividade com MySQL e Kafka) funcionando e testados."
  - "Script de teste de carga e performance em `src/tests/performance/locustfile.py` parametrizado para 10.000 eventos/minuto (~167 req/s), exercitando endpoints de criação, consulta e cenários de duplicatas concorrentes com validação de integridade."
  - "Revisão e atualização da documentação técnica em `docs/` (Skills em Markdown/Asciidoc, ADRs em `.adoc` incluindo ADR 005 de Observabilidade e OpenTelemetry)."
  - "100% dos testes da suíte automatizada passando via `pytest` e conformidade de formatação com `ruff`."
created_at: "2026-10-01"
updated_at: "2026-10-01"
---

# Implementation Plan: Etapa 6 — Observabilidade com OpenTelemetry, Métricas Prometheus, Teste de Carga e Documentação

Este documento define o plano de implementação detalhado da **Etapa 6**, contemplando:
1. **Rastreabilidade com OpenTelemetry (W3C TraceContext)**: Spans distribuídos e propagação de `traceparent` através de HTTP e Kafka Headers.
2. **Logs Estruturados Correlacionados com Mascaramento de Dados (PII / LGPD)**: JSON logger com correlação `trace_id`/`span_id` e sanitização automática de dados confidenciais.
3. **Métricas Prometheus e Health Checks**: Endpoint `/metrics` com contagem de transações, erros e histogramas de latência, além de probes `/health/live` e `/health/ready`.
4. **Simulação de Carga Massiva (Locust — 10k eventos/minuto)**: Script em `src/tests/performance/locustfile.py` avaliando particionamento do Kafka, ausência de perdas e deduplicação no banco sob concorrência.
5. **Consolidação Documental**: Revisão de todos os ADRs (`docs/architecture/decisions/`) e Skills (`docs/skills/`).

---

## 1. Contexto e Especificações Relacionadas

* **Specs**:
  * [`specs/api_v1.yaml`](../../specs/api_v1.yaml) (Endpoints `/metrics`, `/health/live`, `/health/ready` e headers de correlação).
  * [`specs/events/transaction_created.json`](../../specs/events/transaction_created.json) (Headers `traceparent` / `x-trace-id`).
* **Skills**:
  * [`docs/skills/observability_and_scaling.md`](../skills/observability_and_scaling.md)
  * [`docs/skills/idempotency.md`](../skills/idempotency.md)
  * [`docs/skills/retry_dlq.md`](../skills/retry_dlq.md)
  * [`docs/skills/outbox_pattern.md`](../skills/outbox_pattern.md)
* **ADRs**:
  * [`docs/architecture/decisions/001_transactional_outbox_pattern.adoc`](../architecture/decisions/001_transactional_outbox_pattern.adoc)
  * [`docs/architecture/decisions/002_idempotency_and_inbox_pattern.adoc`](../architecture/decisions/002_idempotency_and_inbox_pattern.adoc)
  * [`docs/architecture/decisions/003_non_blocking_retry_and_dlq.adoc`](../architecture/decisions/003_non_blocking_retry_and_dlq.adoc)
  * [`docs/architecture/decisions/004_hexagonal_architecture_domain_isolation.adoc`](../architecture/decisions/004_hexagonal_architecture_domain_isolation.adoc)
  * [`docs/architecture/decisions/005_observability_metrics_and_load_testing.adoc`](../architecture/decisions/005_observability_metrics_and_load_testing.adoc)

---

## 2. Escopo e Detalhamento da Implementação

### 2.1. OpenTelemetry & Logs Estruturados em JSON Correlacionados
1. **Configuração de OpenTelemetry (`src/app/infrastructure/observability/telemetry.py`)**:
   - Inicialização do `TracerProvider` e formatador de propagação **W3C TraceContext** (`TraceContextTextMapPropagator`).
   - Helpers para injeção e extração de Spans em headers HTTP e Kafka.
2. **Logging com Correlação e Sanitização PII (`src/app/infrastructure/observability/logging.py`)**:
   - Formatador JSON baseado em `python-json-logger` que extrai `trace_id` e `span_id` do span ativo do OpenTelemetry.
   - `DataMaskingFilter`: Filtro automático para mascarar dados sensíveis (CPF/CNPJ, tokens, senhas, e-mails e identificadores de clientes) em logs e records.
   - Suporte a contexto adicional (`transaction_id`, `event_id`, `customer_id`).
3. **Middleware FastAPI OpenTelemetry / Correlation**:
   - Extrai `traceparent` (ou `X-Correlation-ID`) de requisições HTTP recebidas, abre Span raiz de servidor e devolve `X-Correlation-ID` / `traceparent` na resposta.
4. **Propagação nos Kafka Workers**:
   - `OutboxPublisherWorker` injeta `traceparent` nos headers das mensagens publicadas no Kafka.
   - `TransactionConsumerWorker` e `RetryConsumerWorker` extraem `traceparent` dos headers da mensagem Kafka, criando um Span filho (Consumer Span) para o ciclo de processamento e análise de risco.

### 2.2. Métricas Prometheus e Health Probes
1. **Módulo Central de Métricas (`src/app/infrastructure/observability/metrics.py`)**:
   - `TRANSACTIONS_CREATED_TOTAL`: Counter `[status]` (contagem de transações criadas via API).
   - `TRANSACTIONS_PROCESSED_TOTAL`: Counter `[decision]` (APPROVED, REJECTED).
   - `KAFKA_MESSAGES_CONSUMED_TOTAL`: Counter `[topic, status]` (`processed`, `duplicate_skipped`, `retried`, `dlq`).
   - `CONSUMER_PROCESSING_DURATION_SECONDS`: Histogram `[topic]` (tempo de processamento por mensagem no worker).
   - `RISK_ANALYSIS_DURATION_SECONDS`: Histogram `[result]` (latência da chamada ao serviço de risco).
   - `RISK_ANALYSIS_ERRORS_TOTAL`: Counter `[error_type]` (transient, permanent).
   - `DLQ_MESSAGES_TOTAL`: Counter `[original_topic, reason]`.
2. **Endpoints de Monitoramento**:
   - `GET /metrics`: Retorna as métricas formatadas para coleta do Prometheus (`prometheus_client.generate_latest`).
   - `GET /health/live`: Liveness probe retornando `{"status": "ok"}` (HTTP 200).
   - `GET /health/ready`: Readiness probe executando ping ativo no MySQL (`SELECT 1`) e validando conectividade com o Kafka.

### 2.3. Simulação de Carga com Locust (10k eventos/minuto e Spike Test)
1. **Script de Teste de Performance e Carga (`src/tests/performance/locustfile.py`)**:
   - Centralizado dentro do ecossistema de testes (`src/tests/performance/`).
   - `TransactionLoadUser` (Locust HttpUser) simulando taxa de chegada sustentada de ~167 req/s (10.000 requisições/minuto).
   - `SpikeLoadShape`: Orquestrador de aumento abrupto em 3 estágios (aquecimento a ~100 req/min -> spike abrupto instantâneo para 10.000+ req/min a 100 usuários/s -> estabilização).
   - Tarefas com pesos realistas:
     - `create_transaction` (peso 70%): Envia `POST /transactions` com dados dinâmicos e cabeçalho `traceparent` / `X-Correlation-ID`.
     - `create_duplicate_transaction` (peso 15%): Reenvia a mesma mensagem/id para validar deduplicação do Inbox sob concorrência.
     - `get_transaction_status` (peso 15%): Consulta `GET /transactions/{id}` para acompanhar evolução de status.
   - Suporte a execução headless para automação CI e relatórios de percentis.

### 2.4. Documentação e Auditoria
1. Sincronização e auditoria completa dos documentos em `docs/` (`docs/skills/`, `docs/architecture/decisions/`, `docs/diagrams/`, `docs/plans/`).

---

## 3. Roteiro de Execução

| Passo | Componente | Arquivo(s) | Ação |
|---|---|---|---|
| **3.1** | OpenTelemetry & Logging | `src/app/infrastructure/observability/telemetry.py`, `src/app/infrastructure/observability/logging.py` | Configurar OTel TracerProvider, W3C propagator e JSON logger com `trace_id`/`span_id`. |
| **3.2** | Middleware API | `src/app/entrypoints/api/middlewares/correlation_middleware.py` | Criar middleware HTTP para extração/injeção de `traceparent` e `X-Correlation-ID`. |
| **3.3** | Módulo de Métricas | `src/app/infrastructure/observability/metrics.py` | Definir contadores, histogramas e registry Prometheus. |
| **3.4** | Rotas de Métricas & Health | `src/app/entrypoints/api/routes/metrics.py`, `src/app/entrypoints/api/routes/health.py` | Expor `/metrics`, `/health/live` e `/health/ready`. |
| **3.5** | Instrumentação nos Workers | `src/app/entrypoints/workers/*.py` | Adicionar propagação de Span OTel, contagem de métricas e vínculo de contexto de log. |
| **3.6** | Teste de Carga Locust | `src/tests/performance/locustfile.py` | Criar suite de teste de performance/carga para 10k eventos/min e validação de concorrência. |
| **3.7** | Testes Automatizados | `src/tests/unit/test_observability.py`, `src/tests/integration/test_health_and_metrics.py` | Testar OpenTelemetry Spans, logs JSON, `/metrics` e health checks. |
| **3.8** | Revisão Documental | `docs/` | Validar conformidade de ADRs e Skills. |

---

## 4. Critérios de Aceite para Aprovação

- [ ] OpenTelemetry configurado com padrão W3C TraceContext (`traceparent`) propagado entre HTTP, Outbox e Workers.
- [ ] Logs estruturados em JSON com injeção automática de `trace_id` e `span_id`.
- [ ] Endpoints `/metrics`, `/health/live` e `/health/ready` ativos e cobertos por testes automatizados.
- [ ] Contadores de transações, erros e histogramas de latência instrumentados via Prometheus.
- [ ] Suite `src/tests/performance/locustfile.py` pronta para teste de estresse de 10k eventos/minuto com cenários de duplicatas.
- [ ] Documentação técnica e ADRs consolidados em `docs/`.
- [ ] 100% de testes passando no `pytest` e conformidade total com `ruff`.
- [ ] Apresentação dos commits em grupos estruturados para aprovação antes da execução do Git.
