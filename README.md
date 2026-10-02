# Plataforma de Processamento Assíncrono de Leasing de Máquinas

Sistema de alta performance, resiliência e integridade financeira para recepção, validação e análise de risco assíncrona de propostas de leasing de máquinas pesadas.

Projetado sob os princípios da **Arquitetura Hexagonal (Ports & Adapters)** e dos padrões de resiliência distribuída:

- **Transactional Outbox**: Consistência atômica entre MySQL e Apache Kafka sem _dual-write_.
- **Inbox Pattern (Idempotência)**: Deduplicação e barreira relacional contra reprocessamento.
- **Non-Blocking Retry & DLQ**: Backoff exponencial com jitter e isolamento de falhas sem travar partições (_Head-of-Line Blocking_).
- **Observabilidade & OpenTelemetry**: Rastreabilidade ponta a ponta via W3C TraceContext (`traceparent`), logs em JSON estruturados com mascaramento de dados sensíveis (LGPD/PII) e métricas Prometheus.

---

## 🏛️ Arquitetura e Decisões Técnicas (ADRs)

A documentação arquitetural completa está localizada em `docs/`:

- **Visão Geral e Diagramas C4 / PlantUML**: [`docs/architecture/system_design.adoc`](docs/architecture/system_design.adoc)
- **ADR 001 - Transactional Outbox**: [`docs/architecture/decisions/001_transactional_outbox_pattern.adoc`](docs/architecture/decisions/001_transactional_outbox_pattern.adoc)
- **ADR 002 - Idempotência e Inbox Pattern**: [`docs/architecture/decisions/002_idempotency_and_inbox_pattern.adoc`](docs/architecture/decisions/002_idempotency_and_inbox_pattern.adoc)
- **ADR 003 - Non-Blocking Retry e DLQ**: [`docs/architecture/decisions/003_non_blocking_retry_and_dlq.adoc`](docs/architecture/decisions/003_non_blocking_retry_and_dlq.adoc)
- **ADR 004 - Arquitetura Hexagonal e Isolamento de Domínio**: [`docs/architecture/decisions/004_hexagonal_architecture_domain_isolation.adoc`](docs/architecture/decisions/004_hexagonal_architecture_domain_isolation.adoc)
- **ADR 005 - Observabilidade e Carga**: [`docs/architecture/decisions/005_observability_metrics_and_load_testing.adoc`](docs/architecture/decisions/005_observability_metrics_and_load_testing.adoc)

---

## 🚀 Tecnologias Utilizadas

- **Linguagem & Runtime**: Python 3.11+
- **Framework Web**: FastAPI + Uvicorn
- **Banco de Dados Relacional**: MySQL 8.0 (Fonte primária da verdade)
- **ORM & Migrations**: SQLAlchemy 2.0 + Alembic
- **Mensageria Distribuída**: Apache Kafka 3.8 (Modo KRaft) + `confluent-kafka`
- **Resiliência Síncrona**: `tenacity` + `httpx`
- **Observabilidade**: OpenTelemetry SDK + `prometheus-client` + `python-json-logger`
- **Qualidade & Testes**: Pytest, Pytest-Mock, Pytest-Cov, Locust, Ruff e Pipenv

---

## ⚙️ Pré-requisitos e Instalação

1. **Docker e Docker Compose** instalados.
2. **Python 3.11+** e **Pipenv** instalados.

### 1. Clonar e Instalar Dependências

```bash
# Instalar dependências de produção e desenvolvimento
pipenv install --dev
```

### 2. Executar Toda a Plataforma via Docker Compose (Recomendado)

Os serviços da aplicação e workers estão agrupados sob o profile `app`. Você pode subir toda a stack com:

```bash
# Constrói e inicializa todos os containers (infraestrutura + aplicação com profile 'app')
docker compose --profile app up --build -d

# Visualizar status de todos os serviços
docker compose --profile app ps

# Acompanhar logs integrados dos workers e da API
docker compose --profile app logs -f api worker-outbox worker-transaction-consumer worker-retry-consumer
```

### 3. Modo Desenvolvimento Local (Apenas Infraestrutura)

Se preferir executar apenas a infraestrutura básica (MySQL, Kafka, Redis) e rodar a aplicação localmente via `pipenv`:

```bash
# 1. Subir apenas a infraestrutura padrão (sem o profile app)
docker compose up -d

# 2. Validar saúde dos serviços
pipenv run python scripts/check_infra.py

# 3. Aplicar migrações do banco
pipenv run alembic upgrade head
```

---

## 🏃 Como Executar a Aplicação

### 1. Iniciar a API HTTP (FastAPI)

```bash
pipenv run uvicorn src.app.entrypoints.api.main:app --host 0.0.0.0 --port 8000 --reload
```

Acesse a documentação interativa OpenAPI em: `http://localhost:8000/docs`

### 2. Iniciar os Workers Assíncronos

Abra terminais dedicados para cada worker (ou execute como daemons):

```bash
# 1. Worker Publicador da Outbox (MySQL -> Kafka)
pipenv run python -m app.entrypoints.workers.outbox_publisher_worker

# 2. Worker Consumidor Principal (Idempotente + Análise de Risco)
pipenv run python -m app.entrypoints.workers.transaction_consumer_worker

# 3. Worker Consumidor de Retries (Backoff Exponencial não-bloqueante)
pipenv run python -m app.entrypoints.workers.retry_consumer_worker
```

---

## 🛠️ Endpoints Principais e Monitoramento

### API de Transações

- `POST /api/v1/transactions`: Submete nova proposta de leasing.
- `GET /api/v1/transactions/{id}`: Consulta dados e status atual da transação.

### Observabilidade e Saúde

- `GET /health/live`: Liveness probe (HTTP 200).
- `GET /health/ready`: Readiness probe verificando conexões ativas com MySQL e Kafka.
- `GET /metrics`: Métricas no formato padrão do Prometheus.

---

## 🔁 Gestão e Replay da Dead Letter Queue (DLQ CLI)

A plataforma conta com uma interface CLI para auditoria e recuperação de mensagens que esgotaram tentativas de retry:

```bash
# Listar mensagens atualmente na DLQ
pipenv run python -m app.entrypoints.cli.dlq_replay --list

# Reenviar todas as mensagens da DLQ para a fila principal (com reset de status no Inbox)
pipenv run python -m app.entrypoints.cli.dlq_replay --replay-all --target-topic transactions.created.v1
```

---

## 🧪 Suíte de Testes Automatizados e Performance

### Executar Testes Unitários, de Integração e Contrato

```bash
# Execução da suíte completa de testes
pipenv run pytest

# Execução com relatório de cobertura de código
pipenv run pytest --cov=src/app --cov-report=term-missing
```

### Executar Teste de Carga e Stress (Locust — 10.000 req/min)

```bash
# Modo interativo (Interface Web em http://localhost:8089)
pipenv run locust -f src/tests/performance/locustfile.py

# Modo Headless automatizado
pipenv run locust -f src/tests/performance/locustfile.py --headless -u 100 -r 20 --run-time 1m --host http://localhost:8000
```

### Formatação e Linting

```bash
pipenv run ruff check --fix .
pipenv run ruff format .
```

---

## 👥 Autores

- Agente de IA específico para desenvolvimento de software
- **Revisão, supervisão e arquitetura**:
  - **Adriano Vieira**
