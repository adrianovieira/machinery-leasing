---
title: "Plano de Implementação: Etapa 4 — Consumidor Idempotente e Inbox Pattern"
type: "plan"
phase: "completed"
status: "done"
skills_required:
  - "docs/skills/idempotency.md"
  - "docs/skills/retry_dlq.md"
tools_required:
  - "sqlalchemy"
  - "alembic"
  - "confluent-kafka"
  - "httpx2"
  - "tenacity"
  - "pytest"
  - "pytest-mock"
  - "ruff"
dependencies:
  - "docs/plans/etapa_1_contratos_modelagem_plan.md"
  - "docs/plans/etapa_2_api_basica_persistencia_plan.md"
  - "docs/plans/etapa_3_transactional_outbox_plan.md"
acceptance_criteria:
  - "Tabela `inbox_events` versionada e criada exclusivamente via Alembic migration, contendo chaves, índices (`consumer_group`, `status`, `processed_at`) e enum de status (`PROCESSING`, `COMPLETED`, `RETRYING`, `FAILED`)."
  - "Model SQLAlchemy `InboxEventModel` e entidade de domínio `InboxEvent` mapeados com integridade relacional."
  - "Consumidor Kafka implementado sob a estratégia Inbox Pattern com barreira estrita de concorrência, chave primária (`event_id`) e auto-commit expressamente desativado (`enable.auto.commit=False`), garantindo que o offset seja avançado somente via commit manual síncrono."
  - "Tratamento determinístico de mensagens duplicadas: se o `event_id` já existir no `inbox_events`, o processamento de efeitos colaterais e chamada externa são ignorados e o offset Kafka é confirmado."
  - "Integração de Resiliência com `tenacity`: `HttpRiskClientAdapter` utiliza decorators do `tenacity` com retry síncrono (ex: 2 tentativas rápidas para falhas de rede efêmeras / socket glitch) antes de escalar a falha."
  - "Caso de uso `ProcessRiskAnalysisUseCase` gerenciando transições de estado da transação (`PROCESSING` -> `APPROVED` ou `REJECTED`) e registrando evento de status na Outbox na mesma transação atômica do MySQL."
  - "Ordem de commit inegociável: o offset do Kafka só é confirmado após a confirmação (`commit`) definitiva no MySQL."
  - "Estratégia de retries não-bloqueante via Kafka (`transactions.retry.v1`) com cálculo de backoff exponencial e jitter quando o serviço externo persistir instável além dos retries do tenacity."
  - "Roteamento determinístico para Dead Letter Queue (`transactions.dlq.v1`) com headers de diagnóstico (`x-exception-type`, `x-retry-count`) após esgotamento das tentativas máximas de retry."
  - "Suíte completa de testes unitários e de integração validando: consumo idempotente, descarte de duplicatas, retries rápidos via tenacity, retries não-bloqueantes via Kafka, encaminhamento para DLQ e garantia de ordem de commits."
created_at: "2026-10-01"
updated_at: "2026-10-01"
---

# Implementation Plan: Etapa 4 — Consumidor Idempotente e Inbox Pattern

Este documento define o roteiro detalhado para a implementação do **Consumidor Kafka Idempotente**, do padrão **Inbox Pattern**, da integração com o serviço externo de análise de risco e da estratégia de **Retry com Backoff Exponencial / DLQ**.

---

## 1. Contexto e Especificações Relacionadas

- **Especificações de Eventos:**
  - [`specs/events/transaction_created.json`](../../specs/events/transaction_created.json)
  - [`specs/events/transaction_status_changed.json`](../../specs/events/transaction_status_changed.json)
- **Skills de Negócio e Resiliência:**
  - [`docs/skills/idempotency.md`](../skills/idempotency.md)
  - [`docs/skills/retry_dlq.md`](../skills/retry_dlq.md)
- **Diagrama de Sequência:** [`docs/diagrams/consumer_retry_dlq_sequence.puml`](../diagrams/consumer_retry_dlq_sequence.puml)
- **Diagrama Arquitetural Geral:** [`docs/diagrams/architecture_overview.puml`](../diagrams/architecture_overview.puml)

---

## 2. Componentes e Estrutura de Arquivos a Implementar

```
alembic/
└── versions/
    └── <revision_id>_create_inbox_events_table.py # Migration Alembic da tabela inbox_events

src/app/
├── domain/
│   ├── entities/
│   │   ├── inbox_event.py                    # Entidade de domínio InboxEvent
│   │   └── risk_evaluation.py                # Value Object / Entidade com resultado da análise de risco
│   └── ports/
│       ├── inbox_repository_port.py          # Porta para barreira e persistência do Inbox
│       └── external_risk_client_port.py      # Porta para integração com serviço de risco
│
├── application/
│   └── use_cases/
│       └── process_risk_analysis_use_case.py # Orquestração: transição de status, regra de risco e outbox
│
├── infrastructure/
│   ├── database/
│   │   └── models/
│   │       └── inbox_event_model.py          # Modelo SQLAlchemy para inbox_events
│   ├── repositories/
│   │   └── mysql_inbox_repository.py         # Repositório MySQL com trava atômica de duplicação
│   ├── external_services/
│   │   └── http_risk_client_adapter.py       # Adaptador HTTP (httpx) resiliente com timeout
│   └── messaging/
│       ├── kafka_consumer_adapter.py         # Adaptador Confluent Kafka Consumer (manual commit)
│       └── retry_router.py                   # Roteamento e cálculo de backoff para retry.v1 / dlq.v1
│
└── entrypoints/
    └── workers/
        └── transaction_consumer_worker.py    # Daemon worker consumidor de mensagens

src/tests/
├── unit/
│   ├── test_inbox_event.py                   # Testes unitários da entidade e estados do inbox
│   ├── test_process_risk_analysis_use_case.py# Testes unitários do caso de uso com mocks
│   ├── test_retry_backoff_calculator.py      # Testes de backoff exponencial e cálculo de jitter
│   └── test_http_risk_client.py              # Testes do cliente HTTP com responses simuladas
└── integration/
    ├── test_idempotent_consumer_integration.py # Teste de duplicação de mensagens (idempotência)
    ├── test_consumer_retry_and_dlq.py        # Teste de roteamento de falhas transitórias e DLQ
    └── test_consumer_mysql_commit_order.py   # Teste da ordem estrita: MySQL commit antes de Kafka offset
```

---

## 3. Roteiro Passo a Passo de Execução

### Passo 1: Modelagem do Inbox e Geração de Migration via Alembic (`inbox_events`)
- Implementar `InboxEventModel` (`src/app/infrastructure/database/models/inbox_event_model.py`) e entidade de domínio `InboxEvent`:
  - `event_id`: VARCHAR(36) PRIMARY KEY
  - `consumer_group`: VARCHAR(64) NOT NULL
  - `status`: ENUM('PROCESSING', 'COMPLETED', 'RETRYING', 'FAILED', name='inbox_status_enum') NOT NULL DEFAULT 'PROCESSING'
  - `received_at`: TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
  - `processed_at`: TIMESTAMP NULL
  - `error_message`: TEXT NULL
  - Índices: `idx_inbox_status` (`status`), `idx_inbox_consumer_group` (`consumer_group`).
- Registrar `InboxEventModel` em `alembic/env.py`.
- **Gerar a migration exclusivamente pelo Alembic**:
  - `pipenv run alembic revision --autogenerate -m "create_inbox_events_table"`
  - Validar e aplicar: `pipenv run alembic upgrade head`.

### Passo 2: Implementação do Inbox Repository e Portas de Domínio
- Criar a interface `InboxRepositoryPort` com métodos para:
  - `try_acquire(event_id, consumer_group) -> bool`: Executa inserção com status `PROCESSING`. Retorna `False` se capturar `IntegrityError` (duplicidade).
  - `mark_as_completed(event_id, session)`: Atualiza status para `COMPLETED` e preenche `processed_at`.
  - `mark_as_failed(event_id, error_message, session)`: Atualiza status para `FAILED` ou `RETRYING`.
- Implementar `MySQLInboxRepository`.

### Passo 3: Adaptador de Serviço Externo de Análise de Risco com Resiliência Síncrona (`tenacity`)
- Definir `ExternalRiskClientPort` com o contrato `evaluate(customer_id: str, value: Decimal) -> RiskEvaluation`.
- Implementar `HttpRiskClientAdapter` via `httpx` integrado à biblioteca `tenacity`:
  - **Uso do Tenacity para Retries Rápidos/Locais:**
    - Decorador `@retry` com `stop_after_attempt(2)` e `wait_exponential_jitter(initial=0.2, max=1.0)` exclusivamente para falhas transitórias instantâneas de conexão (ex: `httpx.ConnectTimeout`, `httpx.NetworkError`, `httpx.RemoteProtocolError`).
    - Não fazer retry no Tenacity para erros permanentes (`HTTP 400 Bad Request`, `HTTP 422`).
  - Timeout estrito configurável (ex: 5s por tentativa).
  - Se os retries rápidos locais do Tenacity se esgotarem (indicando indisponibilidade prolongada do serviço externo), lança `TransientRiskServiceException` para acionar a estratégia de Retry Assíncrono Não-Bloqueante via Kafka.

### Passo 4: Caso de Uso de Processamento da Análise de Risco
- Implementar `ProcessRiskAnalysisUseCase`:
  - Atualiza status da transação para `PROCESSING`.
  - Invoca o cliente de risco.
  - Se aprovado: status transiciona para `APPROVED`; se reprovado: `REJECTED`.
  - Emite evento `TransactionStatusChanged` persistido na tabela `outbox_events` na mesma transação MySQL que atualiza a transação e o `inbox_events` para `COMPLETED`.

### Passo 5: Consumer Worker, Idempotência e Roteador de Retries / DLQ
- Implementar `KafkaConsumerAdapter` com leitura contínua e sem auto-commit (`enable.auto.commit=False`).
- Implementar `TransactionConsumerWorker` e módulo `RetryRouter`:
  - Extrai `event_id` do header `x-event-id` ou payload.
  - Verifica barreira no `InboxRepository`: se já processado/duplicado, faz log estruturado, comita offset no Kafka e encerra sem side-effects.
  - Se for nova mensagem: executa `ProcessRiskAnalysisUseCase`.
  - **Tratamento de Exceções e Resiliência em 2 Níveis:**
    1. **Nível 1 (Síncrono / Tenacity):** Resolvido dentro do `HttpRiskClientAdapter` para glitches de rede sub-segundo.
    2. **Nível 2 (Assíncrono / Non-Blocking Retry via Kafka):** Se o erro persistir (`TransientRiskServiceException`) e `retry_count < MAX_RETRIES` (ex: 3 tentativas):
       - Transação transiciona para `RETRYING` no MySQL.
       - Mensagem é publicada no tópico `transactions.retry.v1` com headers `x-retry-count: N+1` e `x-next-retry-timestamp` calculado via backoff exponencial com jitter (`base=2s`, `max=60s`).
       - Comita o offset da mensagem no tópico original para não travar a fila principal (*Head-of-Line Blocking*).
    3. **Nível 3 (Isolamento em DLQ):** Se `retry_count >= MAX_RETRIES` ou se for um erro fatal/poison pill (`PermanentRiskServiceException`):
       - Transação e `inbox_events` são atualizados para status `FAILED` com `failure_reason` descritivo no MySQL.
       - Mensagem é encaminhada para `transactions.dlq.v1` contendo os headers `x-retry-count`, `x-exception-type` e `x-exception-message`.
       - Emite log estruturado de nível `ERROR` e comita o offset no Kafka.
  - **Garantia de Ordem**: O `consumer.commit(msg)` é invocado estritamente após a persistência bem-sucedida no MySQL.

### Passo 6: Suíte de Testes Automatizados e Testes de Cenário

#### A. Testes Unitários:
- Idempotência: comportamento do repositório Inbox ao encontrar chave duplicada (lançamento de erro/retorno booleano controlado).
- Caso de uso de risco (`ProcessRiskAnalysisUseCase`) com simulações de aprovação, reprovação e tratamento de exceções.
- Algoritmo de backoff exponencial e cálculo de jitter (`test_retry_backoff_calculator.py`).
- Adaptador HTTP de risco com simulação de payloads válidos, HTTP 500, HTTP 504 e timeouts.

#### B. Definição Explícita de Testes de Cenário (Integração / Resiliência):

1. **Cenário 1 — Teste de Duplicidade (Idempotência Estrita):**
   - **Ação:** Publicar/Consumir a **mesma mensagem Kafka 2 vezes** com o mesmo `event_id` e payload idêntico.
   - **Expectativa:**
     - A 1ª mensagem insere o registro em `inbox_events`, executa a chamada ao serviço de risco e atualiza o MySQL.
     - A 2ª mensagem bate na barreira de chave primária (`event_id`), faz log estruturado de duplicidade (`"Mensagem duplicada detectada..."`), descarta a reexecução (sem chamar o serviço externo de risco pela 2ª vez) e comita o offset no Kafka.
     - **Verificação no DB:** Exatamente **1 registro** em `inbox_events` e 1 registro em `transactions` com o estado final correto.

2. **Cenário 2 — Teste de Retry (Falha Transitória e Backoff):**
   - **Ação:** Simular indisponibilidade temporária do serviço externo de risco (HTTP 503 / Timeout) na tentativa 1 (`retry_count = 0`).
   - **Expectativa:**
     - O worker captura `TransientRiskServiceException`.
     - Atualiza status da transação para `RETRYING` no MySQL.
     - Registra logs estruturados com nível `WARNING` indicando agendamento de retry com delay calculado.
     - Publica a mensagem no tópico `transactions.retry.v1` com os headers `x-retry-count: 1` e `x-next-retry-timestamp`.
     - Confirma o offset no Kafka da mensagem original sem travar a partição principal.

3. **Cenário 3 — Teste de DLQ (Esgotamento de Retries / Poison Pill):**
   - **Ação:** Simular falha persistente do serviço de risco atingindo o limite de tentativas (`retry_count >= MAX_RETRIES`, ex: 3 ou 5 tentativas) ou payload irrecuperável (*poison pill*).
   - **Expectativa:**
     - O worker detecta o esgotamento de retries.
     - Atualiza o status da transação para `FAILED` com `failure_reason` preenchido e atualiza `inbox_events` para `FAILED`.
     - Publica a mensagem no tópico `transactions.dlq.v1` contendo os headers `x-retry-count`, `x-exception-type` e `x-exception-message`.
     - Registra log de erro estruturado (`ERROR`) de descarte para DLQ e confirma o offset no Kafka.
     - **Verificação no Kafka:** Mensagem presente no tópico `transactions.dlq.v1`.

4. **Cenário 4 — Teste de Garantia de Ordem Transacional (MySQL vs Kafka Offset):**
   - **Ação:** Simular falha/queda no MySQL durante o commit da transação.
   - **Expectativa:**
     - O offset no Kafka **não é confirmado** (`consumer.commit()` não é chamado).
     - Ao reiniciar o consumidor, a mensagem é reprocessada deterministamente.

---

## 4. Ordem de Execução e Diretrizes de Qualidade

1. Submeter o plano para revisão e aprovação do usuário (`status: approved`).
2. Gerar migration do `inbox_events` via Alembic.
3. Desenvolver entidades, portas e repositórios do Inbox.
4. Desenvolver adaptador de cliente de risco HTTP e caso de uso.
5. Desenvolver o worker do consumidor e módulo de retries / DLQ.
6. Executar suíte completa de testes com `pytest` e validações de lint com `ruff check --fix . && ruff format .`.
7. Apresentar os resultados e propor mensagens de commit convencionais para aprovação.
