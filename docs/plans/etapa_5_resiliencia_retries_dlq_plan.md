---
title: "Plano de Implementação: Etapa 5 — Resiliência Avançada, Retries Kafka e DLQ Replay"
type: "plan"
phase: "completed"
status: "done"
skills_required:
  - "docs/skills/retry_dlq.md"
  - "docs/skills/idempotency.md"
tools_required:
  - "confluent-kafka"
  - "sqlalchemy"
  - "httpx2"
  - "tenacity"
  - "pytest"
  - "pytest-mock"
  - "ruff"
dependencies:
  - "docs/plans/etapa_1_contratos_modelagem_plan.md"
  - "docs/plans/etapa_2_api_basica_persistencia_plan.md"
  - "docs/plans/etapa_3_transactional_outbox_plan.md"
  - "docs/plans/etapa_4_consumidor_idempotente_plan.md"
acceptance_criteria:
  - "Worker dedicado de Retry Consumer (`RetryConsumerWorker`) consumindo topic `transactions.retry.v1` e respeitando o timestamp `x-next-retry-timestamp` antes de reprocessar para evitar busy-wait."
  - "Se a mensagem na fila de retry ainda não atingiu o horário agendado (`now < next_retry_timestamp`), aplicar delay proporcional sem travar o worker ou pausar partições com `consumer.pause()` / `resume()`."
  - "Utilitário CLI operacional de Reprocessamento de DLQ (`src/app/entrypoints/cli/dlq_replay.py`): permite inspecionar mensagens em `transactions.dlq.v1` e reenviar para `transactions.created.v1` com reset de retries e regeneração/reinicialização de status no Inbox."
  - "Cenário de Falha 2 (Falha transitória na análise de risco) e Cenário de Falha 3 (Indisponibilidade prolongada de 30 minutos) completamente testados e documentados."
  - "Validação de Carga Concorrente e Stress Test: simulação de alto volume de mensagens concorrentes com reentregas e duplicatas artificiais, garantindo que o banco de dados MySQL mantenha exatamente 1 registro processado por transação e zero perdas."
  - "Garantia de que transações de outros clientes continuam sendo consumidas normalmente sem Head-of-Line Blocking enquanto transações problemáticas estão na fila de retry/DLQ."
  - "Cobertura automatizada via pytest com 100% de aprovação e validação com `ruff`."
created_at: "2026-10-01"
updated_at: "2026-10-01"
---

# Implementation Plan: Etapa 5 — Resiliência Avançada, Retries Kafka e DLQ Replay

Este documento define o plano de implementação da **Etapa 5**, aprofundando os mecanismos de resiliência assíncrona, o consumo autônomo do tópico de retry (`transactions.retry.v1`), o isolamento definitivo na Dead Letter Queue (`transactions.dlq.v1`), o ferramental operacional de reprocessamento manual/programático (**DLQ Replay**) e a validação formal de integridade **sob concorrência e carga massiva (zero perda e zero duplicação)**.

---

## 1. Contexto e Especificações Relacionadas

- **Architecture Decision Record (ADR):** [`docs/architecture/decisions/003_non_blocking_retry_and_dlq.adoc`](../architecture/decisions/003_non_blocking_retry_and_dlq.adoc)
- **Skills de Resiliência:**
  - [`docs/skills/retry_dlq.md`](../skills/retry_dlq.md)
  - [`docs/skills/idempotency.md`](../skills/idempotency.md)
- **Diagrama de Sequência PlantUML:** [`docs/diagrams/consumer_retry_dlq_sequence.puml`](../diagrams/consumer_retry_dlq_sequence.puml)
- **Diagrama Arquitetural de Containers:** [`docs/diagrams/architecture_overview.puml`](../diagrams/architecture_overview.puml)

---

## 2. Componentes e Estrutura de Arquivos a Implementar

```
src/app/
├── entrypoints/
│   ├── cli/
│   │   ├── __init__.py
│   │   └── dlq_replay.py                     # CLI/Script para inspeção e replay de mensagens da DLQ
│   └── workers/
│       ├── retry_consumer_worker.py          # Daemon dedicado ao consumo do tópico transactions.retry.v1
│       └── __init__.py
│
├── infrastructure/
│   └── messaging/
│       └── dlq_manager.py                    # Gerenciador de leitura, contagem e reenvio de mensagens DLQ

src/tests/
├── unit/
│   └── test_dlq_manager.py                   # Testes unitários do mecanismo de replay e manipulação de headers
└── integration/
    ├── test_retry_consumer_worker.py         # Teste do loop do worker de retry com controle de timestamp
    ├── test_dlq_replay_cli.py                # Teste de ponta a ponta do replay de mensagens da DLQ
    ├── test_resilience_failure_scenarios.py  # Testes dos Cenários de Falha 2 e 3 (queda prolongada de 30 min)
    └── test_concurrent_load_and_idempotency.py # Teste de estresse/carga concorrente (zero perda / zero duplicação)
```

---

## 3. Roteiro Passo a Passo de Execução

### Passo 1: Implementação do Worker Dedicado de Retry (`RetryConsumerWorker`)
- Criar `RetryConsumerWorker` (`src/app/entrypoints/workers/retry_consumer_worker.py`) subscrito em `transactions.retry.v1`.
- **Estratégia de Controle Temporal de Reprocessamento:**
  - O worker lê a mensagem do tópico de retry e extrai o header `x-next-retry-timestamp`.
  - Se `datetime.utcnow() < next_retry_timestamp`:
    - Calcula o tempo restante (`delay = (next_retry_timestamp - now).total_seconds()`).
    - Se o delay for curto (ex: $\le 2$ segundos), realiza `time.sleep(delay)`.
    - Se o delay for maior, utiliza pausa controlada de partição no Kafka (`consumer.pause()`), aguarda o tempo necessário e despausa (`consumer.resume()`), preservando o offset sem sobrecarregar a CPU.
  - Se `now >= next_retry_timestamp`: invoca o `ProcessRiskAnalysisUseCase` ou reencaminha para o próximo ciclo de retry / DLQ.

### Passo 2: Gerenciador de DLQ e Utilitário de Replay (`DLQManager` e CLI)
- Criar `DLQManager` (`src/app/infrastructure/messaging/dlq_manager.py`):
  - `inspect_messages(limit: int) -> list[DLQMessageInfo]`: Permite consultar mensagens retidas na DLQ sem comitar seus offsets.
  - `replay_message(message_id: str, target_topic: str) -> bool`: Lê a mensagem da DLQ, remove headers de falha (`x-exception-*`), zera `x-retry-count`, gera novo `event_id` (ou reinicia o status do `event_id` no `inbox_events` para `PROCESSING`) e republica no tópico alvo (`transactions.created.v1`).
- Criar o entrypoint de CLI `src/app/entrypoints/cli/dlq_replay.py` executável via linha de comando:
  - `python -m app.entrypoints.cli.dlq_replay --list`
  - `python -m app.entrypoints.cli.dlq_replay --replay-all --target transactions.created.v1`

### Passo 3: Validação Completa dos Cenários de Falha

#### A. Cenário de Falha 2 (Falha Transitória no Serviço de Risco):
- **Ação:** API de risco responde com erro 503 / Timeout em 1 ou 2 requisições consecutivas.
- **Validação:** A transação vai para `transactions.retry.v1` com `x-retry-count: 1`. O `RetryConsumerWorker` aguarda o timestamp calculado, reexecuta quando a API de risco normaliza e a transação transiciona para `APPROVED` com emissão de evento na Outbox.

#### B. Cenário de Falha 3 (Indisponibilidade Prolongada — ex: 30 minutos fora do ar):
- **Ação:** Simular o serviço de risco completamente inacessível durante todo o ciclo de retries (tentativas 1, 2 e 3).
- **Validação:**
  1. A mensagem transita `created.v1` -> `retry.v1 (tentativa 1)` -> `retry.v1 (tentativa 2)` -> `retry.v1 (tentativa 3)` -> `transactions.dlq.v1`.
  2. No banco de dados MySQL, a transação assume status `FAILED` com `failure_reason = "Limite máximo de retries excedido"`.
  3. Enquanto essa transação sofre retries com backoff exponencial, **outras transações de clientes sem problemas continuam sendo criadas, avaliadas e aprovadas instantaneamente** (zero Head-of-Line Blocking).
  4. Quando o serviço externo se restabelece, a execução da CLI de replay recupera a mensagem da DLQ e a reprocessa com sucesso.

#### C. Cenário de Carga Concorrente e Integridade (Zero Perda / Zero Duplicação):
- **Ação (`test_concurrent_load_and_idempotency.py`):**
  - Disparar $N$ threads concorrentes (ex: 50 workers virtuais) consumindo um lote de $M$ transações (ex: 200 mensagens) onde 30% são réplicas intencionais/duplicadas com o mesmo `event_id`.
  - Injetar atrasos artificiais na camada HTTP do serviço de risco para forçar concorrência e chaveamento de locks no MySQL.
- **Validação Estrita:**
  1. **Zero Duplicação:** Cada transação no MySQL possui exatamente 1 registro correspondente em `inbox_events` e 1 estado final em `transactions`.
  2. **Zero Perda:** Todas as mensagens únicas submetidas terminam em um estado final determinístico (`APPROVED`, `REJECTED` ou `DLQ`).
  3. **Idempotência de Efeitos Colaterais:** A contagem total de chamadas à API externa de risco é estritamente igual à quantidade de eventos únicos recebidos, comprovando que nenhuma duplicata acionou o serviço externo.

---

## 4. Ordem de Execução e Diretrizes de Qualidade

1. Submeter este plano para revisão e aprovação do usuário (`status: approved`).
2. Implementar `RetryConsumerWorker` com gestão temporal de polling.
3. Implementar `DLQManager` e o script CLI `dlq_replay.py`.
4. Criar a suíte de testes de integração para `RetryConsumerWorker`, `DLQManager` e simulação do Cenário de Falha 3.
5. Executar `pipenv run ruff check --fix . && pipenv run ruff format .` e `pipenv run pytest`.
6. Apresentar os resultados ao usuário e sugerir commits em grupos atômicos no padrão Conventional Commits.
