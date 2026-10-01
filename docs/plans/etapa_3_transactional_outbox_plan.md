---
title: "Plano de Implementação: Etapa 3 — Transactional Outbox Pattern"
type: "plan"
phase: "completed"
status: "done"

skills_required:
  - "docs/skills/outbox_pattern.md"
tools_required:
  - "sqlalchemy"
  - "alembic"
  - "confluent-kafka"
  - "pytest"
  - "pytest-mock"
  - "ruff"
dependencies:
  - "docs/plans/etapa_1_contratos_modelagem_plan.md"
  - "docs/plans/etapa_2_api_basica_persistencia_plan.md"
acceptance_criteria:
  - "Tabela `outbox_events` versionada e criada exclusivamente via Alembic migration.
  - "Migration testada e validada no MySQL com suporte aos tipos `JSON`, `ENUM('PENDING', 'PUBLISHED', 'FAILED')` e índices compostos (`status`, `created_at`)."
  - "Model SQLAlchemy `OutboxEventModel` mapeado em perfeita paridade com o schema gerado pelo Alembic."
  - "Caso de uso `CreateTransactionUseCase` e repositório `MySQLTransactionRepository` gravam a transação e o evento outbox na mesma transação atômica do MySQL (commit único)."
  - "Se a persistência no banco falhar ou sofrer rollback, nenhum evento outbox é persistido."
  - "Worker desacoplado `OutboxPublisherWorker` realiza polling resiliente com `with_for_update(skip_locked=True)` para processar lotes de eventos `PENDING`."
  - "Publicação no Kafka no tópico `transactions.created.v1` com `key=aggregate_id`, `headers` de rastreabilidade (`x-event-id`) e confirmação de entrega estrita (`acks=all` / `flush`)."
  - "Após confirmação do Kafka, o status do evento na outbox é atualizado para `PUBLISHED` com timestamp `published_at`."
  - "Cenário de Falha 1 (Queda do Kafka) testado: se o broker estiver indisponível, a API continua aceitando requisições e gravando a transação + outbox como `PENDING`; o worker realiza retries controlados sem perder eventos."
  - "Testes unitários e de integração validando 100% dos fluxos de escrita atômica, migração Alembic, polling concorrente seguro e publicação."
created_at: "2026-10-01"
updated_at: "2026-10-01"
---

# Implementation Plan: Etapa 3 — Transactional Outbox (Atomicidade e Worker de Publicação Kafka)

Este documento define o roteiro detalhado para a implementação do padrão **Transactional Outbox**, garantindo consistência transacional atômica entre o MySQL e o Apache Kafka sem risco de perda de eventos ou dual-write inconsistente.

---

## 1. Contexto e Especificações Relacionadas

- **Especificação de Eventos:** [`specs/events/transaction_created.json`](../../specs/events/transaction_created.json)
- **Skill de Negócio:** [`docs/skills/outbox_pattern.md`](../skills/outbox_pattern.md)
- **Diagrama Arquitetural de Sequência:** [`docs/diagrams/transaction_outbox_sequence.puml`](../diagrams/transaction_outbox_sequence.puml)
- **ADR de Referência:** [`docs/architecture/decisions/001_transactional_outbox_pattern.adoc`](../architecture/decisions/001_transactional_outbox_pattern.adoc)

---

## 2. Componentes e Estrutura de Arquivos a Implementar

```
alembic/
└── versions/
    └── 4d1baf4ed54e_create_outbox_events_table.py  # Migration da tabela outbox_events gerada pelo Alembic


src/app/
├── domain/
│   ├── entities/
│   │   └── outbox_event.py                # Entidade pura de domínio para eventos outbox
│   └── ports/
│       ├── outbox_repository_port.py      # Porta para persistência/consulta de outbox
│       └── message_producer_port.py       # Porta para publicação de mensagens no broker
│
├── infrastructure/
│   ├── database/
│   │   └── models/
│   │       └── outbox_event_model.py      # Modelo SQLAlchemy com status, payload JSON e índices
│   ├── repositories/
│   │   └── mysql_outbox_repository.py     # Repositório com suporte a SKIP LOCKED e locking otimizado
│   └── messaging/
│       ├── kafka_producer_adapter.py      # Adaptador concreto do Producer Kafka (confluent-kafka)
│       └── config.py                      # Configurações do Kafka (bootstrap servers, acks=all)
│
└── entrypoints/
    └── workers/
        ├── __init__.py
        └── outbox_publisher_worker.py     # Daemon worker de polling e publicação contínua

src/tests/
├── unit/
│   ├── test_outbox_event.py               # Testes unitários da entidade e ciclo do evento
│   └── test_outbox_publisher_worker.py    # Testes unitários do loop do worker com mocks
└── integration/
    ├── test_atomic_transaction_outbox.py  # Teste de persistência atômica no banco
    └── test_outbox_failure_scenarios.py   # Teste do Cenário de Falha 1 (Kafka indisponível / resiliência)
```

---

## 3. Roteiro Passo a Passo de Execução

### Passo 1: Modelagem de Dados e Geração de Migration via Alembic (`outbox_events`)

- Implementar `OutboxEventModel` mapeado no SQLAlchemy (`src/app/infrastructure/database/models/outbox_event_model.py`) e entidade pura `OutboxEvent`:
  - `id`: VARCHAR(36) PRIMARY KEY (UUIDv4)
  - `aggregate_type`: VARCHAR(64) NOT NULL (ex: 'TRANSACTION')
  - `aggregate_id`: VARCHAR(36) NOT NULL (UUID da transação)
  - `topic`: VARCHAR(128) NOT NULL (ex: 'transactions.created.v1')
  - `payload`: JSON NOT NULL
  - `status`: ENUM('PENDING', 'PUBLISHED', 'FAILED', name='outbox_status_enum') NOT NULL DEFAULT 'PENDING'
  - `retry_count`: INT NOT NULL DEFAULT 0
  - `created_at`: TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
  - `published_at`: TIMESTAMP NULL
  - Índices: `idx_outbox_status_created` (`status`, `created_at`) e `idx_outbox_aggregate` (`aggregate_id`).
- Importar `OutboxEventModel` em `alembic/env.py` para registro do `Base.metadata`.
- **Gerar o script de migração pelo próprio Alembic**:
  - Executar: `pipenv run alembic revision -m "create_outbox_events_table"` (ou com `--autogenerate`).
  - Ajustar e garantir que as operações `op.create_table(...)` e `op.drop_table(...)` contenham tipos nativos (`JSON`, `ENUM`) e índices idênticos aos modelos.
- Executar e validar migração com `pipenv run alembic upgrade head`.

### Passo 2: Escrita Atômica (Transação + Outbox Event)

- Definir `OutboxRepositoryPort` e implementar `MySQLOutboxRepository`.
- Atualizar `CreateTransactionUseCase` / `MySQLTransactionRepository` para salvar a entidade `Transaction` e o correspondente `OutboxEvent` (com payload conforme `specs/events/transaction_created.json`) sob a **mesma transação física do banco** (`session.commit()` único).

### Passo 3: Adaptador Kafka Producer e Worker de Publicação

- Implementar `KafkaProducerAdapter` implementando `MessageProducerPort` com `acks=all`, headers estruturados (`x-event-id`, `x-timestamp`) e serialização JSON segura.
- Implementar `OutboxPublisherWorker` (Daemon):
  - Consulta em lote (ex: `batch_size=50`) de eventos `PENDING` ordenados por `created_at ASC`;
  - Utiliza `with_for_update(skip_locked=True)` para permitir paralelismo seguro entre múltiplos workers sem conflitos;
  - Publica no Kafka, executa `flush(timeout=...)` e atualiza para `PUBLISHED` e `published_at=utcnow()`;
  - Trata exceções do Kafka com incremento de `retry_count` e backoff sem travar o loop de outros eventos.

### Passo 4: Suíte de Testes e Validação do Cenário de Falha 1

- **Testes Unitários:**
  - Criação da entidade `OutboxEvent` e validação do payload.
  - Lógica do `OutboxPublisherWorker` com mocks de repositório e Kafka Producer.
- **Testes de Integração:**
  - Persistência atômica: se houver erro ao salvar outbox, a transação da aplicação sofre rollback completo.
  - **Cenário de Falha 1 (Queda do Kafka):** Simular Kafka indisponível (`KafkaException`) durante a criação de transações via API; validar que a API responde com sucesso (`HTTP 201`), os dados ficam armazenados de forma consistente no MySQL com status `PENDING`, e quando o Kafka é restabelecido, o worker publica com sucesso e transiciona para `PUBLISHED`.

---

## 4. Ordem de Execução e Diretrizes de Qualidade

1. Revisar e aprovar este plano (`status: approved`).
2. Implementar models, migrations e portas de outbox.
3. Integrar atomicidade no fluxo de criação de transação.
4. Implementar Kafka Producer e Daemon Worker.
5. Criar suíte completa de testes unitários e de integração (com Cenário de Falha 1).
6. Executar `pipenv run ruff check --fix . && pipenv run ruff format .` e `pipenv run pytest`.
7. Apresentar walkthrough e propostas de commit convencionais ao usuário.
