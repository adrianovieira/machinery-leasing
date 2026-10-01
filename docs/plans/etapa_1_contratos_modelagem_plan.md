# Implementation Plan: Etapa 1 — Contratos e Modelagem de Dados (Specs First)

Este documento define o plano de implementação da **Etapa 1**, estabelecendo as entidades puras de domínio, value objects, exceções, DTOs Pydantic alinhados aos contratos OpenAPI/JSON Schema e os modelos SQLAlchemy com migrations Alembic para a tabela `transactions`.

---

## 1. Objetivos da Etapa 1

1. Modularizar os contratos de eventos em `specs/events/` (`transaction_created.json` e `transaction_status_changed.json`).
2. Criar a camada de domínio puro (`src/app/domain/`):
   - `TransactionStatus` (Enum com os 6 estados: `PENDING`, `PROCESSING`, `APPROVED`, `REJECTED`, `RETRYING`, `FAILED`);
   - Entidade pura `Transaction` com métodos de transição de estado e validação de invariantes;
   - Value Objects imutáveis (`Money`, `CustomerId`, `TransactionId`);
   - Exceções de Domínio (`InvalidTransactionStateError`, `DomainValidationError`, `TransactionNotFoundError`);
   - Port Interfaces (`TransactionRepositoryPort`).
3. Criar DTOs de Aplicação (`src/app/application/dtos/`) com validações Pydantic V2 estritas.
4. Criar a camada de persistência com SQLAlchemy 2.0 (`src/app/infrastructure/database/`):
   - Model `TransactionModel` com mapeamento de `status` como Enum nativo do MySQL;
   - Configuração do engine e session factory com `pymysql`;
   - Setup do Alembic e migration inicial `001_create_transactions_table.py`.
5. Criar testes unitários para a máquina de estados e invariantes de domínio em `src/tests/unit/test_domain_transaction.py`.

---

## 2. Estrutura de Arquivos a Implementar

```
specs/
└── events/
    ├── transaction_created.json
    └── transaction_status_changed.json

src/app/
├── domain/
│   ├── entities/
│   │   ├── __init__.py
│   │   └── transaction.py
│   ├── exceptions/
│   │   ├── __init__.py
│   │   └── domain_exceptions.py
│   ├── ports/
│   │   ├── __init__.py
│   │   └── transaction_repository_port.py
│   └── value_objects/
│       ├── __init__.py
│       ├── transaction_status.py
│       └── money.py
│
├── application/
│   └── dtos/
│       ├── __init__.py
│       └── transaction_dtos.py
│
└── infrastructure/
    └── database/
        ├── session.py
        ├── models/
        │   ├── __init__.py
        │   └── transaction_model.py
        └── alembic/ (configuração e migrations)

src/tests/
└── unit/
    └── test_domain_transaction.py
```

---

## 3. Critérios de Aceite

1. Entidade de domínio pura sem qualquer dependência de frameworks ou IO.
2. Todas as 6 transições válidas da máquina de estados testadas e garantidas.
3. Tentativas de transições ilegais lançam `InvalidTransactionStateError`.
4. DTOs Pydantic validam tipos, limites e formatam saídas em conformidade estrita com `specs/api_v1.yaml`.
5. Models SQLAlchemy criados com tipos adequados (`DECIMAL(15, 2)`, `ENUM`, `TIMESTAMP`).
6. Testes unitários de domínio executando com 100% de sucesso.
