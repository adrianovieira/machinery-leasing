# Implementation Plan: Etapa 2 — API Básica e Persistência Síncrona (Core)

Este documento define o plano de implementação da **Etapa 2**, focando na construção dos Casos de Uso, na implementação concreta do Repositório MySQL, nos endpoints HTTP da API FastAPI e na suíte de testes de integração e contrato.

---

## 1. Objetivos da Etapa 2

1. **Casos de Uso de Aplicação (`src/app/application/use_cases/`)**:
   - `CreateTransactionUseCase`: Recebe DTO, instancia a entidade `Transaction` (com validações de invariantes de domínio), persiste via `TransactionRepositoryPort` e retorna `TransactionResponseDTO`.
   - `GetTransactionUseCase`: Busca por ID na porta do repositório, tratando o caso de `TransactionNotFoundError`.
2. **Repositório Concreto MySQL (`src/app/infrastructure/repositories/`)**:
   - `MySQLTransactionRepository`: Implementa a interface `TransactionRepositoryPort` usando sessões SQLAlchemy para persistência e consulta na tabela `transactions`.
3. **API HTTP com FastAPI (`src/app/entrypoints/api/`)**:
   - Configuração da aplicação FastAPI com middlewares e manipuladores globais de exceções (`DomainValidationError` -> HTTP 400, `TransactionNotFoundError` -> HTTP 404, `DomainError` -> HTTP 422/500).
   - Router `/api/v1/transactions`:
     - `POST /transactions`: Endpoint para submissão de nova transação (HTTP 201).
     - `GET /transactions/{id}`: Endpoint para consulta de transação por ID (HTTP 200).
4. **Testes de Integração e Contrato (`src/tests/integration/` e `src/tests/contract/`)**:
   - Testes de integração dos endpoints com SQLite/MySQL em memória;
   - Testes de contrato validando conformidade estrita dos payloads de resposta com `specs/api_v1.yaml`.

---

## 2. Estrutura de Arquivos a Implementar

```
src/app/
├── application/
│   └── use_cases/
│       ├── __init__.py
│       ├── create_transaction_use_case.py
│       └── get_transaction_use_case.py
│
├── infrastructure/
│   └── repositories/
│       ├── __init__.py
│       └── mysql_transaction_repository.py
│
└── entrypoints/
    └── api/
        ├── __init__.py
        ├── main.py
        ├── dependencies.py
        └── routers/
            ├── __init__.py
            └── transactions_router.py

src/tests/
├── integration/
│   └── test_transactions_api.py
└── contract/
    └── test_openapi_contract.py
```

---

## 3. Critérios de Aceite

1. `POST /api/v1/transactions` com payload válido retorna HTTP `201 Created` contendo o ID gerado, status `PENDING` e campos de timestamp.
2. `POST /api/v1/transactions` com payload inválido (ex: `value <= 0` ou `customer_id` vazio) retorna HTTP `400 Bad Request` ou `422 Unprocessable Entity` com schema de erro padronizado.
3. `GET /api/v1/transactions/{id}` retorna HTTP `200 OK` para transações existentes e `404 Not Found` para IDs inexistentes.
4. Suíte de testes automatizados (`pytest`) e linter (`ruff`) executando com 100% de sucesso.
