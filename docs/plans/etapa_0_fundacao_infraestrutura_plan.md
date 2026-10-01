# Implementation Plan: Etapa 0 — Fundação e Infraestrutura Local

Este documento define o plano de implementação da **Etapa 0**, estabelecendo as bases do repositório, ambiente virtual Python com Pipenv, Docker Compose com containers de infraestrutura e health checks.

---

## 1. Objetivos da Etapa 0

1. Criar a estrutura completa de pastas do projeto em `src/app/`, `src/tests/`, `specs/events/` e `docs/plans/`.
2. Configurar o gerenciamento de dependências via **Pipenv** (`Pipfile`) com as bibliotecas essenciais (`fastapi`, `uvicorn`, `sqlalchemy`, `alembic`, `confluent-kafka`, `redis`, `pydantic`, `pydantic-settings`, `pytest`, `pytest-asyncio`, `httpx`, `tenacity`, `ruff`).
3. Configurar variáveis de ambiente (`.env.example` e `.env`).
4. Criar o arquivo `compose.yml` contendo:
   - **MySQL 8.0** (banco de dados relacional principal);
   - **Kafka 3.8 em modo KRaft** (sem dependência de ZooKeeper);
   - **Redis 7.2** (cache de leitura opcional).
5. Criar script de health check / verificação de conectividade da infraestrutura.

---

## 2. Estrutura de Arquivos a Criar

```
.
├── .env.example
├── .gitignore
├── Pipfile
├── compose.yml
├── docs/
│   └── plans/
│       └── etapa_0_fundacao_infraestrutura_plan.md
├── scripts/
│   └── check_infra.py
└── src/
    ├── app/
    │   ├── __init__.py
    │   ├── domain/
    │   │   ├── __init__.py
    │   │   ├── entities/
    │   │   ├── exceptions/
    │   │   ├── ports/
    │   │   └── value_objects/
    │   ├── application/
    │   │   ├── __init__.py
    │   │   ├── use_cases/
    │   │   └── dtos/
    │   ├── infrastructure/
    │   │   ├── __init__.py
    │   │   ├── database/
    │   │   ├── messaging/
    │   │   ├── http_clients/
    │   │   └── cache/
    │   └── entrypoints/
    │       ├── __init__.py
    │       ├── api/
    │       └── workers/
    └── tests/
        ├── __init__.py
        ├── unit/
        ├── integration/
        └── contract/
```

---

## 3. Critérios de Aceite

1. Estrutura de pastas criada sem violações da arquitetura hexagonal.
2. `Pipfile` configurado com versões compatíveis das dependências e `ruff` no ambiente de desenvolvimento.
3. `compose.yml` testado e operacional com health checks válidos.
4. Script de health check conecta com sucesso ao MySQL, Kafka e Redis.
