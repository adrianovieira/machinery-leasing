# Diretrizes de Operação com Agentes de IA (Spec-Driven Development)

Este documento define os padrões, a estrutura de arquivos e o modelo operacional para o desenvolvimento orientado por especificações (SDD - Spec-Driven Development) desta plataforma de processamento financeiro assíncrono.

## Princípios Operacionais

1. **Spec First**: Nenhuma implementação de código deve ser iniciada sem que a especificação em `specs/` ou o Architecture Decision Record (ADR) em `docs/architecture/decisions/` esteja formalizado e aprovado.
2. **Resiliência & Determinismo**: Todo fluxo assíncrono deve suportar entrega _at-least-once_, falhas parciais de dependências externas e garantir idempotência estrita.
3. **Padrão Documental**:
   - Documentação técnica e especificações estruturadas em **AsciiDoc** (`.adoc`).
   - Diagramas arquiteturais e de sequência gerados exclusivamente em **PlantUML** (`.puml`) localizados na pasta `docs/diagrams/` e incluídos via diretiva `include::`.

---

## Contexto do projeto

Este projeto é um sistema de processamento de transações de leasing de máquinas.

Stack:

- Python
- FastAPI
- MySQL
- SQLAlchemy
- Alembic
- Kafka
- Docker Compose
- Pytest
- pipenv
- git

Requisitos principais:

- POST /transactions
- GET /transactions/{id}
- Transactional Outbox
- Consumer idempotente
- Inbox Pattern
- Retry com backoff
- DLQ
- Serviço externo de análise de risco
- Testes unitários e de integração
- Logs estruturados
- Arquitetura em camadas ou hexagonal

Regras:

- MySQL é a fonte principal da verdade.
- A transação e o evento da outbox devem ser persistidos na mesma transação.
- O offset Kafka só deve ser confirmado depois do commit no MySQL.
- O consumidor deve suportar mensagens duplicadas.
- Não alterar arquivos existentes sem explicar a alteração.
- Não inventar bibliotecas ou APIs.
- Priorizar código simples, testável e executável.

---

## Fluxo de Trabalho SDD (Spec-Driven Development)

Para cada nova funcionalidade ou correção crítica solicitada, o agente DEVE seguir a sequência abaixo e gerar os seguintes artefatos antes de escrever código de implementação:

1. **Specs (Contratos):**
   - Criar/atualizar arquivos em `specs/` (ex: `specs/api_v1.yaml`, `specs/kafka_event_schema.json`).
   - Estes arquivos definem os tipos estritos, validações e interfaces.
   - _Regra:_ O código nunca deve violar estas specs.

2. **Skills (Lógica de Negócio):**
   - Criar/atualizar `docs/skills/<nome_da_skill>.md` (ex: `docs/skills/idempotency.md`, `docs/skills/outbox_pattern.md`).
   - Descrever aqui a estratégia lógica, algoritmos de retry, e como lidar com falhas específicas.
   - _Regra:_ Esta seção explica o "COMO" a lógica será resolvida.

3. **Implementation Plan (Roteiro):**
   - Criar `docs/plans/<feature_name>_plan.md`.
   - Detalhar os passos de implementação, quais Tools serão usadas para cada Skill e a ordem de execução.
   - _Regra:_ Este plano deve ser aprovado pelo usuário antes da geração de código.

**Ordem de Execução Obrigatória:**

1. Ler `AGENTS.md` para contexto global.
2. Gerar/Atualizar `specs/` e `docs/skills/`.
3. Gerar `docs/plans/...`.
4. Aguardar aprovação do usuário.
5. Gerar código de implementação (`src/`, `tests/`) e testes.

---

## Controle de Versão e Padrão de Commits (Git)

Todo código gerado ou modificado deve ser versionado seguindo estritamente as diretrizes abaixo. O agente deve simular ou executar comandos `git` conforme solicitado, garantindo que as mensagens (em pt-BR) de commit sigam o padrão **Conventional Commits**.

### Formato do Commit

A mensagem do commit deve seguir a estrutura estrita:

```text
<tipo>(<escopo>): <assunto curto> (<60 chars>)

<body opcional>
- Bullet point 1
- Bullet point 2

<footer opcional>
Closes #ID
```

### Regras de Estilo e Limite

- **Linha de Assunto (Subject):**
  - Máximo de **60 caracteres**.
  - Comece com letra minúscula (após o tipo/escopo).
  - Não use ponto final (.).
  - Use o modo imperativo (exemplo: "Adiciona funcionalidade", não "Adicionou funcionalidade").
- **Corpo (Body):**
  - Separe do assunto por uma linha em branco.
  - Limite de **72 caracteres** por linha.
  - Use **bullet points** (`-`) para listar mudanças, motivos ou impactos.
  - Explique o "O QUE" e o "POR QUÊ", não o "COMO" (o código já mostra o como).
- **Rodapé (Footer):**
  - Separe do corpo por uma linha em branco.
  - Use para referenciar issues (`Closes #ID`) ou quebrar compatibilidade (`BREAKING CHANGE:`).

### Tipos Permitidos

- `feat`: Nova funcionalidade.
- `fix`: Correção de bug.
- `docs`: Mudança apenas na documentação.
- `refactor`: Refatoração de código sem mudança de comportamento.
- `test`: Adição ou correção de testes.
- `chore`: Manutenção de build, ferramentas, dependências.
- `ci`: Mudanças em arquivos de CI/CD.
- `perf`: Melhoria de performance.
- `style`: Formatação, sintaxe (sem lógica).

### Exemplos Corretos vs Incorretos

**Correto:**

```text
feat(outbox): adiciona persistencia atomica e worker

- Cria migration SQL para tabela outbox_events
- Implementa logica de publicacao no Kafka
- Garante atomicidade com transacao principal

Closes #12
```

**Incorreto:**

```text
Feito a tabela outbox e o worker pra mandar pro kafka.
(Assunto longo, pontuação, não imperativo, sem escopo)
```

### Ação do Agente

Sempre que uma tarefa for concluída ou o usuário solicitar:

1. Analise as mudanças nos arquivos.
2. Determine o `<tipo>` usando a tabela de mapeamento.
3. Redija o `<assunto>` (max 60 chars) e `<body>` (bullets, max 72 chars).
4. Apresente a mensagem completa ao usuário para **aprovação** antes de executar `git commit`.
