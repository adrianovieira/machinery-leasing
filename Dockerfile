# ==========================================
# Stage 1: Builder (Compilação e Dependências)
# ==========================================
FROM python:3.11-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Instala ferramentas de compilação necessárias para C extensions (confluent-kafka, cryptography)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Cria ambiente virtual isolado para exportar dependências
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Instala pipenv e gera os pacotes instalados dentro do /opt/venv
RUN pip install pipenv

COPY Pipfile Pipfile.lock ./
RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy --system

# ==========================================
# Stage 2: Runtime da Aplicação (Slim & Seguro)
# ==========================================
FROM python:3.11-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH=/app/src

WORKDIR /app

# Instala apenas as bibliotecas dinâmicas de runtime (librdkafka e curl para healthchecks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copia apenas o ambiente virtual compilado do estágio builder (sem compilers/build-essential)
COPY --from=builder /opt/venv /opt/venv

# Copia exclusivamente o código-fonte da aplicação
COPY src/app/ ./src/app/

EXPOSE 8000

CMD ["uvicorn", "app.entrypoints.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

# ==========================================
# Stage 3: Database Migrations (Alembic)
# ==========================================
FROM python:3.11-slim AS migration

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH=/app/src

WORKDIR /app

# Copia o ambiente virtual compilado
COPY --from=builder /opt/venv /opt/venv

# Copia a configuração e scripts do Alembic e os modelos SQLAlchemy em src/app
COPY alembic.ini ./
COPY alembic/ ./alembic/
COPY src/app/ ./src/app/

CMD ["alembic", "upgrade", "head"]

# ==========================================
# Stage 4: Mock Services Runtime (Isolado)
# ==========================================
FROM python:3.11-slim AS mock

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH=/app

WORKDIR /app

# Instala apenas curl para healthcheck do serviço mock
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copia o ambiente virtual compilado
COPY --from=builder /opt/venv /opt/venv

# Copia exclusivamente os arquivos de mocks
COPY mocks/ ./mocks/

EXPOSE 8001

CMD ["uvicorn", "mocks.mock_risk_service:app", "--host", "0.0.0.0", "--port", "8001"]

