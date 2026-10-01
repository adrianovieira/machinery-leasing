from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.domain.exceptions.domain_exceptions import (
    DomainError,
    DomainValidationError,
    TransactionNotFoundError,
)
from app.entrypoints.api.middlewares.correlation_middleware import (
    CorrelationAndTelemetryMiddleware,
)
from app.entrypoints.api.routers.transactions_router import (
    router as transactions_router,
)
from app.entrypoints.api.routes.health import router as health_router
from app.entrypoints.api.routes.metrics import router as metrics_router
from app.infrastructure.observability.logging import configure_logging


def create_app() -> FastAPI:
    """Factory para criação e configuração da aplicação FastAPI."""
    configure_logging()

    app = FastAPI(
        title="API de Processamento de Transações de Leasing",
        description="Serviço síncrono para criação e consulta de transações financeiras.",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # Middleware de OpenTelemetry, Trace Context e Correlation ID
    app.add_middleware(CorrelationAndTelemetryMiddleware)

    # Inclusão de Rotas
    app.include_router(transactions_router, prefix="/api/v1")
    app.include_router(metrics_router)
    app.include_router(health_router)

    # ==========================================
    # Global Exception Handlers
    # ==========================================
    @app.exception_handler(TransactionNotFoundError)
    async def transaction_not_found_handler(
        request: Request, exc: TransactionNotFoundError
    ):
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={
                "code": "TRANSACTION_NOT_FOUND",
                "message": str(exc),
                "details": None,
            },
        )

    @app.exception_handler(DomainValidationError)
    async def domain_validation_handler(request: Request, exc: DomainValidationError):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "code": "VALIDATION_ERROR",
                "message": str(exc),
                "details": None,
            },
        )

    @app.exception_handler(DomainError)
    async def generic_domain_error_handler(request: Request, exc: DomainError):
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                "code": "DOMAIN_ERROR",
                "message": str(exc),
                "details": None,
            },
        )

    @app.get("/health", tags=["Health"])
    def health_check():
        return {"status": "UP"}

    return app


app = create_app()
