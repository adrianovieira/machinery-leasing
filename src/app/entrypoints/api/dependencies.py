from collections.abc import Generator
from typing import Annotated

from fastapi import Depends
from sqlalchemy.orm import Session

from app.application.use_cases.create_transaction_use_case import (
    CreateTransactionUseCase,
)
from app.application.use_cases.get_transaction_use_case import (
    GetTransactionUseCase,
)
from app.infrastructure.database.session import SessionLocal
from app.infrastructure.repositories.mysql_transaction_repository import (
    MySQLTransactionRepository,
)


def get_db() -> Generator[Session, None, None]:
    """Dependency para injeção de sessão do banco de dados na API."""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


DbSessionDep = Annotated[Session, Depends(get_db)]


def get_transaction_repository(
    db: DbSessionDep,
) -> MySQLTransactionRepository:
    return MySQLTransactionRepository(db)


TransactionRepoDep = Annotated[
    MySQLTransactionRepository, Depends(get_transaction_repository)
]


def get_create_transaction_use_case(
    repo: TransactionRepoDep,
) -> CreateTransactionUseCase:
    return CreateTransactionUseCase(repo)


def get_transaction_by_id_use_case(
    repo: TransactionRepoDep,
) -> GetTransactionUseCase:
    return GetTransactionUseCase(repo)


CreateTxUseCaseDep = Annotated[
    CreateTransactionUseCase, Depends(get_create_transaction_use_case)
]
GetTxUseCaseDep = Annotated[
    GetTransactionUseCase, Depends(get_transaction_by_id_use_case)
]
