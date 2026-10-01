from fastapi import APIRouter, status

from app.application.dtos.transaction_dtos import (
    CreateTransactionRequestDTO,
    TransactionResponseDTO,
)
from app.entrypoints.api.dependencies import (
    CreateTxUseCaseDep,
    GetTxUseCaseDep,
)
from app.infrastructure.observability.metrics import (
    TRANSACTIONS_CREATED_TOTAL,
)


router = APIRouter(prefix="/transactions", tags=["Transactions"])


@router.post(
    "",
    response_model=TransactionResponseDTO,
    status_code=status.HTTP_201_CREATED,
    summary="Cria uma nova solicitação de leasing financeiro",
)
def create_transaction(
    request_dto: CreateTransactionRequestDTO,
    use_case: CreateTxUseCaseDep,
) -> TransactionResponseDTO:
    result = use_case.execute(request_dto)
    TRANSACTIONS_CREATED_TOTAL.labels(status=result.status).inc()
    return result


@router.get(
    "/{transaction_id}",
    response_model=TransactionResponseDTO,
    status_code=status.HTTP_200_OK,
    summary="Consulta uma transação por identificador único",
)
def get_transaction(
    transaction_id: str,
    use_case: GetTxUseCaseDep,
) -> TransactionResponseDTO:
    return use_case.execute(transaction_id)
