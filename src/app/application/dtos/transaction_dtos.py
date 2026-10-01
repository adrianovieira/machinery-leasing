from datetime import datetime

from pydantic import BaseModel, Field, field_validator

from app.domain.entities.transaction import Transaction
from app.domain.value_objects.transaction_status import TransactionStatus


class CreateTransactionRequestDTO(BaseModel):
    """Payload de entrada para criação de transação via POST /transactions."""

    customer_id: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="Identificador único do cliente solicitante",
        examples=["123"],
    )
    value: float = Field(
        ...,
        gt=0.0,
        description="Valor monetário da transação (deve ser estritamente maior que zero)",
        examples=[1500.00],
    )

    @field_validator("customer_id")
    @classmethod
    def validate_customer_id(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError(
                "O customer_id não pode ser vazio ou conter apenas espaços."
            )
        return stripped


class TransactionResponseDTO(BaseModel):
    """Contrato de resposta da API para transações criadas e consultadas."""

    id: str = Field(..., description="Identificador UUIDv4 da transação")
    customer_id: str = Field(..., description="Identificador do cliente")
    value: float = Field(..., description="Valor da transação")
    status: TransactionStatus = Field(..., description="Status atual da transação")
    failure_reason: str | None = Field(
        None, description="Motivo da falha caso status=FAILED"
    )
    created_at: datetime = Field(..., description="Timestamp ISO-8601 de criação")
    updated_at: datetime = Field(
        ..., description="Timestamp ISO-8601 da última atualização"
    )

    @classmethod
    def from_domain(cls, tx: Transaction) -> "TransactionResponseDTO":
        """Factory para mapear da Entidade Pura de Domínio para DTO de saída."""
        return cls(
            id=tx.id,
            customer_id=tx.customer_id,
            value=tx.value.to_float(),
            status=tx.status,
            failure_reason=tx.failure_reason,
            created_at=tx.created_at,
            updated_at=tx.updated_at,
        )


class ErrorResponseDTO(BaseModel):
    """Formato padronizado de erro da API."""

    code: str = Field(..., description="Código do erro de negócio ou validação")
    message: str = Field(..., description="Descrição legível da falha")
    details: list[str] | None = Field(
        None, description="Detalhes adicionais dos campos inválidos"
    )
