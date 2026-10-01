from datetime import datetime, timezone
from decimal import Decimal
import uuid

from app.domain.exceptions.domain_exceptions import (
    DomainValidationError,
    InvalidTransactionStateError,
)
from app.domain.value_objects.money import Money
from app.domain.value_objects.transaction_status import TransactionStatus


class Transaction:
    """Entidade pura que representa uma transação financeira de leasing.

    Gerencia o ciclo de vida e a integridade das transições de estado.
    """

    def __init__(
        self,
        id: str,
        customer_id: str,
        value: Money | Decimal | float,
        status: TransactionStatus = TransactionStatus.PENDING,
        failure_reason: str | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
    ):
        if not id:
            raise DomainValidationError("O identificador da transação é obrigatório.")
        if not customer_id or not customer_id.strip():
            raise DomainValidationError(
                "O customer_id é obrigatório e não pode ser vazio."
            )

        self._id = str(id)
        self._customer_id = customer_id.strip()
        self._value = (
            value if isinstance(value, Money) else Money.from_float_or_str(value)
        )
        self._status = (
            status
            if isinstance(status, TransactionStatus)
            else TransactionStatus(status)
        )
        self._failure_reason = failure_reason
        self._created_at = created_at or datetime.now(timezone.utc)
        self._updated_at = updated_at or self._created_at

    @classmethod
    def create(
        cls,
        customer_id: str,
        value: Money | Decimal | float,
        transaction_id: str | None = None,
    ) -> "Transaction":
        """Factory method para criação de nova solicitação de transação."""
        tx_id = transaction_id or str(uuid.uuid4())
        return cls(
            id=tx_id,
            customer_id=customer_id,
            value=value,
            status=TransactionStatus.PENDING,
        )

    # ==========================================
    # Getters
    # ==========================================
    @property
    def id(self) -> str:
        return self._id

    @property
    def customer_id(self) -> str:
        return self._customer_id

    @property
    def value(self) -> Money:
        return self._value

    @property
    def status(self) -> TransactionStatus:
        return self._status

    @property
    def failure_reason(self) -> str | None:
        return self._failure_reason

    @property
    def created_at(self) -> datetime:
        return self._created_at

    @property
    def updated_at(self) -> datetime:
        return self._updated_at

    # ==========================================
    # Transições da Máquina de Estados
    # ==========================================
    def mark_as_processing(self) -> None:
        """Transição PENDING -> PROCESSING ou RETRYING -> PROCESSING."""
        if self._status not in (TransactionStatus.PENDING, TransactionStatus.RETRYING):
            raise InvalidTransactionStateError(
                self._status.value, TransactionStatus.PROCESSING.value
            )
        self._status = TransactionStatus.PROCESSING
        self._updated_at = datetime.now(timezone.utc)

    def mark_as_approved(self) -> None:
        """Transição PROCESSING -> APPROVED."""
        if self._status != TransactionStatus.PROCESSING:
            raise InvalidTransactionStateError(
                self._status.value, TransactionStatus.APPROVED.value
            )
        self._status = TransactionStatus.APPROVED
        self._failure_reason = None
        self._updated_at = datetime.now(timezone.utc)

    def mark_as_rejected(self, reason: str | None = None) -> None:
        """Transição PROCESSING -> REJECTED."""
        if self._status != TransactionStatus.PROCESSING:
            raise InvalidTransactionStateError(
                self._status.value, TransactionStatus.REJECTED.value
            )
        self._status = TransactionStatus.REJECTED
        self._failure_reason = reason
        self._updated_at = datetime.now(timezone.utc)

    def mark_as_retrying(self, reason: str | None = None) -> None:
        """Transição PROCESSING -> RETRYING."""
        if self._status != TransactionStatus.PROCESSING:
            raise InvalidTransactionStateError(
                self._status.value, TransactionStatus.RETRYING.value
            )
        self._status = TransactionStatus.RETRYING
        self._failure_reason = reason
        self._updated_at = datetime.now(timezone.utc)

    def mark_as_failed(self, reason: str) -> None:
        """Transição RETRYING -> FAILED (ou falha crítica durante PROCESSING)."""
        if self._status not in (
            TransactionStatus.RETRYING,
            TransactionStatus.PROCESSING,
        ):
            raise InvalidTransactionStateError(
                self._status.value, TransactionStatus.FAILED.value
            )
        self._status = TransactionStatus.FAILED
        self._failure_reason = reason
        self._updated_at = datetime.now(timezone.utc)
