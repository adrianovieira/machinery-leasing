from enum import Enum


class TransactionStatus(str, Enum):
    """Ciclo de estados de uma transação financeira de leasing de máquinas."""

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    RETRYING = "RETRYING"
    FAILED = "FAILED"

    @property
    def is_final(self) -> bool:
        """Indica se o estado é conclusivo/terminal."""
        return self in (
            TransactionStatus.APPROVED,
            TransactionStatus.REJECTED,
            TransactionStatus.FAILED,
        )
