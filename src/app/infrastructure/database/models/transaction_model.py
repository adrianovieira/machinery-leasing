from sqlalchemy import (
    Column,
    DateTime,
    Numeric,
    String,
    func,
)
from sqlalchemy import (
    Enum as SQLEnum,
)
from sqlalchemy.orm import declarative_base

from src.app.domain.entities.transaction import Transaction
from src.app.domain.value_objects.money import Money
from src.app.domain.value_objects.transaction_status import TransactionStatus


Base = declarative_base()


class TransactionModel(Base):
    """Mapeamento ORM SQLAlchemy da tabela 'transactions'."""

    __tablename__ = "transactions"

    id = Column(String(36), primary_key=True, nullable=False)
    customer_id = Column(String(64), nullable=False, index=True)
    value = Column(Numeric(15, 2), nullable=False)
    status = Column(
        SQLEnum(
            TransactionStatus,
            native_enum=True,
            values_callable=lambda x: [e.value for e in x],
        ),
        nullable=False,
        default=TransactionStatus.PENDING,
        index=True,
    )
    failure_reason = Column(String(255), nullable=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    def to_domain(self) -> Transaction:
        """Converte o modelo ORM para a Entidade Pura de Domínio."""
        return Transaction(
            id=self.id,
            customer_id=self.customer_id,
            value=Money.from_float_or_str(self.value),
            status=self.status
            if isinstance(self.status, TransactionStatus)
            else TransactionStatus(self.status),
            failure_reason=self.failure_reason,
            created_at=self.created_at,
            updated_at=self.updated_at,
        )

    @classmethod
    def from_domain(cls, tx: Transaction) -> "TransactionModel":
        """Converte a Entidade Pura de Domínio para o modelo ORM SQLAlchemy."""
        return cls(
            id=tx.id,
            customer_id=tx.customer_id,
            value=tx.value.to_decimal(),
            status=tx.status,
            failure_reason=tx.failure_reason,
            created_at=tx.created_at,
            updated_at=tx.updated_at,
        )
