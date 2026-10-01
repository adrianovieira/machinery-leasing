from decimal import Decimal

import pytest

from app.application.dtos.transaction_dtos import (
    CreateTransactionRequestDTO,
    TransactionResponseDTO,
)
from app.domain.entities.transaction import Transaction
from app.domain.exceptions.domain_exceptions import (
    DomainValidationError,
    InvalidTransactionStateError,
)
from app.domain.value_objects.transaction_status import TransactionStatus
from app.infrastructure.database.models.transaction_model import TransactionModel


def test_create_transaction_success():
    tx = Transaction.create(customer_id="cust_123", value=1500.50)
    assert tx.id is not None
    assert tx.customer_id == "cust_123"
    assert tx.value.to_float() == 1500.50
    assert tx.status == TransactionStatus.PENDING
    assert tx.failure_reason is None


def test_create_transaction_validation_errors():
    with pytest.raises(DomainValidationError):
        Transaction.create(customer_id="", value=100.0)

    with pytest.raises(ValueError):
        Transaction.create(customer_id="cust_1", value=0.0)

    with pytest.raises(ValueError):
        Transaction.create(customer_id="cust_1", value=-50.0)


def test_transaction_state_machine_happy_path_approval():
    tx = Transaction.create(customer_id="cust_123", value=500.0)
    assert tx.status == TransactionStatus.PENDING

    tx.mark_as_processing()
    assert tx.status == TransactionStatus.PROCESSING

    tx.mark_as_approved()
    assert tx.status == TransactionStatus.APPROVED
    assert tx.status.is_final is True


def test_transaction_state_machine_rejection():
    tx = Transaction.create(customer_id="cust_123", value=500.0)
    tx.mark_as_processing()
    tx.mark_as_rejected(reason="Score de crédito insuficiente")
    assert tx.status == TransactionStatus.REJECTED
    assert tx.failure_reason == "Score de crédito insuficiente"
    assert tx.status.is_final is True


def test_transaction_state_machine_retries_and_failure():
    tx = Transaction.create(customer_id="cust_123", value=500.0)
    tx.mark_as_processing()

    # Falha temporária vai para RETRYING
    tx.mark_as_retrying(reason="Timeout no serviço externo de risco")
    assert tx.status == TransactionStatus.RETRYING

    # Consumer reprocessa nova tentativa: RETRYING -> PROCESSING
    tx.mark_as_processing()
    assert tx.status == TransactionStatus.PROCESSING

    # Nova falha que esgota retries: PROCESSING/RETRYING -> FAILED
    tx.mark_as_failed(reason="Limite máximo de retries excedido (DLQ)")
    assert tx.status == TransactionStatus.FAILED
    assert tx.failure_reason == "Limite máximo de retries excedido (DLQ)"
    assert tx.status.is_final is True


def test_invalid_state_transitions():
    tx = Transaction.create(customer_id="cust_123", value=500.0)
    # Não pode aprovar direto sem ir para PROCESSING
    with pytest.raises(InvalidTransactionStateError):
        tx.mark_as_approved()

    tx.mark_as_processing()
    tx.mark_as_approved()

    # Estado final não pode transicionar novamente
    with pytest.raises(InvalidTransactionStateError):
        tx.mark_as_processing()

    with pytest.raises(InvalidTransactionStateError):
        tx.mark_as_rejected()


def test_pydantic_dtos_serialization():
    tx = Transaction.create(customer_id="cust_123", value=2500.75)
    response_dto = TransactionResponseDTO.from_domain(tx)
    assert response_dto.id == tx.id
    assert response_dto.customer_id == "cust_123"
    assert response_dto.value == 2500.75
    assert response_dto.status == TransactionStatus.PENDING

    req_dto = CreateTransactionRequestDTO(customer_id="  cust_456  ", value=100.0)
    assert req_dto.customer_id == "cust_456"
    assert req_dto.value == 100.0


def test_sqlalchemy_model_mapping():
    tx = Transaction.create(customer_id="cust_123", value=Decimal("1234.56"))
    model = TransactionModel.from_domain(tx)

    assert model.id == tx.id
    assert model.customer_id == "cust_123"
    assert model.value == Decimal("1234.56")
    assert model.status == TransactionStatus.PENDING

    tx_restored = model.to_domain()
    assert tx_restored.id == tx.id
    assert tx_restored.value.to_decimal() == Decimal("1234.56")
    assert tx_restored.status == TransactionStatus.PENDING
