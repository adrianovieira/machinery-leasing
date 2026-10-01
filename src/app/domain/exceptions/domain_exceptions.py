class DomainError(Exception):
    """Exceção base para todas as violações de regras do domínio puro."""


class InvalidTransactionStateError(DomainError):
    """Lançada ao tentar executar uma transição de estado não permitida."""

    def __init__(self, current_status: str, target_status: str):
        self.current_status = current_status
        self.target_status = target_status
        super().__init__(
            f"Transição inválida de estado: de '{current_status}' para '{target_status}'."
        )


class DomainValidationError(DomainError):
    """Lançada ao violar regras de invariantes de negócio (ex: valores negativos, IDs vazios)."""


class TransactionNotFoundError(DomainError):
    """Lançada quando uma transação pesquisada não existe na base."""

    def __init__(self, transaction_id: str):
        self.transaction_id = transaction_id
        super().__init__(f"Transação com id '{transaction_id}' não foi encontrada.")
