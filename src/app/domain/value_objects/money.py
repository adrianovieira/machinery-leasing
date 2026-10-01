from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class Money:
    """Objeto de Valor imutável para representação monetária com precisão decimal."""

    amount: Decimal

    def __post_init__(self):
        if not isinstance(self.amount, Decimal):
            object.__setattr__(self, "amount", Decimal(str(self.amount)))
        if self.amount <= Decimal("0.00"):
            raise ValueError("O valor monetário deve ser estritamente maior que zero.")

    @classmethod
    def from_float_or_str(cls, value: float | str | Decimal) -> "Money":
        return cls(amount=Decimal(str(value)))

    def to_decimal(self) -> Decimal:
        return self.amount

    def to_float(self) -> float:
        return float(self.amount)

    def __str__(self) -> str:
        return f"{self.amount:.2f}"
