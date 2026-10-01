from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal


@dataclass(frozen=True)
class Money:
    """Objeto de Valor imutável para representação monetária com precisão decimal (2 casas)."""

    amount: Decimal

    def __post_init__(self):
        dec_amount = (
            self.amount
            if isinstance(self.amount, Decimal)
            else Decimal(str(self.amount))
        )
        quantized = dec_amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        object.__setattr__(self, "amount", quantized)
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
