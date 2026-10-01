from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class RiskDecisionEnum(str, Enum):
    """Decisões possíveis do serviço externo de análise de risco."""

    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class RiskEvaluation(BaseModel):
    """Resultado da análise de risco externa."""

    model_config = ConfigDict(frozen=True, use_enum_values=True)

    decision: RiskDecisionEnum = Field(..., description="Decisão da análise de risco")
    reason: str | None = Field(
        default=None, description="Motivo complementar retornado pelo serviço"
    )
    score: float | None = Field(
        default=None, description="Pontuação de crédito calculada"
    )

    @property
    def is_approved(self) -> bool:
        return self.decision == RiskDecisionEnum.APPROVED
