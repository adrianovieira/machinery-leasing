from abc import ABC, abstractmethod
from decimal import Decimal

from app.domain.entities.risk_evaluation import RiskEvaluation


class ExternalRiskClientPort(ABC):
    """Porta para integração com o serviço externo de análise de risco."""

    @abstractmethod
    def evaluate(self, customer_id: str, value: Decimal) -> RiskEvaluation:
        """Executa a chamada para análise de risco do cliente e valor solicitado."""
        pass
