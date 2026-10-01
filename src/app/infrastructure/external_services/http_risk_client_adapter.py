from decimal import Decimal
import os
import time

import httpx2
from opentelemetry.trace import SpanKind
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from app.domain.entities.risk_evaluation import (
    RiskDecisionEnum,
    RiskEvaluation,
)
from app.domain.ports.external_risk_client_port import ExternalRiskClientPort
from app.infrastructure.observability.metrics import (
    RISK_ANALYSIS_DURATION_SECONDS,
    RISK_ANALYSIS_REQUESTS_TOTAL,
)
from app.infrastructure.observability.telemetry import (
    get_tracer,
    inject_trace_context,
)


tracer = get_tracer()


class RiskServiceException(Exception):
    """Exceção base para falhas do serviço de risco."""

    pass


class TransientRiskServiceException(RiskServiceException):
    """Exceção para falhas transitórias (timeouts, 5xx, erros de socket)."""

    pass


class PermanentRiskServiceException(RiskServiceException):
    """Exceção para falhas permanentes (4xx, schema inválido, poison pills)."""

    pass


class HttpRiskClientAdapter(ExternalRiskClientPort):
    """Adaptador HTTP com resiliência local via tenacity para análise de risco."""

    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = 5.0,
        max_quick_retries: int = 2,
    ):
        self._base_url = base_url or os.getenv(
            "RISK_SERVICE_URL", "http://localhost:8001"
        )
        self._timeout = timeout
        self._max_quick_retries = max_quick_retries

    def evaluate(self, customer_id: str, value: Decimal) -> RiskEvaluation:
        """Executa a requisição HTTP POST /risk-analysis com retry local via tenacity."""
        return self._execute_with_retry(customer_id=customer_id, value=str(value))

    def _execute_with_retry(self, customer_id: str, value: str) -> RiskEvaluation:
        start_time = time.time()
        url = f"{self._base_url.rstrip('/')}/risk-analysis"

        with tracer.start_as_current_span(
            "http.client.risk_analysis",
            kind=SpanKind.CLIENT,
        ) as span:
            span.set_attribute("http.url", url)

            @retry(
                retry=retry_if_exception_type(
                    (
                        httpx2.NetworkError,
                        httpx2.TimeoutException,
                        httpx2.RemoteProtocolError,
                    )
                ),
                stop=stop_after_attempt(self._max_quick_retries),
                wait=wait_exponential_jitter(initial=0.1, max=1.0),
                reraise=True,
            )
            def _send_request() -> RiskEvaluation:
                payload = {
                    "customer_id": customer_id,
                    "value": value,
                }
                headers = {}
                inject_trace_context(headers)

                try:
                    with httpx2.Client(timeout=self._timeout) as client:
                        response = client.post(url, json=payload, headers=headers)

                        if response.status_code >= 500:
                            raise TransientRiskServiceException(
                                f"Erro interno no serviço de risco: HTTP {response.status_code}"
                            )
                        elif response.status_code >= 400:
                            raise PermanentRiskServiceException(
                                f"Erro de validação ou requisição no serviço de risco: HTTP {response.status_code} - {response.text}"
                            )

                        data = response.json()
                        decision_str = (
                            data.get("result") or data.get("decision") or "REJECTED"
                        )
                        decision_enum = (
                            RiskDecisionEnum.APPROVED
                            if str(decision_str).upper() == "APPROVED"
                            else RiskDecisionEnum.REJECTED
                        )

                        evaluation = RiskEvaluation(
                            decision=decision_enum,
                            reason=data.get("reason"),
                            score=data.get("score"),
                        )

                        RISK_ANALYSIS_REQUESTS_TOTAL.labels(
                            decision=decision_enum.value, result="success"
                        ).inc()
                        RISK_ANALYSIS_DURATION_SECONDS.labels(result="success").observe(
                            time.time() - start_time
                        )

                        return evaluation

                except (
                    httpx2.NetworkError,
                    httpx2.TimeoutException,
                    httpx2.RemoteProtocolError,
                ) as exc:
                    raise exc

            try:
                return _send_request()
            except TransientRiskServiceException as exc:
                RISK_ANALYSIS_REQUESTS_TOTAL.labels(
                    decision="NONE", result="transient_error"
                ).inc()
                RISK_ANALYSIS_DURATION_SECONDS.labels(result="transient_error").observe(
                    time.time() - start_time
                )
                raise exc
            except PermanentRiskServiceException as exc:
                RISK_ANALYSIS_REQUESTS_TOTAL.labels(
                    decision="NONE", result="permanent_error"
                ).inc()
                RISK_ANALYSIS_DURATION_SECONDS.labels(result="permanent_error").observe(
                    time.time() - start_time
                )
                raise exc
            except (
                httpx2.NetworkError,
                httpx2.TimeoutException,
                httpx2.RemoteProtocolError,
            ) as exc:
                RISK_ANALYSIS_REQUESTS_TOTAL.labels(
                    decision="NONE", result="transient_error"
                ).inc()
                RISK_ANALYSIS_DURATION_SECONDS.labels(result="transient_error").observe(
                    time.time() - start_time
                )
                raise TransientRiskServiceException(
                    f"Falha de conexão com serviço de risco após tentativas: {exc}"
                ) from exc
