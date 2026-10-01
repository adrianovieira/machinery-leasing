from decimal import Decimal

import httpx2
import pytest

from app.domain.entities.risk_evaluation import RiskDecisionEnum
from app.infrastructure.external_services.http_risk_client_adapter import (
    HttpRiskClientAdapter,
    PermanentRiskServiceException,
    TransientRiskServiceException,
)


def test_http_risk_client_approved(monkeypatch):
    class MockResponse:
        status_code = 200

        def json(self):
            return {"result": "APPROVED", "reason": "Score alto", "score": 850.0}

    def mock_post(*args, **kwargs):
        return MockResponse()

    monkeypatch.setattr(httpx2.Client, "post", mock_post)

    client = HttpRiskClientAdapter(base_url="http://risk-api")
    eval_res = client.evaluate(customer_id="cust-1", value=Decimal("50000.00"))

    assert eval_res.decision == RiskDecisionEnum.APPROVED
    assert eval_res.is_approved is True
    assert eval_res.reason == "Score alto"
    assert eval_res.score == 850.0


def test_http_risk_client_rejected(monkeypatch):
    class MockResponse:
        status_code = 200

        def json(self):
            return {
                "result": "REJECTED",
                "reason": "Restrição cadastral",
                "score": 300.0,
            }

    def mock_post(*args, **kwargs):
        return MockResponse()

    monkeypatch.setattr(httpx2.Client, "post", mock_post)

    client = HttpRiskClientAdapter(base_url="http://risk-api")
    eval_res = client.evaluate(customer_id="cust-2", value=Decimal("150000.00"))

    assert eval_res.decision == RiskDecisionEnum.REJECTED
    assert eval_res.is_approved is False
    assert eval_res.reason == "Restrição cadastral"


def test_http_risk_client_transient_error_500(monkeypatch):
    class MockResponse:
        status_code = 503
        text = "Service Unavailable"

    def mock_post(*args, **kwargs):
        return MockResponse()

    monkeypatch.setattr(httpx2.Client, "post", mock_post)

    client = HttpRiskClientAdapter(base_url="http://risk-api")
    with pytest.raises(TransientRiskServiceException):
        client.evaluate(customer_id="cust-3", value=Decimal("1000.00"))


def test_http_risk_client_permanent_error_400(monkeypatch):
    class MockResponse:
        status_code = 400
        text = "Bad Request: Invalid Customer ID"

    def mock_post(*args, **kwargs):
        return MockResponse()

    monkeypatch.setattr(httpx2.Client, "post", mock_post)

    client = HttpRiskClientAdapter(base_url="http://risk-api")
    with pytest.raises(PermanentRiskServiceException):
        client.evaluate(customer_id="invalid", value=Decimal("1000.00"))
