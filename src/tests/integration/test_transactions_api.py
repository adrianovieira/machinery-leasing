from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.entrypoints.api.dependencies import get_db
from app.entrypoints.api.main import app
from app.infrastructure.database.models.transaction_model import Base


# Setup de banco de dados SQLite em memória para testes de integração isolados
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(autouse=True)
def setup_database():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def client():
    def override_get_db():
        db = TestingSessionLocal()
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_create_transaction_api_success(client: TestClient):
    payload = {"customer_id": "cust_999", "value": 2500.00}
    response = client.post("/api/v1/transactions", json=payload)

    assert response.status_code == 201
    data = response.json()
    assert "id" in data
    assert data["customer_id"] == "cust_999"
    assert data["value"] == 2500.00
    assert data["status"] == "PENDING"
    assert data["failure_reason"] is None
    assert "created_at" in data
    assert "updated_at" in data


def test_get_transaction_api_success(client: TestClient):
    # 1. Cria transação
    create_resp = client.post(
        "/api/v1/transactions", json={"customer_id": "cust_888", "value": 1200.00}
    )
    assert create_resp.status_code == 201
    tx_id = create_resp.json()["id"]

    # 2. Consulta por ID
    get_resp = client.get(f"/api/v1/transactions/{tx_id}")
    assert get_resp.status_code == 200
    data = get_resp.json()
    assert data["id"] == tx_id
    assert data["customer_id"] == "cust_888"
    assert data["value"] == 1200.00
    assert data["status"] == "PENDING"


def test_get_transaction_api_not_found(client: TestClient):
    response = client.get("/api/v1/transactions/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404
    data = response.json()
    assert data["code"] == "TRANSACTION_NOT_FOUND"


def test_create_transaction_api_validation_error(client: TestClient):
    # Valor negativo ou zero
    response = client.post(
        "/api/v1/transactions", json={"customer_id": "cust_1", "value": -10.0}
    )
    assert response.status_code == 422  # Pydantic validation error

    response_zero = client.post(
        "/api/v1/transactions", json={"customer_id": "cust_1", "value": 0.0}
    )
    assert response_zero.status_code == 422

    # customer_id vazio ou apenas espaços
    response2 = client.post(
        "/api/v1/transactions", json={"customer_id": "   ", "value": 100.0}
    )
    assert response2.status_code == 422

    response_empty = client.post(
        "/api/v1/transactions", json={"customer_id": "", "value": 100.0}
    )
    assert response_empty.status_code == 422


def test_create_transaction_api_edge_cases(client: TestClient):
    # 1. Menor valor positivo válido (0.01 centavo)
    resp_min = client.post(
        "/api/v1/transactions", json={"customer_id": "cust_min", "value": 0.01}
    )
    assert resp_min.status_code == 201
    assert resp_min.json()["value"] == 0.01

    # 2. Grande valor financeiro (ex: 99.999.999,99)
    large_val = 99999999.99
    resp_large = client.post(
        "/api/v1/transactions", json={"customer_id": "cust_corp_99", "value": large_val}
    )
    assert resp_large.status_code == 201
    assert resp_large.json()["value"] == large_val

    # 3. Precisão decimal (arredondamento/truncamento padrão financeiro de 2 casas)
    resp_prec = client.post(
        "/api/v1/transactions", json={"customer_id": "cust_prec", "value": 1500.555}
    )
    assert resp_prec.status_code == 201
    assert (
        resp_prec.json()["value"] == 1500.56
    )  # Arredondado para 2 casas decimais via Money VO

    # 4. customer_id com caracteres especiais e espaços que devem ser sanitizados/trimmed
    resp_special = client.post(
        "/api/v1/transactions",
        json={"customer_id": "   cust_ABC-123_#45   ", "value": 250.0},
    )
    assert resp_special.status_code == 201
    assert resp_special.json()["customer_id"] == "cust_ABC-123_#45"

    # 5. customer_id no limite máximo de tamanho (64 caracteres)
    max_customer_id = "C" * 64
    resp_max_len = client.post(
        "/api/v1/transactions", json={"customer_id": max_customer_id, "value": 100.0}
    )
    assert resp_max_len.status_code == 201
    assert resp_max_len.json()["customer_id"] == max_customer_id

    # 6. customer_id excedendo tamanho máximo (65 caracteres)
    too_long_customer_id = "C" * 65
    resp_too_long = client.post(
        "/api/v1/transactions",
        json={"customer_id": too_long_customer_id, "value": 100.0},
    )
    assert resp_too_long.status_code == 422


def test_api_malformed_payloads(client: TestClient):
    # Payload vazio
    resp = client.post("/api/v1/transactions", json={})
    assert resp.status_code == 422

    # Tipos incompatíveis (value como string inválida)
    resp_str = client.post(
        "/api/v1/transactions", json={"customer_id": "c1", "value": "invalid_number"}
    )
    assert resp_str.status_code == 422

    # ID inválido no GET (não-UUID)
    resp_invalid_uuid = client.get("/api/v1/transactions/not-a-valid-uuid-123")
    # Deve retornar 404 de não encontrado ou 422 se validado
    assert resp_invalid_uuid.status_code in (404, 422)
