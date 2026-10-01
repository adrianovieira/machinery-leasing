from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
import yaml

from app.entrypoints.api.dependencies import get_db
from app.entrypoints.api.main import app
from app.infrastructure.database.models.transaction_model import Base


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


def test_openapi_spec_compliance(client: TestClient):
    """Valida se as respostas da API correspondem estritamente à especificação OpenAPI."""
    with open("specs/api_v1.yaml", encoding="utf-8") as f:
        spec = yaml.safe_load(f)

    expected_paths = list(spec["paths"].keys())
    assert "/transactions" in expected_paths
    assert "/transactions/{id}" in expected_paths

    # Valida criação
    response = client.post(
        "/api/v1/transactions", json={"customer_id": "cust_contract", "value": 1500.00}
    )
    assert response.status_code == 201
    data = response.json()

    # Campos obrigatórios da spec
    required_fields = spec["components"]["schemas"]["TransactionResponse"]["required"]
    for field in required_fields:
        assert field in data, f"Campo obrigatório '{field}' ausente na resposta da API"

    # Valida consulta
    tx_id = data["id"]
    get_response = client.get(f"/api/v1/transactions/{tx_id}")
    assert get_response.status_code == 200
    get_data = get_response.json()
    for field in required_fields:
        assert field in get_data, f"Campo obrigatório '{field}' ausente no GET da API"
