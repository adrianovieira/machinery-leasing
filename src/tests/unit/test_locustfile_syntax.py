from unittest.mock import MagicMock

from tests.performance.locustfile import TransactionLoadUser


def test_locustfile_structure_and_tasks():
    """Valida a estrutura de classes, pesos das tarefas e métodos do Locust."""
    user = TransactionLoadUser(environment=MagicMock())
    user.client = MagicMock()
    user.on_start()

    assert user.created_transaction_ids == []

    # 1. Simula create_new_transaction
    user.client.post.return_value.__enter__.return_value.status_code = 201
    user.client.post.return_value.__enter__.return_value.json.return_value = {
        "id": "tx-locust-1",
        "status": "PENDING",
    }
    user.create_new_transaction()
    assert "tx-locust-1" in user.created_transaction_ids

    # 2. Simula query_transaction_status
    user.client.get.return_value.__enter__.return_value.status_code = 200
    user.query_transaction_status()
    user.client.get.assert_called()

    # 3. Simula send_duplicate_transaction_scenario
    user.send_duplicate_transaction_scenario()
    assert user.client.post.call_count >= 2
