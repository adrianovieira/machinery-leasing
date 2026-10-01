import random
import uuid

from locust import HttpUser, LoadTestShape, between, task


class TransactionLoadUser(HttpUser):
    """Simula carga massiva de 10.000 requisições/minuto (~167 req/s com ~100 a 300 usuários).

    Cenários:
    - 70% criação de transações com dados aleatórios e cabeçalhos de correlação / traceparent.
    - 15% reenvio de transação idêntica para estressar a barreira de idempotência do Inbox sob concorrência.
    - 15% consultas de status da transação.
    """

    host = "http://localhost:8000"
    wait_time = between(0.05, 0.3)  # Intervalo curto para atingir alto throughput

    def on_start(self):
        self.created_transaction_ids = []

    @task(70)
    def create_new_transaction(self):
        correlation_id = str(uuid.uuid4())
        trace_id = uuid.uuid4().hex
        span_id = uuid.uuid4().hex[:16]
        traceparent = f"00-{trace_id}-{span_id}-01"

        payload = {
            "customer_id": f"cust-locust-{random.randint(1, 50000)}",
            "value": round(random.uniform(5000.0, 350000.0), 2),
        }
        headers = {
            "Content-Type": "application/json",
            "X-Correlation-ID": correlation_id,
            "traceparent": traceparent,
        }

        with self.client.post(
            "/api/v1/transactions",
            json=payload,
            headers=headers,
            catch_response=True,
            name="POST /api/v1/transactions [Create]",
        ) as response:
            if response.status_code == 201:
                data = response.json()
                tx_id = data.get("id")
                if tx_id:
                    self.created_transaction_ids.append(tx_id)
                    # Mantém buffer limitado para não estourar memória do worker Locust
                    if len(self.created_transaction_ids) > 1000:
                        self.created_transaction_ids.pop(0)
                response.success()
            else:
                response.failure(
                    f"Falha ao criar transação: HTTP {response.status_code} - {response.text}"
                )

    @task(15)
    def send_duplicate_transaction_scenario(self):
        """Simula reentrega imediata de mensagem para validar integridade do Inbox e idempotência."""
        correlation_id = str(uuid.uuid4())
        payload = {
            "customer_id": f"cust-dup-{random.randint(1, 1000)}",
            "value": 15000.00,
        }
        headers = {
            "Content-Type": "application/json",
            "X-Correlation-ID": correlation_id,
        }

        # Primeira requisição
        self.client.post(
            "/api/v1/transactions",
            json=payload,
            headers=headers,
            name="POST /api/v1/transactions [Idempotency Test 1]",
        )

        # Reenvio idêntico
        self.client.post(
            "/api/v1/transactions",
            json=payload,
            headers=headers,
            name="POST /api/v1/transactions [Idempotency Test 2]",
        )

    @task(15)
    def query_transaction_status(self):
        if not self.created_transaction_ids:
            return

        tx_id = random.choice(self.created_transaction_ids)
        with self.client.get(
            f"/api/v1/transactions/{tx_id}",
            catch_response=True,
            name="GET /api/v1/transactions/{id} [Query]",
        ) as response:
            if response.status_code == 200:
                response.success()
            elif response.status_code == 404:
                response.failure(f"Transação {tx_id} não encontrada")
            else:
                response.failure(f"Erro inesperado: HTTP {response.status_code}")


class SpikeLoadShape(LoadTestShape):
    """Simula um aumento abrupto (Spike Test) de carga:

    - Estágio 1 (0 a 30s): Carga baixa/aquecimento (~100 a 300 req/min -> 5 usuários)
    - Estágio 2 (30s a 90s): Spike abrupto massivo (10.000+ req/min -> 150 usuários gerados instantaneamente a 100 usuários/s)
    - Estágio 3 (90s a 120s): Sustentação e estabilização (100 usuários)
    - Estágio 4 (> 120s): Encerramento
    """

    stages = [
        {"duration": 30, "users": 5, "spawn_rate": 5},
        {"duration": 90, "users": 150, "spawn_rate": 100},
        {"duration": 120, "users": 100, "spawn_rate": 10},
    ]

    def tick(self):
        run_time = self.get_run_time()

        for stage in self.stages:
            if run_time < stage["duration"]:
                tick_data = (stage["users"], stage["spawn_rate"])
                return tick_data

        return None
