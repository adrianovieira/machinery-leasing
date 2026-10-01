# Skill: Retry Strategy & Dead Letter Queue (DLQ)

Este documento descreve os algoritmos de **Non-Blocking Retry**, cálculo de backoff exponencial com jitter e isolamento em Dead Letter Queue (DLQ).

---

## 1. Problema e Objetivos

* **Problema**: O serviço externo de análise de risco (`POST /risk-analysis`) pode apresentar lentidão ou ficar fora do ar temporariamente (ex: 30 minutos).
* **Objetivo**: Reprocessar transações com segurança sem travar o processamento de novas transações saudáveis na partição principal (*Head-of-Line Blocking*).

---

## 2. Estratégia de Tópicos e Topologia Kafka

```
[ transactions.created.v1 ] --------(Falha Transitória)--------> [ transactions.retry.v1 ]
           |                                                                 |
    (Processamento OK)                                             (Retry com Backoff)
           |                                                                 |
           v                                                                 v
      [ SUCESSO ]                                            (Excedeu Max Retries / Fatal)
                                                                             |
                                                                             v
                                                                  [ transactions.dlq.v1 ]
```

---

## 3. Algoritmo de Backoff Exponencial com Jitter

Para a fila de retry `transactions.retry.v1`:

$$\text{delay} = \min\left(\text{max\_backoff},\, \text{base\_backoff} \times 2^{\text{retry\_count}}\right) + \text{jitter}$$

Onde:
* `base_backoff` = 2 segundos
* `max_backoff` = 60 segundos
* `jitter` = valor aleatório uniforme entre $0$ e $1$ segundo (evita thundering herd).
* `max_retries` = 3 tentativas

```python
import math
import random
import time
from datetime import datetime, timedelta


def calculate_next_retry(
    retry_count: int, base_seconds: float = 2.0, max_seconds: float = 60.0
) -> datetime:
    exponential_delay = min(max_seconds, base_seconds * (2**retry_count))
    jitter = random.uniform(0.1, 1.0)
    total_delay = exponential_delay + jitter
    return datetime.utcnow() + timedelta(seconds=total_delay)
```

---

## 4. Algoritmo de Roteamento de Erros no Consumidor

```python
def handle_consumer_exception(msg, exc, tx_id, event_id):
    current_retries = int(msg.headers.get("x-retry-count", 0))

    if isinstance(exc, TransientNetworkError) and current_retries < MAX_RETRIES:
        next_attempt_count = current_retries + 1
        next_retry_time = calculate_next_retry(next_attempt_count)

        with session_scope() as session:
            tx = session.query(TransactionModel).filter_by(id=tx_id).one()
            tx.status = "RETRYING"
            session.commit()

        # Publica no tópico de retry com headers de controle
        kafka_producer.produce(
            topic="transactions.retry.v1",
            key=msg.key,
            value=msg.value,
            headers=[
                ("x-retry-count", str(next_attempt_count).encode()),
                ("x-next-retry-timestamp", next_retry_time.isoformat().encode()),
                ("x-event-id", event_id.encode()),
            ],
        )
        kafka_producer.flush()
        consumer.commit(msg)  # Libera a mensagem da fila original

    else:
        # Falha fatal, poison pill ou limite de retries esgotado
        with session_scope() as session:
            tx = session.query(TransactionModel).filter_by(id=tx_id).one()
            tx.status = "FAILED"
            tx.failure_reason = str(exc)[:255]

            inbox_entry = (
                session.query(InboxEventModel)
                .filter_by(event_id=event_id)
                .one_or_none()
            )
            if inbox_entry:
                inbox_entry.status = "FAILED"
            session.commit()

        kafka_producer.produce(
            topic="transactions.dlq.v1",
            key=msg.key,
            value=msg.value,
            headers=[
                ("x-retry-count", str(current_retries).encode()),
                ("x-exception-type", exc.__class__.__name__.encode()),
                ("x-exception-message", str(exc).encode()),
            ],
        )
        kafka_producer.flush()
        consumer.commit(msg)
```

---

## 5. Procedimento de Reprocessamento da DLQ

Para reprocessar mensagens que caíram na DLQ após correção de dependências externas:
1. Executar o script de replay: `python -m src.app.entrypoints.workers.dlq_replay --topic transactions.dlq.v1 --target transactions.created.v1`.
2. O script zera os headers de retries, gera novo `event_id` se necessário ou reinicializa o status no Inbox para `PROCESSING`, permitindo nova execução controlada.
