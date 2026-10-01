# Skill: Idempotency & Inbox Pattern

Este documento descreve a lógica e os procedimentos operacionais para garantir **idempotência estrita** e tratamento de duplicidade no consumidor Kafka.

---

## 1. Problema e Objetivos

* **Problema**: Mensagens duplicadas geradas por rebalanceamento de partições, falhas antes do commit de offset ou retransmissões de rede.
* **Objetivo**: Garantir que cada evento seja processado exatamente uma vez nos seus efeitos colaterais de negócio (chamada ao serviço externo de risco e mutações de banco).

---

## 2. Estratégia Lógica da Tabela `inbox_events`

A tabela `inbox_events` funciona como barreira de idempotência relacional:

* `event_id` (VARCHAR 36 - Primary Key): Identificador único original do evento.
* `consumer_group` (VARCHAR 64): Identificador do grupo de consumidores.
* `status` (ENUM): `'PROCESSING'`, `'COMPLETED'`, `'RETRYING'`, `'FAILED'`.
* `processed_at` (TIMESTAMP): Data do registro.

---

## 3. Algoritmo do Consumidor Idempotente

```python
def process_kafka_message(msg):
    event_id = msg.key_or_header("x-event-id") or msg.value["event_id"]
    payload = msg.value["data"]
    tx_id = payload["transaction_id"]

    with session_scope() as session:
        # 1. Tentativa atômica de inserção no Inbox
        try:
            inbox_entry = InboxEventModel(
                event_id=event_id,
                consumer_group="machinery-leasing-consumer",
                status='PROCESSING'
            )
            session.add(inbox_entry)
            session.flush()  # Força validação de PK
        except IntegrityError:
            # Chave primária duplicada detectada!
            session.rollback()
            logger.info(f"Mensagem duplicada detectada {event_id}. Ignorando reexecução.")
            consumer.commit(msg)  # Confirma offset e sai sem chamar serviço externo
            return

        # 2. Transição da transação para PROCESSING
        tx = session.query(TransactionModel).filter_by(id=tx_id).one()
        tx.status = 'PROCESSING'
        session.commit()

    # 3. Execução da regra de negócio externa (fora de transação de longa duração)
    risk_decision = risk_client.evaluate(customer_id=tx.customer_id, value=tx.value)

    # 4. Finalização com commit transacional
    with session_scope() as session:
        tx = session.query(TransactionModel).filter_by(id=tx_id).one()
        inbox_entry = session.query(InboxEventModel).filter_by(event_id=event_id).one()

        if risk_decision.approved:
            tx.status = 'APPROVED'
        else:
            tx.status = 'REJECTED'

        inbox_entry.status = 'COMPLETED'
        session.commit()

    # 5. Commit manual do offset estritamente após o commit no MySQL
    consumer.commit(msg)
```

---

## 4. Tratamento de Falhas Específicas

| Cenário de Falha | Resolução da Skill |
|---|---|
| **Consumer morre antes de chamar `consumer.commit()`** | Ao reiniciar, o Kafka reentrega a mensagem. A tentativa de `INSERT` no Inbox falha por chave duplicada (`IntegrityError`), a execução é abortada sem chamar o serviço de risco novamente e o offset é commitado. |
| **Dois consumers recebem réplicas da mesma mensagem** | O banco de dados rejeita a segunda inserção concorrente pela restrição de Primary Key, garantindo exclusão mútua. |
