# Skill: Transactional Outbox Pattern

Este documento descreve a lógica e os procedimentos operacionais para a implementação do padrão **Transactional Outbox**, garantindo atomicidade entre o banco de dados (MySQL) e o message broker (Apache Kafka).

---

## 1. Problema e Objetivos

* **Problema**: Evitar dual-write inconsistente (`db.commit()` seguido de `kafka.produce()`).
* **Objetivo**: Garantir que nenhum evento de transação criada ou atualizada seja perdido, operando sob semântica *at-least-once*.

---

## 2. Estratégia Lógica da Escrita (Producer / API)

1. Abrir transação no MySQL via Session do SQLAlchemy.
2. Inserir a entidade `Transaction` na tabela `transactions` com status inicial `PENDING`.
3. Inserir o evento correspondente na tabela `outbox_events`:
   * `id`: UUIDv4 do evento.
   * `aggregate_type`: `"TRANSACTION"`.
   * `aggregate_id`: `transaction.id`.
   * `topic`: `"transactions.created.v1"`.
   * `payload`: JSON serializado do `TransactionCreatedEvent`.
   * `status`: `'PENDING'`.
4. Executar `session.commit()` atômico. Se o banco falhar, nenhum evento é gerado.

---

## 3. Algoritmo do Outbox Relay Worker (Publisher Daemon)

O worker executa um loop de polling resiliente:

```python
def outbox_polling_loop():
    while running:
        with session_scope() as session:
            # 1. Lock pessimista não bloqueante com concorrência segura
            pending_events = session.query(OutboxEventModel)\
                .filter(OutboxEventModel.status == 'PENDING')\
                .order_by(OutboxEventModel.created_at.asc())\
                .limit(BATCH_SIZE)\
                .with_for_update(skip_locked=True)\
                .all()

            if not pending_events:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            # 2. Publicação com confirmação estrita (acks=all)
            for event in pending_events:
                try:
                    kafka_producer.produce(
                        topic=event.topic,
                        key=event.aggregate_id.encode('utf-8'),
                        value=json.dumps(event.payload).encode('utf-8'),
                        headers=[("x-event-id", event.id.encode('utf-8'))]
                    )
                    kafka_producer.flush(timeout=5.0)
                    
                    # 3. Marcação de sucesso
                    event.status = 'PUBLISHED'
                    event.published_at = datetime.utcnow()
                except KafkaError as exc:
                    logger.error(f"Erro ao publicar outbox {event.id}: {exc}")
                    event.retry_count += 1
                    if event.retry_count >= MAX_OUTBOX_RETRIES:
                        event.status = 'FAILED'
                    break  # Pausa lote e tenta novamente no próximo ciclo
            
            session.commit()
```

---

## 4. Tratamento de Falhas Específicas

| Cenário de Falha | Resolução da Skill |
|---|---|
| **Queda do Kafka** | O loop do worker falha no `flush()`, não comita a alteração para `PUBLISHED`, e os eventos continuam persistidos em segurança no MySQL como `PENDING`. |
| **Múltiplas instâncias do Worker** | A cláusula `SKIP LOCKED` do MySQL 8.0 garante que instâncias concorrentes nunca peguem o mesmo lote de eventos. |
| **Erro de Serialização JSON** | Se o payload for corrompido, o contador de retries é incrementado até o limite e marcado como `FAILED` para auditoria. |
