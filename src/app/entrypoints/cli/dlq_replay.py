import argparse
import json

from app.infrastructure.messaging.dlq_manager import DLQManager
from app.infrastructure.messaging.kafka_producer_adapter import (
    KafkaProducerAdapter,
)


def main():
    parser = argparse.ArgumentParser(
        description="CLI Operacional para Gestão e Reprocessamento (Replay) de mensagens da DLQ."
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Lista as mensagens retidas atualmente no tópico transactions.dlq.v1.",
    )
    parser.add_argument(
        "--replay-all",
        action="store_true",
        help="Republica todas as mensagens da DLQ no tópico alvo e zera retries.",
    )
    parser.add_argument(
        "--target-topic",
        default="transactions.created.v1",
        help="Tópico de destino para o replay (padrão: transactions.created.v1).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Limite de mensagens a listar ou reprocessar (padrão: 50).",
    )
    parser.add_argument(
        "--regenerate-event-id",
        action="store_true",
        help="Gera novo event_id único em vez de resetar o status existente no Inbox.",
    )

    args = parser.parse_args()

    producer = KafkaProducerAdapter()
    manager = DLQManager(producer=producer)

    if args.list:
        print(f"[*] Inspecionando mensagens na DLQ (limite: {args.limit})...")
        messages = manager.inspect_messages(max_messages=args.limit)
        print(f"[+] Total de mensagens encontradas: {len(messages)}")
        for i, msg in enumerate(messages, 1):
            print(f"--- Mensagem #{i} ---")
            print(f"  Chave: {msg.key}")
            print(f"  Headers: {json.dumps(msg.headers, indent=2)}")
            print(f"  Payload: {json.dumps(msg.payload, indent=2)}")
            print("-" * 30)

    elif args.replay_all:
        print(f"[*] Coletando mensagens da DLQ para replay em {args.target_topic}...")
        messages = manager.inspect_messages(max_messages=args.limit)
        if not messages:
            print("[-] Nenhuma mensagem encontrada na DLQ.")
            return

        replayed = 0
        for msg in messages:
            success = manager.replay_message(
                message=msg,
                target_topic=args.target_topic,
                regenerate_event_id=args.regenerate_event_id,
            )
            if success:
                replayed += 1

        print(
            f"[✓] Replay concluído com sucesso: {replayed}/{len(messages)} mensagens reprocessadas."
        )

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
