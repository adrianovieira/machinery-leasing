"""
Script de verificação de integridade e conectividade da infraestrutura local (MySQL, Kafka, Redis).
"""

import socket
import sys


def check_tcp_port(
    host: str, port: int, service_name: str, timeout: float = 3.0
) -> bool:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        if result == 0:
            print(f"✅ {service_name} está acessível em {host}:{port}")
            return True
        else:
            print(f"❌ {service_name} NÃO está acessível em {host}:{port}")
            return False
    except OSError as exc:
        print(f"❌ Erro ao conectar em {service_name} ({host}:{port}): {exc}")
        return False


def main():
    print("Iniciando verificação de conectividade da infraestrutura...")
    services = [
        ("localhost", 3306, "MySQL 8.0"),
        ("localhost", 9092, "Apache Kafka (KRaft)"),
        ("localhost", 6379, "Redis 7.2"),
    ]

    all_ok = True
    for host, port, name in services:
        if not check_tcp_port(host, port, name):
            all_ok = False

    if all_ok:
        print("\n🎉 Toda a infraestrutura está online e pronta para uso!")
        sys.exit(0)
    else:
        print("\n⚠️ Alguns serviços não responderam. Execute: docker compose up -d")
        sys.exit(1)


if __name__ == "__main__":
    main()
