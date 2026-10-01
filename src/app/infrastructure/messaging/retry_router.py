from datetime import datetime, timedelta
import random


class RetryBackoffCalculator:
    """Calculadora de backoff exponencial com jitter para retries de mensagens Kafka."""

    def __init__(
        self,
        base_seconds: float = 2.0,
        max_seconds: float = 60.0,
        jitter_min: float = 0.1,
        jitter_max: float = 1.0,
    ):
        self._base_seconds = base_seconds
        self._max_seconds = max_seconds
        self._jitter_min = jitter_min
        self._jitter_max = jitter_max

    def calculate_delay_seconds(self, retry_count: int) -> float:
        """Calcula o delay total em segundos: min(max_backoff, base * 2^retry_count) + jitter."""
        count = max(0, int(retry_count))
        exponential_delay = min(self._max_seconds, self._base_seconds * (2**count))
        jitter = random.uniform(self._jitter_min, self._jitter_max)
        return exponential_delay + jitter

    def calculate_next_retry_timestamp(self, retry_count: int) -> datetime:
        """Calcula o timestamp UTC do próximo reprocessamento."""
        delay = self.calculate_delay_seconds(retry_count)
        return datetime.utcnow() + timedelta(seconds=delay)
