from datetime import datetime

from app.infrastructure.messaging.retry_router import RetryBackoffCalculator


def test_retry_backoff_calculator():
    calc = RetryBackoffCalculator(
        base_seconds=2.0, max_seconds=60.0, jitter_min=0.0, jitter_max=0.0
    )

    # tentativa 0: 2 * 2^0 = 2s
    assert calc.calculate_delay_seconds(0) == 2.0
    # tentativa 1: 2 * 2^1 = 4s
    assert calc.calculate_delay_seconds(1) == 4.0
    # tentativa 2: 2 * 2^2 = 8s
    assert calc.calculate_delay_seconds(2) == 8.0
    # tentativa 5: 2 * 2^5 = 64s -> capped at 60s
    assert calc.calculate_delay_seconds(5) == 60.0


def test_retry_backoff_with_jitter():
    calc = RetryBackoffCalculator(
        base_seconds=2.0, max_seconds=60.0, jitter_min=0.1, jitter_max=0.5
    )
    delay = calc.calculate_delay_seconds(1)
    assert 4.1 <= delay <= 4.5

    next_time = calc.calculate_next_retry_timestamp(1)
    assert next_time > datetime.utcnow()
