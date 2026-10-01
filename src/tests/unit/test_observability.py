import io
import json
import logging

from opentelemetry.trace import SpanKind

from app.infrastructure.observability.logging import (
    DataMaskingFilter,
    OpenTelemetryJsonFormatter,
    clear_log_context,
    set_log_context,
)
from app.infrastructure.observability.telemetry import (
    extract_trace_context,
    get_tracer,
    inject_trace_context,
)


def test_data_masking_filter_cpf_and_email():
    """Valida mascaramento de CPF e Email em mensagens de log."""
    filter_instance = DataMaskingFilter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=10,
        msg="Cliente com CPF 123.456.789-00 e email adriano@bamaq.com.br solicitou leasing.",
        args=(),
        exc_info=None,
    )

    filter_instance.filter(record)
    assert "123.456.789-00" not in record.msg
    assert "***.456.***-00" in record.msg
    assert "adriano@bamaq.com.br" not in record.msg
    assert "adr***@bamaq.com.br" in record.msg


def test_data_masking_filter_tokens_and_passwords():
    """Valida mascaramento de chaves sensíveis em dicionários passados ao log."""
    filter_instance = DataMaskingFilter()
    payload = {
        "password": "secret_password_123",
        "token": "bearer_xyz",
        "customer_id": "cust-999",
    }
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=20,
        msg="Payload recebido: %s",
        args=(payload,),
        exc_info=None,
    )

    filter_instance.filter(record)
    masked_dict = record.args[0] if isinstance(record.args, tuple) else record.args
    assert masked_dict["password"] == "******"
    assert masked_dict["token"] == "******"
    assert masked_dict["customer_id"] == "cust-999"


def test_opentelemetry_w3c_context_propagation():
    """Valida injeção e extração de headers W3C TraceContext (traceparent)."""
    tracer = get_tracer()

    with tracer.start_as_current_span("parent_test_span", kind=SpanKind.SERVER) as span:
        ctx = span.get_span_context()
        expected_trace_id = format(ctx.trace_id, "032x")

        # Injeta no carrier
        carrier = {}
        inject_trace_context(carrier)
        assert "traceparent" in carrier
        assert expected_trace_id in carrier["traceparent"]

        # Extrai no filho
        extracted_ctx = extract_trace_context(carrier)
        assert extracted_ctx is not None


def test_opentelemetry_json_log_formatting():
    """Valida que o formatador JSON inclui trace_id, span_id e campos customizados."""
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    formatter = OpenTelemetryJsonFormatter()
    handler.setFormatter(formatter)

    test_logger = logging.getLogger("test_otel_logger")
    test_logger.setLevel(logging.INFO)
    test_logger.addHandler(handler)

    tracer = get_tracer()
    with tracer.start_as_current_span("test_span") as span:
        ctx = span.get_span_context()
        trace_id_hex = format(ctx.trace_id, "032x")
        span_id_hex = format(ctx.span_id, "016x")

        set_log_context(transaction_id="tx-abc-123", customer_id="cust-100")
        test_logger.info("Executando teste de observabilidade")
        clear_log_context()

    log_output = stream.getvalue()
    log_json = json.loads(log_output)

    assert log_json["message"] == "Executando teste de observabilidade"
    assert log_json["level"] == "INFO"
    assert log_json["trace_id"] == trace_id_hex
    assert log_json["span_id"] == span_id_hex
    assert log_json["transaction_id"] == "tx-abc-123"
