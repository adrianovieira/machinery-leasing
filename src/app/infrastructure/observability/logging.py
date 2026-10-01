import contextvars
import logging
import re
from typing import Any

from pythonjsonlogger import json

from app.infrastructure.observability.telemetry import (
    get_current_trace_and_span_ids,
)


# Context variables para enriquecer logs em chamadas assíncronas/threads
_log_context: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "log_context", default=None
)


def set_log_context(**kwargs):
    current = _log_context.get()
    ctx = current.copy() if current else {}
    ctx.update(kwargs)
    _log_context.set(ctx)


def clear_log_context():
    _log_context.set(None)


class DataMaskingFilter(logging.Filter):
    """Filtro de Logging para mascaramento de PII e dados sensíveis (LGPD)."""

    # Padrões comuns de dados sensíveis
    CPF_REGEX = re.compile(r"\b(\d{3})\.?(\d{3})\.?(\d{3})-?(\d{2})\b")
    EMAIL_REGEX = re.compile(
        r"\b([A-Za-z0-9._%+-]{1,3})[A-Za-z0-9._%+-]*@([A-Za-z0-9.-]+\.[A-Z|a-z]{2,})\b"
    )
    TOKEN_KEYS = {
        "password",
        "senha",
        "token",
        "secret",
        "authorization",
        "api_key",
        "credit_card",
    }

    def filter(self, record: logging.LogRecord) -> bool:
        # Mascarar mensagem se for string
        if isinstance(record.msg, str):
            record.msg = self._mask_text(record.msg)

        # Mascarar argumentos se houver
        if record.args:
            if isinstance(record.args, dict):
                record.args = self._mask_dict(record.args)
            elif isinstance(record.args, (list, tuple)):
                record.args = tuple(
                    self._mask_dict(a)
                    if isinstance(a, dict)
                    else (self._mask_text(a) if isinstance(a, str) else a)
                    for a in record.args
                )

        return True

    def _mask_text(self, text: str) -> str:
        # Mascara CPF: 123.456.789-00 -> ***.456.***-00
        text = self.CPF_REGEX.sub(r"***.\2.***-\4", text)
        # Mascara Email: adriano@dominio.com -> adr***@dominio.com
        text = self.EMAIL_REGEX.sub(r"\1***@\2", text)
        return text

    def _mask_dict(self, data: dict) -> dict:
        masked = {}
        for k, v in data.items():
            if str(k).lower() in self.TOKEN_KEYS:
                masked[k] = "******"
            elif isinstance(v, str):
                masked[k] = self._mask_text(v)
            elif isinstance(v, dict):
                masked[k] = self._mask_dict(v)
            else:
                masked[k] = v
        return masked


class OpenTelemetryJsonFormatter(json.JsonFormatter):
    """Formatador JSON estruturado que anexa trace_id e span_id do OpenTelemetry e contexto local."""

    def format(self, record: logging.LogRecord) -> str:
        # Garante que record.message existe e record.msg foi interpolado se necessário
        if record.args:
            try:
                record.msg = record.msg % record.args
                record.args = None
            except Exception:
                pass
        return super().format(record)

    def add_fields(
        self,
        log_record: dict[str, Any],
        record: logging.LogRecord,
        message_dict: dict[str, Any],
    ):
        super().add_fields(log_record, record, message_dict)

        # Nível de log padronizado
        log_record["level"] = record.levelname
        log_record["logger"] = record.name

        # Injeta OpenTelemetry trace_id e span_id
        trace_id, span_id = get_current_trace_and_span_ids()
        if trace_id:
            log_record["trace_id"] = trace_id
        if span_id:
            log_record["span_id"] = span_id

        # Injeta context variables adicionais
        ctx = _log_context.get()
        if ctx:
            for k, v in ctx.items():
                if k not in log_record:
                    log_record[k] = v


def configure_logging(level: int = logging.INFO):
    """Configura o root logger para emitir JSON estruturado com mascaramento."""
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Limpa handlers existentes
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    handler = logging.StreamHandler()
    formatter = OpenTelemetryJsonFormatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    handler.setFormatter(formatter)
    handler.addFilter(DataMaskingFilter())

    root_logger.addHandler(handler)
