import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from typing import Any, Dict

import structlog
from structlog.types import Processor

# Configurações
LOG_DIR = 'logs'
LOG_APP = os.path.join(LOG_DIR, 'app.log')
LOG_ERROR = os.path.join(LOG_DIR, 'error.log')
LOG_ROUTINES = os.path.join(LOG_DIR, 'routines.log')
RETENTION_DAYS = 7
VECTOR_TRUNCATE_LIMIT = 50


def truncate_large_vectors(
    _, __, event_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Processador para truncar listas longas (embeddings) nos logs.
    Substitui a lista pelo seu tamanho se ultrapassar o limite.
    """
    for key, value in event_dict.items():
        if isinstance(value, list) and len(value) > VECTOR_TRUNCATE_LIMIT:
            event_dict[key] = f'<list size={len(value)}>'
    return event_dict


class LevelFilter(logging.Filter):
    """Filtro para separar logs por nível de severidade."""

    def __init__(self, low, high):
        self._low = low
        self._high = high

    def filter(self, record):
        return self._low <= record.levelno <= self._high


def setup_logging(debug: bool = False):
    """
    Configura o structlog com separação de arquivos e redução de ruído.
    """
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    # Processadores compartilhados
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.format_exc_info,
        structlog.processors.TimeStamper(fmt='iso'),
        truncate_large_vectors,  # Anti-noise para vetores
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    logging_level = logging.DEBUG if debug else logging.INFO

    # 1. Handler para APP (INFO e WARNING)
    app_handler = TimedRotatingFileHandler(
        LOG_APP,
        when='midnight',
        interval=1,
        backupCount=RETENTION_DAYS,
        encoding='utf-8',
    )
    app_handler.addFilter(LevelFilter(logging.DEBUG, logging.WARNING))
    app_handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer()
        )
    )

    # 2. Handler para ERROR (ERROR e CRITICAL)
    error_handler = TimedRotatingFileHandler(
        LOG_ERROR,
        when='midnight',
        interval=1,
        backupCount=RETENTION_DAYS,
        encoding='utf-8',
    )
    error_handler.addFilter(LevelFilter(logging.ERROR, logging.CRITICAL))
    error_handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer()
        )
    )

    # 3. Handler para ROUTINES
    routines_handler = logging.FileHandler(LOG_ROUTINES, encoding='utf-8')
    routines_handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer()
        )
    )

    # 4. Handler para Console (Desenvolvimento)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processor=structlog.dev.ConsoleRenderer(colors=True)
            if sys.stdout.isatty()
            else structlog.processors.JSONRenderer()
        )
    )

    routines_log = logging.getLogger('routines')
    routines_log.addHandler(routines_handler)
    routines_log.addHandler(console_handler)  # Adiciona saída no console
    routines_log.setLevel(logging_level)
    routines_log.propagate = False

    # Configuração global do logging stdlib
    logging.basicConfig(
        handlers=[app_handler, error_handler, console_handler],
        level=logging_level,
    )


    # Silenciar loggers do Uvicorn para evitar duplicação
    for logger_name in ['uvicorn', 'uvicorn.error', 'uvicorn.access']:
        log = logging.getLogger(logger_name)
        log.handlers.clear()
        log.propagate = False

    # Configuração do Structlog
    structlog.configure(
        processors=shared_processors
        + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str = None) -> Any:
    """Retorna um logger do structlog."""
    return structlog.get_logger(name)
