import time
import uuid
from typing import Callable

import structlog
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from simcc.core.dependencies import get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()

# Configurações do Middleware
SLOW_REQUEST_THRESHOLD = 1.0  # 1 segundo


class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: Callable
    ) -> Response:
        request_id = str(uuid.uuid4())

        # Bind request_id ao contexto para todos os logs desta request
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            request_id=request_id,
            url=str(request.url),
            path=request.url.path,
            method=request.method,
        )

        start_time = time.perf_counter()

        try:
            response = await call_next(request)
            process_time = time.perf_counter() - start_time

            status_code = response.status_code
            is_slow = process_time > SLOW_REQUEST_THRESHOLD
            is_error = status_code >= 400

            # Controle de nível de log do middleware
            should_log = True
            log_config = settings.LOG_LEVEL_MIDDLEWARE

            if log_config == 'intermediate':
                should_log = is_error or is_slow
            elif log_config == 'error':
                should_log = is_error

            if should_log:
                event = 'request_finished' if not is_error else 'request_failed'
                log_params = {
                    'status_code': status_code,
                    'duration': f'{process_time:.3f}s',
                    'is_slow': is_slow,
                }
                if status_code >= 500:
                    logger.error(event, **log_params)
                elif status_code >= 400:
                    logger.warning(event, **log_params)
                else:
                    logger.info(event, **log_params)

            # Adiciona request_id no header da resposta para debug
            response.headers['X-Request-ID'] = request_id
            return response

        except Exception:
            process_time = time.perf_counter() - start_time
            # Deixa o structlog processar a exceção naturalmente
            logger.exception(
                'request_unhandled_exception',
                duration=f'{process_time:.3f}s',
            )
            raise
