import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, Optional
from uuid import uuid4

from simcc.core.logging import logger


class AITracer:
    def __init__(
        self, request_id: Optional[str] = None, query: Optional[str] = None
    ):
        self.request_id = request_id or str(uuid4())
        self.query = query or ''
        self.start_time = time.perf_counter()
        self.stages: Dict[str, float] = {}
        self.metadata: Dict[str, Any] = {
            'cache_hit': False,
            'intent': None,
            'retrieved_count': 0,
            'cutoff_dropped_count': 0,
            'final_count': 0,
            'model': 'gpt-4o-mini',
        }

    def set_meta(self, key: str, value: Any) -> None:
        self.metadata[key] = value

    @asynccontextmanager
    async def trace_stage(self, stage_name: str) -> AsyncIterator[None]:
        t0 = time.perf_counter()
        try:
            yield
        finally:
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            self.stages[stage_name] = round(elapsed_ms, 2)

    def finish(
        self, status: str = 'success', error_message: Optional[str] = None
    ) -> Dict[str, Any]:
        total_duration_ms = round(
            (time.perf_counter() - self.start_time) * 1000.0, 2
        )

        trace_summary = {
            'request_id': self.request_id,
            'query': self.query[:200] if self.query else '',
            'status': status,
            'total_duration_ms': total_duration_ms,
            'stages': self.stages,
            'metadata': self.metadata,
            'error_message': error_message,
        }

        if status == 'success':
            logger.info(
                'ai.pipeline.completed',
                message=f'Pipeline de IA concluída em {total_duration_ms}ms (cache_hit={self.metadata.get("cache_hit")})',
                category='ai',
                request_id=self.request_id,
                duration=total_duration_ms,
                data=trace_summary,
            )
        else:
            logger.error(
                'ai.pipeline.failed',
                message=f'Falha na pipeline de IA: {error_message}',
                category='ai',
                request_id=self.request_id,
                duration=total_duration_ms,
                data=trace_summary,
            )

        return trace_summary
