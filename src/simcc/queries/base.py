import re
import time
from typing import Any, Dict, List, Set

import structlog
from sqlalchemy import text

from simcc.core.dependencies import get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()

SLOW_QUERY_MS = 300


class BaseQuery:
    SUPPORTED_FILTERS: Set[str] = set()

    def __init__(self, session):
        self.session = session
        self.params: Dict[str, Any] = {}
        self.filters_sql: List[str] = []
        self.joins: Dict[str, str] = {}
        self.order_by: str = ''
        self.distinct_sql: str = ''
        self.pagination_sql: str = ''

    def _format_websearch(self, sql_string: str) -> str:
        return re.sub(r'%\(([^)]+)\)s', r':\1', sql_string)

    def apply_filters(self, filters: Any):
        for field in self.SUPPORTED_FILTERS:
            value = getattr(filters, field, None)
            # Skip None AND empty strings to avoid errors from frontend
            if value is not None and value != '':
                method_name = f'_apply_{field}_filter'
                if hasattr(self, method_name):
                    getattr(self, method_name)(value)
                else:
                    logger.warning(
                        'filter_implementation_missing',
                        field=field,
                        query_class=self.__class__.__name__,
                    )

    def apply_pagination(self, pagination: Any):
        if pagination:
            self.pagination_sql = (
                f'OFFSET {pagination.offset} LIMIT {pagination.limit}'
            )

    async def execute(self):
        query = self.build_sql()
        start_time = time.perf_counter()

        try:
            result = await self.session.execute(text(query), self.params)
            rows = result.mappings().all()

            duration_ms = (time.perf_counter() - start_time) * 1000

            log_params = {
                'query_class': self.__class__.__name__,
                'duration_ms': round(duration_ms, 2),
                'rows': len(rows),
            }

            # Controle de nível de log das queries
            should_log = True
            log_config = settings.LOG_LEVEL_MIDDLEWARE

            if duration_ms > SLOW_QUERY_MS:
                logger.warning(
                    'sql_slow_query',
                    **log_params,
                    sql=query,
                    params=self.params,
                )
            else:
                if log_config == 'intermediate':
                    should_log = False
                elif log_config == 'error':
                    should_log = False

                if should_log:
                    logger.info('sql_query_success', **log_params)

                logger.debug('sql_debug', sql=query, params=self.params)
            # QUERY
            # print(query, self.params)
            return rows

        except Exception:
            duration_ms = (time.perf_counter() - start_time) * 1000
            logger.exception(
                'sql_query_failed',
                query_class=self.__class__.__name__,
                duration_ms=round(duration_ms, 2),
                sql=query,
                params=self.params,
            )
            raise

    def build_sql(self) -> str:
        raise NotImplementedError('Subclasses must implement build_sql')
