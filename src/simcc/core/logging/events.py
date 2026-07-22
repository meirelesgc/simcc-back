import logging
import structlog
from typing import Any, Optional
logger = structlog.get_logger()
from simcc.core.logging.constants import LogCategory, LogEvent

def request_received(method: str, route: str, user_id: Any = None, **kwargs) -> None:
    message = f"Request received: {method} {route}"
    logger.info(
        LogEvent.HTTP_RECEIVED,
        message=message,
        category=LogCategory.HTTP,
        method=method,
        route=route,
        user_id=user_id,
        **kwargs
    )

def request_finished(method: str, route: str, duration: float, user_id: Any = None, **kwargs) -> None:
    message = f"Request finished: {method} {route}"
    logger.info(
        LogEvent.HTTP_FINISHED,
        message=message,
        category=LogCategory.HTTP,
        method=method,
        route=route,
        duration=duration,
        user_id=user_id,
        **kwargs
    )

def request_error(method: str, route: str, duration: float, error: str, user_id: Any = None, **kwargs) -> None:
    message = f"Request error: {method} {route} - {error}"
    logger.error(
        LogEvent.HTTP_ERROR,
        message=message,
        category=LogCategory.HTTP,
        method=method,
        route=route,
        duration=duration,
        error_message=error,
        user_id=user_id,
        **kwargs
    )

def routine_started(routine_name: str, **kwargs) -> None:
    message = f"Routine started: {routine_name}"
    logger.info(
        LogEvent.ROUTINE_STARTED,
        message=message,
        category=LogCategory.ROUTINE,
        routine_name=routine_name,
        **kwargs
    )

def routine_finished(routine_name: str, duration: float, **kwargs) -> None:
    message = f"Routine finished: {routine_name}"
    logger.info(
        LogEvent.ROUTINE_FINISHED,
        message=message,
        category=LogCategory.ROUTINE,
        routine_name=routine_name,
        duration=duration,
        **kwargs
    )

def routine_error(routine_name: str, duration: float, error: str, **kwargs) -> None:
    message = f"Routine error: {routine_name} - {error}"
    logger.error(
        LogEvent.ROUTINE_ERROR,
        message=message,
        category=LogCategory.ROUTINE,
        routine_name=routine_name,
        duration=duration,
        error_message=error,
        **kwargs
    )

def query_error(operation_name: str, database_name: str, error: str, duration: Optional[float] = None, sql: Optional[str] = None, **kwargs) -> None:
    message = f"Database query error in operation: {operation_name} on db: {database_name}"
    
    # We never log SQL in production, only if LOG_LEVEL is DEBUG
    from simcc.core.logging.config import get_configured_log_level
    
    log_data = {
        'operation_name': operation_name,
        'database_name': database_name,
        'error_message': error,
    }
    
    if get_configured_log_level() <= logging.DEBUG and sql:
        log_data['sql'] = sql
        
    logger.error(
        LogEvent.DB_ERROR,
        message=message,
        category=LogCategory.DATABASE,
        duration=duration,
        data=log_data,
        **kwargs
    )
