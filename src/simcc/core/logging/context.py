import contextvars
import socket
from typing import Any, Dict

try:
    from simcc.core.settings import Settings

    settings = Settings()
    app_default = settings.APPLICATION
    env_default = settings.ENVIRONMENT
except Exception:
    app_default = 'simcc'
    env_default = 'development'

# Create context variables
request_id_ctx: contextvars.ContextVar[Any] = contextvars.ContextVar(
    'request_id', default=None
)
application_ctx: contextvars.ContextVar[str] = contextvars.ContextVar(
    'application', default=app_default
)
environment_ctx: contextvars.ContextVar[str] = contextvars.ContextVar(
    'environment', default=env_default
)
hostname_ctx: contextvars.ContextVar[str] = contextvars.ContextVar(
    'hostname', default=socket.gethostname()
)
user_id_ctx: contextvars.ContextVar[Any] = contextvars.ContextVar(
    'user_id', default=None
)
route_ctx: contextvars.ContextVar[Any] = contextvars.ContextVar(
    'route', default=None
)
method_ctx: contextvars.ContextVar[Any] = contextvars.ContextVar(
    'method', default=None
)
routine_name_ctx: contextvars.ContextVar[Any] = contextvars.ContextVar(
    'routine_name', default=None
)
script_name_ctx: contextvars.ContextVar[Any] = contextvars.ContextVar(
    'script_name', default=None
)


def get_logging_context() -> Dict[str, Any]:
    """Retrieve all current logging context variables."""
    return {
        'request_id': request_id_ctx.get(),
        'application': application_ctx.get(),
        'environment': environment_ctx.get(),
        'hostname': hostname_ctx.get(),
        'user_id': user_id_ctx.get(),
        'route': route_ctx.get(),
        'method': method_ctx.get(),
        'routine_name': routine_name_ctx.get(),
        'script_name': script_name_ctx.get(),
    }


def clear_logging_context() -> None:
    """Clear context variables that change per request or routine."""
    request_id_ctx.set(None)
    user_id_ctx.set(None)
    route_ctx.set(None)
    method_ctx.set(None)
    routine_name_ctx.set(None)
    script_name_ctx.set(None)
