from enum import Enum

class LogCategory(str, Enum):
    HTTP = 'http'
    DATABASE = 'database'
    ROUTINE = 'routine'
    FRONTEND = 'frontend'
    SYSTEM = 'system'

class LogEvent(str, Enum):
    HTTP_RECEIVED = 'request.received'
    HTTP_FINISHED = 'request.finished'
    HTTP_ERROR = 'request.error'
    
    DB_ERROR = 'query.error'
    
    ROUTINE_STARTED = 'routine.started'
    ROUTINE_FINISHED = 'routine.finished'
    ROUTINE_ERROR = 'routine.error'
    ROUTINE_STEP_STARTED = 'routine.step.started'
    ROUTINE_STEP_FINISHED = 'routine.step.finished'
    ROUTINE_PROGRESS = 'routine.progress'
    ROUTINE_ITEM_ERROR = 'routine.item_error'

