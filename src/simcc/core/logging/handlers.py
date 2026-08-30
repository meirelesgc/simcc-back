import json
import os
import sys
from datetime import datetime
from typing import Any, Callable, Dict, List

from simcc.core.logging.cleanup import clean_old_logs

try:
    from simcc.core.settings import Settings

    settings = Settings()
    LOG_DIR = settings.LOG_DIR
except Exception:
    LOG_DIR = 'logs'

_last_cleanup_date: str | None = None


def write_to_file(log_data: Dict[str, Any]) -> None:
    """Writes a log entry dictionary as a single line in JSONL format to a file."""
    global _last_cleanup_date
    os.makedirs(LOG_DIR, exist_ok=True)
    date_str = datetime.now().strftime('%Y-%m-%d')

    if _last_cleanup_date != date_str:
        _last_cleanup_date = date_str
        try:
            clean_old_logs(LOG_DIR)
        except Exception:
            pass

    filepath = os.path.join(LOG_DIR, f'{date_str}.jsonl')

    # Serialize structure to JSON using default=str to catch non-serializable objects
    log_line = json.dumps(log_data, default=str)

    with open(filepath, 'a', encoding='utf-8') as f:
        f.write(log_line + '\n')


def write_to_console(log_data: Dict[str, Any]) -> None:
    """Writes a log entry dictionary as a single line to console (stdout)."""
    # Write to stdout
    sys.stdout.write(json.dumps(log_data, default=str) + '\n')
    sys.stdout.flush()


# List of log destination callbacks. Can be modified/extended to support queues, APIs, etc.
LOG_DESTINATIONS: List[Callable[[Dict[str, Any]], None]] = [
    write_to_file,
]


def dispatch_log(log_data: Dict[str, Any]) -> None:
    """Dispatches a log entry to all registered destinations."""
    for dest in LOG_DESTINATIONS:
        try:
            dest(log_data)
        except Exception as e:
            # Backup error reporting to stderr in case a destination fails
            sys.stderr.write(f"[Logging Handler Error] Failed to write log: {e}\n")
            sys.stderr.flush()
