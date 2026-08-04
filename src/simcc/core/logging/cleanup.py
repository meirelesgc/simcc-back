import os
import re
import sys
from datetime import datetime, timedelta


def clean_old_logs(
    log_dir: str | None = None, max_age_days: int | None = None
) -> list[str]:
    """
    Deletes log files in log_dir older than max_age_days.
    Returns a list of deleted file paths.
    """
    try:
        from simcc.core.settings import Settings

        settings = Settings()
        if log_dir is None:
            log_dir = settings.LOG_DIR
        if max_age_days is None:
            max_age_days = settings.LOG_RETENTION_DAYS
    except Exception:
        if log_dir is None:
            log_dir = 'logs'
        if max_age_days is None:
            max_age_days = 7

    if not os.path.exists(log_dir):
        return []

    cutoff_date = datetime.now() - timedelta(days=max_age_days)
    deleted_files: list[str] = []

    date_pattern = re.compile(r'^(\d{4}-\d{2}-\d{2})\.jsonl$')

    for entry in os.scandir(log_dir):
        if not entry.is_file():
            continue

        match = date_pattern.match(entry.name)
        should_delete = False

        if match:
            try:
                file_date = datetime.strptime(match.group(1), '%Y-%m-%d')
                if file_date.date() < cutoff_date.date():
                    should_delete = True
            except ValueError:
                pass

        # Fallback to mtime if filename didn't match standard YYYY-MM-DD.jsonl
        if not match and entry.name.endswith('.jsonl'):
            mtime = datetime.fromtimestamp(entry.stat().st_mtime)
            if mtime < cutoff_date:
                should_delete = True

        if should_delete:
            try:
                os.remove(entry.path)
                deleted_files.append(entry.path)
            except OSError as e:
                sys.stderr.write(
                    f'[Log Cleanup Error] Failed to delete {entry.path}: {e}\n'
                )

    return deleted_files
