#!/usr/bin/env python3
"""CLI Script for cleaning up logs older than LOG_RETENTION_DAYS (default 7 days)."""
import argparse
import sys
from simcc.core.logging.cleanup import clean_old_logs


def main():
    parser = argparse.ArgumentParser(
        description='Clean log files older than N days.'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=None,
        help='Number of retention days. Overrides LOG_RETENTION_DAYS setting if provided.',
    )
    parser.add_argument(
        '--dir',
        type=str,
        default=None,
        help='Log directory path. Overrides LOG_DIR setting if provided.',
    )

    args = parser.parse_args()
    deleted = clean_old_logs(log_dir=args.dir, max_age_days=args.days)
    if deleted:
        print(f'Deleted {len(deleted)} old log file(s):')
        for f in deleted:
            print(f'  - {f}')
    else:
        print('No old log files to delete.')


if __name__ == '__main__':
    main()
