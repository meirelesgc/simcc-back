import time
import unicodedata
from collections import Counter

import polars as pl
from sqlalchemy import text

from simcc.core.db.database import get_sync_session
from simcc.core.logging import logger
from simcc.core.logging.events import (
    routine_item_error,
    routine_progress,
    routine_step_finished,
    routine_step_started,
)


def normalize_string(s):
    if not isinstance(s, str):
        return str(s) if s is not None else ''
    s = unicodedata.normalize('NFD', s).replace('\n', '')
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return s.lower()


def load_research_lines_csv(path='storage/seed/program_research_lines.csv'):
    df = pl.read_csv(path)
    df = df.rename({col: normalize_string(col) for col in df.columns})

    df = df.with_columns(
        pl
        .col('data de inicio')
        .str.to_date(format='%d/%m/%Y', strict=False)
        .dt.year()
        .alias('start_year'),
        pl
        .col('data de fim')
        .str.to_date(format='%d/%m/%Y', strict=False)
        .dt.year()
        .alias('end_year'),
    )
    return df


def get_programs_mapping(session):
    sql = text('SELECT graduate_program_id, code FROM graduate_program')
    result = session.execute(sql).mappings().all()
    return {row['code']: row['graduate_program_id'] for row in result}


def format_research_lines(session):
    sql = text("""
        UPDATE research_lines_programs SET
            name = INITCAP(name),
            area = UPPER(area);
    """)
    session.execute(sql)


items_found = 0
items_succeeded = 0
items_failed = 0


def main():
    global items_found, items_succeeded, items_failed
    session = next(get_sync_session())
    start_time = time.perf_counter()

    try:
        routine_step_started('process_research_lines_csv')
        df = load_research_lines_csv()
        total_rows = len(df)
        items_found = total_rows

        programs_map = get_programs_mapping(session)

        stats = Counter()
        records_to_insert = []

        records = df.to_dicts()
        for idx, row in enumerate(records):
            code = row.get('codigo do programa')
            graduate_program_id = programs_map.get(code)
            line_name = row.get('nome')

            if not graduate_program_id:
                stats['Barrado: programa nao encontrado'] += 1
                routine_item_error(
                    str(line_name),
                    'Programa de pós não encontrado no banco',
                    programa_codigo=code,
                )
                continue

            records_to_insert.append({
                'graduate_program_id': str(graduate_program_id),
                'name': line_name,
                'area': row.get('area de concentracao'),
                'start_year': row.get('start_year'),
                'end_year': row.get('end_year'),
            })
            stats['Processado'] += 1

            if (idx + 1) % 50 == 0 or (idx + 1) == total_rows:
                routine_progress(
                    'process_research_lines_csv',
                    idx + 1,
                    total_rows,
                    stats['Processado'],
                    total_rows - stats['Processado'],
                )

        routine_step_finished(
            'process_research_lines_csv', total_processed=stats['Processado']
        )

        if records_to_insert:
            routine_step_started('insert_research_lines')
            query_insert = text("""
                INSERT INTO public.research_lines_programs
                    (graduate_program_id, name, area, start_year, end_year)
                VALUES
                    (:graduate_program_id, :name, :area, :start_year, :end_year)
                ON CONFLICT DO NOTHING;
            """)
            session.execute(query_insert, records_to_insert)
            format_research_lines(session)
            routine_step_finished(
                'insert_research_lines', total_inserted=len(records_to_insert)
            )

        session.commit()
        items_succeeded = stats['Processado']
        items_failed = total_rows - items_succeeded
        duration = time.perf_counter() - start_time
    except Exception as e:
        items_succeeded = 0
        items_failed = items_found
        logger.error(f'Error in sync_research_lines: {e}')
        session.rollback()
        duration = time.perf_counter() - start_time
        raise e


if __name__ == '__main__':
    main()
