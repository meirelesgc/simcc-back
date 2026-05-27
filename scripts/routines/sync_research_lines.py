import time
import unicodedata
from collections import Counter

import pandas as pd
from numpy import nan
from sqlalchemy import text

from simcc.core.db.database import get_sync_session
from simcc.core.logging import get_logger

logger = get_logger('routines')


def normalize_string(s):
    if not isinstance(s, str):
        return str(s) if s is not None else ''
    s = unicodedata.normalize('NFD', s).replace('\n', '')
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return s.lower()


def load_research_lines_csv(path='storage/seed/program_research_lines.csv'):
    df = pd.read_csv(path)
    df.columns = [normalize_string(col) for col in df.columns]

    df['start_year'] = pd.to_datetime(
        df['data de inicio'], format='%d/%m/%Y', errors='coerce'
    ).dt.year

    df['end_year'] = pd.to_datetime(
        df['data de fim'], format='%d/%m/%Y', errors='coerce'
    ).dt.year

    df['start_year'] = df['start_year'].replace({nan: None})
    df['end_year'] = df['end_year'].replace({nan: None})
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


def main():
    session = next(get_sync_session())
    start_time = time.perf_counter()
    logger.info('research_lines_seed_routine_started')

    try:
        df = load_research_lines_csv()
        total_rows = len(df)

        programs_map = get_programs_mapping(session)

        stats = Counter()
        records_to_insert = []

        for _, row in df.iterrows():
            code = row.get('codigo do programa')
            graduate_program_id = programs_map.get(code)

            if not graduate_program_id:
                stats['Barrado: programa nao encontrado'] += 1
                continue

            records_to_insert.append({
                'graduate_program_id': str(graduate_program_id),
                'name': row.get('nome'),
                'area': row.get('area de concentracao'),
                'start_year': row.get('start_year'),
                'end_year': row.get('end_year'),
            })
            stats['Processado'] += 1

        if records_to_insert:
            query_insert = text("""
                INSERT INTO public.research_lines_programs
                    (graduate_program_id, name, area, start_year, end_year)
                VALUES
                    (:graduate_program_id, :name, :area, :start_year, :end_year)
                ON CONFLICT DO NOTHING;
            """)
            session.execute(query_insert, records_to_insert)

        format_research_lines(session)

        session.commit()
        duration = time.perf_counter() - start_time

        logger.info(
            'research_lines_seed_routine_finished',
            total_processed=total_rows,
            success=stats['Processado'],
            errors=dict(stats),
            duration=f'{duration:.2f}s',
        )

    except Exception as e:
        session.rollback()
        duration = time.perf_counter() - start_time
        logger.error(
            'research_lines_seed_routine_failed',
            error=str(e),
            duration=f'{duration:.2f}s',
        )


if __name__ == '__main__':
    main()
