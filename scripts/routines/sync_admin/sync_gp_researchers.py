import re
import time
import unicodedata
from collections import Counter

import pandas as pd
from sqlalchemy import text

from simcc.core.db.database import get_admin_sync_session
from simcc.core.logging import get_logger

logger = get_logger('routines')


def normalize_string(s):
    if not isinstance(s, str):
        return str(s) if s is not None else ''
    s = unicodedata.normalize('NFD', s).replace('\n', '')
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return s.lower()


def normalize_name_match(name):
    if not isinstance(name, str):
        return ''
    name = unicodedata.normalize('NFD', name)
    name = name.encode('ascii', 'ignore').decode('utf-8').upper()
    return re.sub(r'[^A-Z0-9]', '', name)


def normalize_categoria(value):
    v = str(value) if value is not None else ''
    return 'PERMANENTE' if 'PERMANENTE' in v.upper() else 'COLABORADOR'


def load_researchers_csv(path='storage/seed/program_researchers.csv'):
    df = pd.read_csv(path)
    df.columns = [normalize_string(col) for col in df.columns]
    return df


def get_researchers_mapping(session):
    sql = text('SELECT researcher_id, name FROM researcher')
    result = session.execute(sql).mappings().all()
    df_res = pd.DataFrame(result)
    if not df_res.empty:
        df_res['match_name'] = df_res['name'].apply(normalize_name_match)
        df_res = df_res.drop_duplicates(subset=['match_name'])
    return df_res


def get_programs_mapping(session):
    sql = text('SELECT graduate_program_id, code FROM graduate_program')
    result = session.execute(sql).mappings().all()
    return pd.DataFrame(result)


def main():
    session = next(get_admin_sync_session())
    start_time = time.perf_counter()
    logger.info('program_researchers_seed_routine_started')

    try:
        df = load_researchers_csv()
        total_rows = len(df)

        required_cols = {'nome', 'id do programa', 'categoria'}
        missing = required_cols - set(df.columns)
        if missing:
            logger.error('csv_missing_columns', missing=list(missing))
            return

        df_res = get_researchers_mapping(session)
        df_prog = get_programs_mapping(session)

        df['match_name'] = df['nome'].apply(normalize_name_match)
        df['categoria'] = df['categoria'].apply(normalize_categoria)

        if not df_res.empty:
            df = df.merge(
                df_res[['researcher_id', 'match_name']],
                on='match_name',
                how='left',
            )
        else:
            df['researcher_id'] = None

        if not df_prog.empty:
            df = df.merge(
                df_prog, left_on='id do programa', right_on='code', how='left'
            )
        else:
            df['graduate_program_id'] = None

        years = [2026, 2025, 2024, 2023]
        stats = Counter()
        records_to_insert = []

        for _, row in df.iterrows():
            r_id = row.get('researcher_id')
            pg_id = row.get('graduate_program_id')

            if pd.isna(r_id):
                stats['Barrado: pesquisador nao encontrado'] += 1
                continue

            if pd.isna(pg_id):
                stats['Barrado: programa nao encontrado'] += 1
                continue

            records_to_insert.append({
                'graduate_program_id': str(pg_id),
                'researcher_id': str(r_id),
                'year': years,
                'type_': row['categoria'],
            })
            stats['Processado'] += 1

        if records_to_insert:
            query_insert = text("""
                INSERT INTO public.graduate_program_researcher
                    (graduate_program_id, researcher_id, year, type_)
                VALUES
                    (:graduate_program_id, :researcher_id, :year, :type_)
                ON CONFLICT (graduate_program_id, researcher_id) DO UPDATE SET
                    year = EXCLUDED.year,
                    type_ = EXCLUDED.type_;
            """)
            session.execute(query_insert, records_to_insert)

        session.commit()
        duration = time.perf_counter() - start_time

        logger.info(
            'program_researchers_seed_routine_finished',
            total_processed=total_rows,
            success=stats['Processado'],
            errors=dict(stats),
            duration=f'{duration:.2f}s',
        )

    except Exception as e:
        session.rollback()
        duration = time.perf_counter() - start_time
        logger.error(
            'program_researchers_seed_routine_failed',
            error=str(e),
            duration=f'{duration:.2f}s',
        )


if __name__ == '__main__':
    main()
