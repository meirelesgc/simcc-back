import re
import unicodedata
from collections import Counter

import polars as pl
from sqlalchemy import text

from simcc.core.db.database import get_admin_sync_session


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
    df = pl.read_csv(path)
    df = df.rename({col: normalize_string(col) for col in df.columns})
    return df


def get_researchers_mapping(session):
    sql = text('SELECT researcher_id, name FROM researcher')
    result = session.execute(sql).mappings().all()
    if not result:
        return pl.DataFrame(
            schema={
                'researcher_id': pl.String,
                'name': pl.String,
                'match_name': pl.String,
            }
        )
    df_res = pl.DataFrame(result).with_columns(
        pl.col('researcher_id').cast(pl.String)
    )
    df_res = df_res.with_columns(
        pl
        .col('name')
        .map_elements(normalize_name_match, return_dtype=pl.String)
        .alias('match_name')
    )
    df_res = df_res.unique(subset=['match_name'])
    return df_res


def get_programs_mapping(session):
    sql = text('SELECT graduate_program_id, code FROM graduate_program')
    result = session.execute(sql).mappings().all()
    if not result:
        return pl.DataFrame(
            schema={'graduate_program_id': pl.String, 'code': pl.String}
        )
    return pl.DataFrame(result).with_columns(
        pl.col('graduate_program_id').cast(pl.String),
        pl.col('code').cast(pl.String),
    )


def main():
    session = next(get_admin_sync_session())
    try:
        df = load_researchers_csv()
        required_cols = {'nome', 'id do programa', 'categoria'}
        missing = required_cols - set(df.columns)
        if missing:
            return

        df_res = get_researchers_mapping(session)
        df_prog = get_programs_mapping(session)

        df = df.with_columns(
            pl
            .col('nome')
            .map_elements(normalize_name_match, return_dtype=pl.String)
            .alias('match_name'),
            pl
            .col('categoria')
            .map_elements(normalize_categoria, return_dtype=pl.String)
            .alias('categoria'),
            pl.col('id do programa').cast(pl.String),
        )

        if not df_res.is_empty():
            df = df.join(
                df_res.select(['researcher_id', 'match_name']),
                on='match_name',
                how='left',
            )
        else:
            df = df.with_columns(pl.lit(None).alias('researcher_id'))

        if not df_prog.is_empty():
            df = df.join(
                df_prog, left_on='id do programa', right_on='code', how='left'
            )
        else:
            df = df.with_columns(pl.lit(None).alias('graduate_program_id'))

        years = [2026, 2025, 2024, 2023]
        stats = Counter()
        records_to_insert = []

        for row in df.to_dicts():
            r_id = row.get('researcher_id')
            pg_id = row.get('graduate_program_id')

            if r_id is None:
                stats['Barrado: pesquisador nao encontrado'] += 1
                continue

            if pg_id is None:
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
    except Exception as E:
        print(E)
        session.rollback()


if __name__ == '__main__':
    main()
