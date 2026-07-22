import argparse
import datetime
import time

import polars as pl
from sqlalchemy import text

from simcc.core.db.database import get_sync_session



def article_metrics(session, year):
    SCRIPT_SQL = text("""
        SELECT qualis, COUNT(*) AS count_article, researcher_id
        FROM public.bibliographic_production bp
            JOIN public.bibliographic_production_article bpa ON
                bp.id = bpa.bibliographic_production_id
        WHERE type = 'ARTICLE' AND year_ >= :year
        GROUP BY qualis, researcher_id;
    """)

    result = session.execute(SCRIPT_SQL, {'year': year}).mappings().all()
    columns = [
        'researcher_id',
        'A1',
        'A2',
        'A3',
        'A4',
        'B1',
        'B2',
        'B3',
        'B4',
        'C',
        'SQ',
    ]

    if not result:
        return pl.DataFrame(schema={c: pl.Int64 if c != 'researcher_id' else pl.String for c in columns})

    df = pl.DataFrame(result).with_columns(pl.col('researcher_id').cast(pl.String))

    df = df.pivot(
        on='qualis',
        index='researcher_id',
        values='count_article',
        aggregate_function='sum',
    ).fill_null(0)

    for col in columns:
        if col not in df.columns:
            df = df.with_columns(pl.lit(0).alias(col))

    return df.select(columns)


def patent_metrics(session, year):
    SCRIPT_SQL = text("""
        SELECT researcher_id,
            COUNT(*) FILTER (WHERE p.grant_date IS NULL) AS patent_not_granted,
            COUNT(*) FILTER (WHERE p.grant_date IS NOT NULL) AS patent_granted
        FROM patent p
        WHERE development_year::INT >= :year
        GROUP BY researcher_id;
    """)
    result = session.execute(SCRIPT_SQL, {'year': year}).mappings().all()
    columns = ['researcher_id', 'patent_not_granted', 'patent_granted']
    if not result:
        return pl.DataFrame(schema={c: pl.Int64 if c != 'researcher_id' else pl.String for c in columns})
    return pl.DataFrame(result).with_columns(pl.col('researcher_id').cast(pl.String))


def guidance_metrics(session, year):
    SCRIPT_SQL = text("""
        SELECT researcher_id,
            unaccent(lower((g.nature || ' ' || g.status))) AS nature,
            COUNT(*) as count_nature
        FROM guidance g
        WHERE g.year >= :year
        GROUP BY nature, g.status, g.researcher_id;
    """)
    result = session.execute(SCRIPT_SQL, {'year': year}).mappings().all()

    rename_dict = {
        'iniciacao cientifica concluida': 'ic_completed',
        'iniciacao cientifica em andamento': 'ic_in_progress',
        'dissertacao de mestrado concluida': 'm_completed',
        'dissertacao de mestrado em andamento': 'm_in_progress',
        'tese de doutorado concluida': 'd_completed',
        'tese de doutorado em andamento': 'd_in_progress',
        'trabalho de conclusão de curso graduacao concluida': 'g_completed',
        'trabalho de conclusao de curso de graduacao em andamento': 'g_in_progress',
        'monografia de conclusao de curso aperfeicoamento e especializacao concluida': 'e_completed',
        'monografia de conclusao de curso aperfeicoamento e especializacao em andamento': 'e_in_progress',
        'orientacao-de-outra-natureza concluida': 'o_completed',
        'supervisao de pos-doutorado concluida': 'sd_completed',
        'supervisao de pos-doutorado em andamento': 'sd_in_progress',
    }
    columns = ['researcher_id'] + list(rename_dict.values())

    if not result:
        return pl.DataFrame(schema={c: pl.Int64 if c != 'researcher_id' else pl.String for c in columns})

    df = pl.DataFrame(result).with_columns(pl.col('researcher_id').cast(pl.String))
    df = df.pivot(
        on='nature',
        index='researcher_id',
        values='count_nature',
        aggregate_function='sum',
    ).fill_null(0)

    rename_existing = {k: v for k, v in rename_dict.items() if k in df.columns}
    df = df.rename(rename_existing)

    for col in columns:
        if col not in df.columns:
            df = df.with_columns(pl.lit(0).alias(col))

    return df.select(columns)


def academic_degree_metrics(session):
    SCRIPT_SQL = text("""
        SELECT researcher_id, MAX(education_end) AS first_doc
        FROM education
        WHERE degree = 'DOCTORATE'
        GROUP BY researcher_id
    """)
    result = session.execute(SCRIPT_SQL).mappings().all()
    if not result:
        return pl.DataFrame(schema={'researcher_id': pl.String, 'first_doc': pl.Int64})
    return pl.DataFrame(result).with_columns(
        pl.col('researcher_id').cast(pl.String),
        pl.col('first_doc').cast(pl.Int64)
    )


def simple_count_metrics(session, sql, params, column_name):
    result = session.execute(text(sql), params).mappings().all()
    if not result:
        return pl.DataFrame(schema={'researcher_id': pl.String, column_name: pl.Int64})
    return pl.DataFrame(result).with_columns(pl.col('researcher_id').cast(pl.String))


def list_researchers(session, researcher_ids=None, lattes_ids=None):
    base_query = """
        SELECT id AS researcher_id, name, lattes_id
        FROM public.researcher
        WHERE 1=1
    """
    params = {}

    if researcher_ids:
        base_query += ' AND id IN (:researcher_ids)'
        params['researcher_ids'] = tuple(researcher_ids)

    if lattes_ids:
        base_query += ' AND lattes_id IN (:lattes_ids)'
        params['lattes_ids'] = tuple(lattes_ids)

    script_sql = text(base_query)

    if params:
        result = session.execute(script_sql, params)
    else:
        result = session.execute(script_sql)

    res_list = result.mappings().all()
    if not res_list:
        return pl.DataFrame(schema={'researcher_id': pl.String, 'name': pl.String, 'lattes_id': pl.String})
    return pl.DataFrame(res_list).with_columns(pl.col('researcher_id').cast(pl.String))


def researcher_classification(researcher: dict) -> str:
    if researcher['first_doc'] == 0:
        return 'E'

    YEAR_DOC = datetime.datetime.now().year - researcher['first_doc']
    QUALIS_A = (
        researcher['A1']
        + researcher['A2']
        + researcher['A3']
        + researcher['A4']
    )
    QUALIS_B = (
        researcher['B1']
        + researcher['B2']
        + researcher['B3']
        + researcher['B4']
    )
    QUALIS_NA = researcher['C'] + researcher['SQ']

    ARTICLES = QUALIS_A + QUALIS_B + QUALIS_NA
    ACADEMIC_PRODUCTION = (
        ARTICLES + researcher['book'] + researcher['book_chapter']
    )

    DOC_GUIDANCE = researcher['d_completed'] + researcher['d_in_progress']
    MASTERS_GUIDANCE = researcher['m_completed'] + researcher['m_in_progress']

    PATENT = researcher['patent_granted'] + researcher['patent_not_granted']
    SOFTWARE = researcher['software']

    if YEAR_DOC >= 10:
        PROD_RULE = ACADEMIC_PRODUCTION >= 5 and researcher['A1'] >= 2
        GUIDANCE_RULE = DOC_GUIDANCE >= 4
        COMBINED_RULE = researcher['A1'] >= 1 and PATENT >= 1
        if (PROD_RULE and GUIDANCE_RULE) or COMBINED_RULE:
            return 'A+'

        PROD_RULE = ACADEMIC_PRODUCTION >= 5 and researcher['A1'] >= 1
        GUIDANCE_RULE = DOC_GUIDANCE >= 2
        COMBINED_RULE = PATENT >= 1
        if (PROD_RULE and GUIDANCE_RULE) or COMBINED_RULE:
            return 'A'

    if YEAR_DOC >= 8:
        PROD_RULE = ACADEMIC_PRODUCTION >= 4 and QUALIS_A >= 2
        GUIDANCE_RULE = MASTERS_GUIDANCE >= 2 or DOC_GUIDANCE >= 1
        COMBINED_RULE = QUALIS_A >= 1 and (PATENT >= 1 or SOFTWARE >= 3)
        if (PROD_RULE and GUIDANCE_RULE) or COMBINED_RULE:
            return 'B+'

        PROD_RULE = ACADEMIC_PRODUCTION >= 4 and QUALIS_A >= 1
        GUIDANCE_RULE = MASTERS_GUIDANCE >= 2 or DOC_GUIDANCE >= 1
        COMBINED_RULE = PATENT >= 1 or SOFTWARE >= 3
        if (PROD_RULE and GUIDANCE_RULE) or COMBINED_RULE:
            return 'B'

    if YEAR_DOC >= 6:
        PROD_RULE = ACADEMIC_PRODUCTION >= 3 and QUALIS_A >= 2
        GUIDANCE_RULE = MASTERS_GUIDANCE >= 1 or DOC_GUIDANCE >= 1
        COMBINED_RULE = QUALIS_A >= 1 and (PATENT >= 1 or SOFTWARE >= 3)
        if (PROD_RULE and GUIDANCE_RULE) or COMBINED_RULE:
            return 'C+'

        PROD_RULE = ACADEMIC_PRODUCTION >= 3 and QUALIS_A >= 1
        GUIDANCE_RULE = MASTERS_GUIDANCE >= 1 or DOC_GUIDANCE >= 1
        COMBINED_RULE = PATENT >= 1 or SOFTWARE >= 3
        if (PROD_RULE and GUIDANCE_RULE) or COMBINED_RULE:
            return 'C'

    if YEAR_DOC >= 3:
        PROD_RULE = ACADEMIC_PRODUCTION >= 2 and QUALIS_A >= 1
        COMBINED_RULE = PATENT >= 1 or SOFTWARE >= 3
        if PROD_RULE or COMBINED_RULE:
            return 'D+'

        PROD_RULE = ACADEMIC_PRODUCTION >= 2 and ARTICLES >= 1
        COMBINED_RULE = PATENT >= 1 or SOFTWARE >= 3
        if PROD_RULE or COMBINED_RULE:
            return 'D'

    if YEAR_DOC > 0:
        PROD_RULE = ACADEMIC_PRODUCTION >= 1
        COMBINED_RULE = QUALIS_A >= 1 and (PATENT >= 1 or SOFTWARE >= 3)
        if PROD_RULE or COMBINED_RULE:
            return 'E+'

    return 'E'


def main(researcher_ids=None, lattes_ids=None):
    YEAR_FILTER = 2019
    session = next(get_sync_session())
    start_time = time.perf_counter()


    try:
        dataframe = list_researchers(session, researcher_ids, lattes_ids)

        if dataframe.is_empty():
            return

        metrics_calls = [
            (article_metrics, [YEAR_FILTER]),
            (patent_metrics, [YEAR_FILTER]),
            (guidance_metrics, [YEAR_FILTER]),
            (academic_degree_metrics, []),
            (
                simple_count_metrics,
                [
                    """
                SELECT researcher_id, COUNT(*) AS software
                FROM public.software s
                WHERE s.year >= :year
                GROUP BY researcher_id;
                """,
                    {'year': YEAR_FILTER},
                    'software',
                ],
            ),
            (
                simple_count_metrics,
                [
                    """
                SELECT researcher_id, COUNT(*) AS book
                FROM bibliographic_production
                WHERE type = 'BOOK' AND year_ >= :year
                GROUP BY researcher_id
                """,
                    {'year': YEAR_FILTER},
                    'book',
                ],
            ),
            (
                simple_count_metrics,
                [
                    """
                SELECT researcher_id, COUNT(*) AS book_chapter
                FROM bibliographic_production
                WHERE type = 'BOOK_CHAPTER' AND year_ >= :year
                GROUP BY researcher_id
                """,
                    {'year': YEAR_FILTER},
                    'book_chapter',
                ],
            ),
            (
                simple_count_metrics,
                [
                    """
                SELECT researcher_id, COUNT(*) AS brand
                FROM public.brand b
                WHERE b.year >= :year
                GROUP BY researcher_id;
                """,
                    {'year': YEAR_FILTER},
                    'brand',
                ],
            ),
        ]

        for func, args in metrics_calls:
            m_df = func(session, *args)
            dataframe = dataframe.join(m_df, how='left', on='researcher_id')

        dataframe = dataframe.fill_null(0)

        classes = []
        for row in dataframe.to_dicts():
            classes.append(researcher_classification(row))
        dataframe = dataframe.with_columns(pl.Series(name='class', values=classes))

        UPDATE_SQL = text("""
            UPDATE researcher
            SET classification = :class
            WHERE id = :researcher_id
        """)

        for row in dataframe.to_dicts():
            session.execute(
                UPDATE_SQL,
                {
                    'class': row['class'],
                    'researcher_id': row['researcher_id'],
                },
            )

        session.commit()
        duration = time.perf_counter() - start_time
    except Exception as e:
        session.rollback()
        duration = time.perf_counter() - start_time


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--researcher-ids',
        nargs='+',
        type=str,
        default=None,
    )
    parser.add_argument(
        '--lattes-ids',
        nargs='+',
        type=str,
        default=None,
    )
    args = parser.parse_args()

    main(researcher_ids=args.researcher_ids, lattes_ids=args.lattes_ids)

