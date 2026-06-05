import argparse
import datetime
import time

import pandas as pd
from sqlalchemy import text

from simcc.core.db.database import get_sync_session
from simcc.core.logging import get_logger

logger = get_logger('routines')


def article_metrics(session, year, researcher_id=None):
    if researcher_id:
        SCRIPT_SQL = text("""
            SELECT qualis, COUNT(*) AS count_article, researcher_id
            FROM public.bibliographic_production bp
                JOIN public.bibliographic_production_article bpa ON
                    bp.id = bpa.bibliographic_production_id
            WHERE type = 'ARTICLE' AND year_ >= :year
            AND bp.researcher_id = CAST(:researcher_id AS UUID)
            GROUP BY qualis, researcher_id;
        """)
        result = session.execute(
            SCRIPT_SQL, {'year': year, 'researcher_id': researcher_id}
        ).mappings().all()
    else:
        SCRIPT_SQL = text("""
            SELECT qualis, COUNT(*) AS count_article, researcher_id
            FROM public.bibliographic_production bp
                JOIN public.bibliographic_production_article bpa ON
                    bp.id = bpa.bibliographic_production_id
            WHERE type = 'ARTICLE' AND year_ >= :year
            GROUP BY qualis, researcher_id;
        """)
        result = session.execute(SCRIPT_SQL, {'year': year}).mappings().all()

    articles = pd.DataFrame(result)

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

    if articles.empty:
        return pd.DataFrame(columns=columns)

    articles = articles.pivot_table(
        index=['researcher_id'],
        columns='qualis',
        values='count_article',
        aggfunc='sum',
        fill_value=0,
    ).reset_index()

    articles = articles.reindex(columns, axis='columns', fill_value=0)
    return articles


def patent_metrics(session, year, researcher_id=None):
    if researcher_id:
        SCRIPT_SQL = text("""
            SELECT researcher_id,
                COUNT(*) FILTER (WHERE p.grant_date IS NULL) AS patent_not_granted,
                COUNT(*) FILTER (WHERE p.grant_date IS NOT NULL) AS patent_granted
            FROM patent p
            WHERE development_year::INT >= :year
            AND researcher_id = CAST(:researcher_id AS UUID)
            GROUP BY researcher_id;
        """)
        result = session.execute(
            SCRIPT_SQL, {'year': year, 'researcher_id': researcher_id}
        ).mappings().all()
    else:
        SCRIPT_SQL = text("""
            SELECT researcher_id,
                COUNT(*) FILTER (WHERE p.grant_date IS NULL) AS patent_not_granted,
                COUNT(*) FILTER (WHERE p.grant_date IS NOT NULL) AS patent_granted
            FROM patent p
            WHERE development_year::INT >= :year
            GROUP BY researcher_id;
        """)
        result = session.execute(SCRIPT_SQL, {'year': year}).mappings().all()

    df = pd.DataFrame(result)
    if df.empty:
        return pd.DataFrame(
            columns=['researcher_id', 'patent_not_granted', 'patent_granted']
        )
    return df


def guidance_metrics(session, year, researcher_id=None):
    if researcher_id:
        SCRIPT_SQL = text("""
            SELECT researcher_id,
                unaccent(lower((g.nature || ' ' || g.status))) AS nature,
                COUNT(*) as count_nature
            FROM guidance g
            WHERE g.year >= :year
            AND g.researcher_id = CAST(:researcher_id AS UUID)
            GROUP BY nature, g.status, g.researcher_id;
        """)
        result = session.execute(
            SCRIPT_SQL, {'year': year, 'researcher_id': researcher_id}
        ).mappings().all()
    else:
        SCRIPT_SQL = text("""
            SELECT researcher_id,
                unaccent(lower((g.nature || ' ' || g.status))) AS nature,
                COUNT(*) as count_nature
            FROM guidance g
            WHERE g.year >= :year
            GROUP BY nature, g.status, g.researcher_id;
        """)
        result = session.execute(SCRIPT_SQL, {'year': year}).mappings().all()

    guidance = pd.DataFrame(result)

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

    if guidance.empty:
        return pd.DataFrame(columns=columns)

    guidance = (
        guidance
        .pivot_table(
            index=['researcher_id'],
            columns='nature',
            values='count_nature',
            aggfunc='sum',
            fill_value=0,
        )
        .reset_index()
        .rename(columns=rename_dict)
    )

    guidance = guidance.reindex(columns, axis='columns', fill_value=0)
    return guidance


def academic_degree_metrics(session, researcher_id=None):
    if researcher_id:
        SCRIPT_SQL = text("""
            SELECT researcher_id, MAX(education_end) AS first_doc
            FROM education
            WHERE degree = 'DOCTORATE'
            AND researcher_id = CAST(:researcher_id AS UUID)
            GROUP BY researcher_id
        """)
        result = session.execute(
            SCRIPT_SQL, {'researcher_id': researcher_id}
        ).mappings().all()
    else:
        SCRIPT_SQL = text("""
            SELECT researcher_id, MAX(education_end) AS first_doc
            FROM education
            WHERE degree = 'DOCTORATE'
            GROUP BY researcher_id
        """)
        result = session.execute(SCRIPT_SQL).mappings().all()

    df = pd.DataFrame(result)
    if df.empty:
        return pd.DataFrame(columns=['researcher_id', 'first_doc'])
    return df


def simple_count_metrics(session, sql, params, column_name):
    result = session.execute(text(sql), params).mappings().all()
    df = pd.DataFrame(result)
    if df.empty:
        return pd.DataFrame(columns=['researcher_id', column_name])
    return df


def list_researchers(session, researcher_id=None):
    if researcher_id:
        SCRIPT_SQL = text("""
            SELECT id AS researcher_id, name, lattes_id
            FROM public.researcher
            WHERE id = CAST(:researcher_id AS UUID)
        """)
        result = session.execute(
            SCRIPT_SQL, {'researcher_id': researcher_id}
        ).mappings().all()
    else:
        SCRIPT_SQL = text("""
            SELECT id AS researcher_id, name, lattes_id
            FROM public.researcher
        """)
        result = session.execute(SCRIPT_SQL).mappings().all()
    return pd.DataFrame(result)


def researcher_classification(researcher: pd.Series) -> str:
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


def main(researcher_id: str | None = None):
    YEAR_FILTER = 2019
    session = next(get_sync_session())
    start_time = time.perf_counter()
    logger.info('researcher_classification_routine_started')

    try:
        dataframe = list_researchers(session, researcher_id)

        if dataframe.empty:
            duration = time.perf_counter() - start_time
            logger.info(
                'researcher_classification_routine_no_data',
                duration=f'{duration:.2f}s',
            )
            return

        rid_filter = {'year': YEAR_FILTER, 'researcher_id': researcher_id} if researcher_id else {'year': YEAR_FILTER}

        def rid_sql(base_sql):
            if researcher_id:
                return base_sql + ' AND researcher_id = CAST(:researcher_id AS UUID)'
            return base_sql

        metrics_calls = [
            (article_metrics, [YEAR_FILTER, researcher_id]),
            (patent_metrics, [YEAR_FILTER, researcher_id]),
            (guidance_metrics, [YEAR_FILTER, researcher_id]),
            (academic_degree_metrics, [researcher_id]),
            (
                simple_count_metrics,
                [
                    rid_sql("""
                SELECT researcher_id, COUNT(*) AS software
                FROM public.software s
                WHERE s.year >= :year""") + ' GROUP BY researcher_id;',
                    rid_filter,
                    'software',
                ],
            ),
            (
                simple_count_metrics,
                [
                    rid_sql("""
                SELECT researcher_id, COUNT(*) AS book
                FROM bibliographic_production
                WHERE type = 'BOOK' AND year_ >= :year""") + ' GROUP BY researcher_id',
                    rid_filter,
                    'book',
                ],
            ),
            (
                simple_count_metrics,
                [
                    rid_sql("""
                SELECT researcher_id, COUNT(*) AS book_chapter
                FROM bibliographic_production
                WHERE type = 'BOOK_CHAPTER' AND year_ >= :year""") + ' GROUP BY researcher_id',
                    rid_filter,
                    'book_chapter',
                ],
            ),
            (
                simple_count_metrics,
                [
                    rid_sql("""
                SELECT researcher_id, COUNT(*) AS brand
                FROM public.brand b
                WHERE b.year >= :year""") + ' GROUP BY researcher_id;',
                    rid_filter,
                    'brand',
                ],
            ),
        ]

        for func, args in metrics_calls:
            m_df = func(session, *args)
            dataframe = dataframe.merge(m_df, how='left', on='researcher_id')

        dataframe = dataframe.fillna(0)

        dataframe['class'] = dataframe.apply(researcher_classification, axis=1)

        UPDATE_SQL = text("""
            UPDATE researcher
            SET classification = :class
            WHERE id = :researcher_id
        """)

        for _, researcher in dataframe.iterrows():
            session.execute(
                UPDATE_SQL,
                {
                    'class': researcher['class'],
                    'researcher_id': researcher['researcher_id'],
                },
            )

        session.commit()
        duration = time.perf_counter() - start_time
        logger.info(
            'researcher_classification_routine_finished_successfully',
            duration=f'{duration:.2f}s',
        )
    except Exception as e:
        session.rollback()
        duration = time.perf_counter() - start_time
        logger.error(
            'researcher_classification_routine_failed',
            error=str(e),
            duration=f'{duration:.2f}s',
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--researcher-id', type=str, default=None)
    args = parser.parse_args()
    main(researcher_id=args.researcher_id)
