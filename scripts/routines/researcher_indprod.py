import argparse
import time
from datetime import datetime
from uuid import uuid4

import polars as pl
from sqlalchemy import text
from unidecode import unidecode

from simcc.core.db.database import get_sync_session


barema = {
    'A1': 1.0,
    'A2': 0.875,
    'A3': 0.75,
    'A4': 0.625,
    'B1': 0.5,
    'B2': 0.375,
    'B3': 0.25,
    'B4': 0.125,
    'C': 0.0,
    'SQ': 0.0,
    'BOOK': 1.0,
    'BOOK_CHAPTER': 0.25,
    'SOFTWARE': 0.25,
    'PATENT_GRANTED': 1.0,
    'PATENT_NOT_GRANTED': 0.25,
    'REPORT': 0.25,
    'TESE DE DOUTORADO CONCLUIDA': 0.5,
    'TESE DE DOUTORADO EM ANDAMENTO': 0.25,
    'DISSERTACAO DE MESTRADO CONCLUIDA': 0.25,
    'DISSERTACAO DE MESTRADO EM ANDAMENTO': 0.125,
    'INICIACAO CIENTIFICA CONCLUIDA': 0.125,
    'INICIACAO CIENTIFICA EM ANDAMENTO': 0.1,
}


def article_indprod(session):
    SCRIPT_SQL = text("""
        SELECT year_ AS year, qualis, COUNT(*) AS count_article, researcher_id
        FROM bibliographic_production bp
        RIGHT JOIN bibliographic_production_article bpa
            ON bp.id = bpa.bibliographic_production_id
        GROUP BY year_, qualis, researcher_id;
    """)
    result = session.execute(SCRIPT_SQL).mappings().all()
    if not result:
        return [{'year': 0, 'researcher_id': uuid4(), 'article_prod': 0.0}]

    df = pl.DataFrame(result)
    df = df.with_columns(
        (pl.col('qualis').replace_strict(barema, default=0.0).cast(pl.Float64) * pl.col('count_article')).alias('article_prod'),
        pl.col('year').cast(pl.Int64)
    )
    df = df.group_by(['year', 'researcher_id']).agg(pl.col('article_prod').sum())
    return df.select(['year', 'researcher_id', 'article_prod']).to_dicts()


def book_indprod(session):
    SCRIPT_SQL = text("""
        SELECT year, COUNT(*) AS count_book, researcher_id
        FROM bibliographic_production bp
        WHERE type = 'BOOK'
        GROUP BY year, researcher_id;
    """)
    result = session.execute(SCRIPT_SQL).mappings().all()
    if not result:
        return []
    df = pl.DataFrame(result)
    df = df.with_columns(
        (pl.col('count_book') * barema.get('BOOK', 0.0)).alias('book_prod'),
        pl.col('year').cast(pl.Int64)
    )
    return df.select(['year', 'researcher_id', 'book_prod']).to_dicts()


def book_chapter_indprod(session):
    SCRIPT_SQL = text("""
        SELECT year, COUNT(*) AS count_book_chapter, researcher_id
        FROM bibliographic_production bp
        WHERE type = 'BOOK_CHAPTER'
        GROUP BY year, researcher_id;
    """)
    result = session.execute(SCRIPT_SQL).mappings().all()
    if not result:
        return []
    df = pl.DataFrame(result)
    df = df.with_columns(
        (pl.col('count_book_chapter') * barema.get('BOOK_CHAPTER', 0.0)).alias('book_chapter_prod'),
        pl.col('year').cast(pl.Int64)
    )
    return df.select(['year', 'researcher_id', 'book_chapter_prod']).to_dicts()


def patent_indprod(session):
    SCRIPT_SQL = text("""
        SELECT development_year AS year, 'PATENT_GRANTED' AS granted,
            researcher_id, COUNT(*) as count_patent
        FROM patent p
        WHERE grant_date IS NOT NULL
        GROUP BY development_year, researcher_id

        UNION

        SELECT development_year AS year, 'PATENT_NOT_GRANTED' AS granted,
            researcher_id, COUNT(*) as count_patent
        FROM patent p
        WHERE grant_date IS NULL
        GROUP BY development_year, researcher_id
    """)
    result = session.execute(SCRIPT_SQL).mappings().all()
    if not result:
        return []
    df = pl.DataFrame(result)
    df = df.with_columns(
        (pl.col('granted').replace_strict(barema, default=0.0).cast(pl.Float64) * pl.col('count_patent')).alias('patent_prod'),
        pl.col('year').cast(pl.Int64)
    )
    df = df.group_by(['researcher_id', 'year']).agg(pl.col('patent_prod').sum())
    return df.select(['year', 'researcher_id', 'patent_prod']).to_dicts()


def software_indprod(session):
    SCRIPT_SQL = text("""
        SELECT year, COUNT(*) AS software_count, researcher_id
        FROM software
        GROUP BY year, researcher_id;
    """)
    result = session.execute(SCRIPT_SQL).mappings().all()
    if not result:
        return []
    df = pl.DataFrame(result)
    df = df.with_columns(
        (pl.col('software_count') * barema.get('SOFTWARE', 0.0)).alias('software_prod'),
        pl.col('year').cast(pl.Int64)
    )
    return df.select(['year', 'researcher_id', 'software_prod']).to_dicts()


def report_indprod(session):
    SCRIPT_SQL = text("""
        SELECT year, COUNT(*) AS report_count, researcher_id
        FROM research_report
        GROUP BY year, researcher_id;
    """)
    result = session.execute(SCRIPT_SQL).mappings().all()
    if not result:
        return [{'year': 0, 'researcher_id': uuid4(), 'report_prod': 0.0}]
    df = pl.DataFrame(result)
    df = df.with_columns(
        (pl.col('report_count') * barema.get('REPORT', 0.0)).alias('report_prod'),
        pl.col('year').cast(pl.Int64)
    )
    return df.select(['year', 'researcher_id', 'report_prod']).to_dicts()


def guidance_indprod(session):
    SCRIPT_SQL = text("""
        SELECT year, nature || ' ' || status AS nature_status,
            COUNT(*) AS guidance_count, researcher_id
        FROM guidance
        GROUP BY year, nature_status, researcher_id;
    """)
    result = session.execute(SCRIPT_SQL).mappings().all()
    if not result:
        return []
    df = pl.DataFrame(result)
    df = df.with_columns(
        pl.col('nature_status')
        .map_elements(lambda ns: unidecode(ns).upper() if ns is not None else '', return_dtype=pl.String)
        .alias('nature_status'),
        pl.col('year').cast(pl.Int64)
    )
    df = df.with_columns(
        (pl.col('nature_status').replace_strict(barema, default=0.0).cast(pl.Float64) * pl.col('guidance_count')).alias('guidance_prod')
    )
    df = df.group_by(['year', 'researcher_id']).agg(pl.col('guidance_prod').sum())
    return df.select(['year', 'researcher_id', 'guidance_prod']).to_dicts()


def list_researchers(session, researcher_ids=None, lattes_ids=None):
    base_query = """
        SELECT id AS researcher_id
        FROM public.researcher
        WHERE 1=1
    """
    params = {}
    if researcher_ids:
        base_query += ' AND id = ANY(:researcher_ids)'
        params['researcher_ids'] = list(researcher_ids)
    if lattes_ids:
        base_query += ' AND lattes_id = ANY(:lattes_ids)'
        params['lattes_ids'] = list(lattes_ids)

    return session.execute(text(base_query), params).mappings().all()


items_found = 0
items_succeeded = 0
items_failed = 0


def main(researcher_ids=None, lattes_ids=None):
    global items_found, items_succeeded, items_failed
    session = next(get_sync_session())
    start_time = time.perf_counter()

    try:
        current_year = datetime.now().year
        YEAR = range(2008, current_year + 1)
        history = pl.DataFrame({'year': list(YEAR)}).with_columns(pl.col('year').cast(pl.Int64))

        researchers = list_researchers(session, researcher_ids, lattes_ids)
        if not researchers:
            raise ValueError('No researchers found')
        researchers = pl.DataFrame(researchers).with_columns(pl.col('researcher_id').cast(pl.String))

        researchers = researchers.join(history, how='cross')

        on = ['researcher_id', 'year']

        articles = pl.DataFrame(article_indprod(session)).with_columns(pl.col('researcher_id').cast(pl.String))
        researchers = researchers.join(articles, on=on, how='left')

        books = pl.DataFrame(book_indprod(session)).with_columns(pl.col('researcher_id').cast(pl.String))
        researchers = researchers.join(books, on=on, how='left')

        book_chapter = pl.DataFrame(book_chapter_indprod(session)).with_columns(pl.col('researcher_id').cast(pl.String))
        researchers = researchers.join(book_chapter, on=on, how='left')

        software = pl.DataFrame(software_indprod(session)).with_columns(pl.col('researcher_id').cast(pl.String))
        researchers = researchers.join(software, on=on, how='left')

        patent = pl.DataFrame(patent_indprod(session)).with_columns(pl.col('researcher_id').cast(pl.String))
        researchers = researchers.join(patent, on=on, how='left')

        report = pl.DataFrame(report_indprod(session)).with_columns(pl.col('researcher_id').cast(pl.String))
        researchers = researchers.join(report, on=on, how='left')

        guidance = guidance_indprod(session)
        if guidance:
            guidance_df = pl.DataFrame(guidance).with_columns(pl.col('researcher_id').cast(pl.String))
            researchers = researchers.join(guidance_df, on=on, how='left')
        else:
            researchers = researchers.with_columns(pl.lit(0.0).alias('guidance_prod'))

        researchers = researchers.fill_null(0.0).with_columns(pl.col('year').cast(pl.Int64))

        if researcher_ids or lattes_ids:
            base_query = 'DELETE FROM researcher_ind_prod WHERE 1=1'
            params = {}
            if researcher_ids:
                base_query += ' AND researcher_id = ANY(:researcher_ids)'
                params['researcher_ids'] = list(researcher_ids)
            if lattes_ids:
                base_query += (
                    ' AND researcher_id IN (SELECT id FROM researcher '
                    'WHERE lattes_id = ANY(:lattes_ids))'
                )
                params['lattes_ids'] = list(lattes_ids)
            session.execute(text(base_query), params)
        else:
            session.execute(text('DELETE FROM researcher_ind_prod;'))

        query_insert = text("""
            INSERT INTO researcher_ind_prod (
                researcher_id, year,
                ind_prod_article, ind_prod_book, ind_prod_book_chapter,
                ind_prod_software, ind_prod_granted_patent,
                ind_prod_not_granted_patent, ind_prod_report, ind_prod_guidance
            ) VALUES (
                :researcher_id, :year,
                :article_prod, :book_prod, :book_chapter_prod,
                :software_prod, :patent_prod, :patent_prod_not,
                :report_prod, :guidance_prod
            );
        """)

        params = []
        for row in researchers.to_dicts():
            params.append({
                'researcher_id': str(row['researcher_id']),
                'year': int(row['year']),
                'article_prod': float(row['article_prod']),
                'book_prod': float(row['book_prod']),
                'book_chapter_prod': float(row['book_chapter_prod']),
                'software_prod': float(row['software_prod']),
                'patent_prod': float(row['patent_prod']),
                'patent_prod_not': float(row['patent_prod']),
                'report_prod': float(row['report_prod']),
                'guidance_prod': float(row['guidance_prod']),
            })

        items_found = len(params)
        BATCH_SIZE = 5000
        for i in range(0, len(params), BATCH_SIZE):
            batch = params[i : i + BATCH_SIZE]
            session.execute(query_insert, batch)

        session.commit()
        items_succeeded = items_found
        items_failed = 0
        duration = time.perf_counter() - start_time
    except Exception as e:
        items_succeeded = 0
        items_failed = items_found
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

