import argparse
import time

import numpy as np
import pandas as pd
from sqlalchemy import text

from simcc.core.db.database import get_sync_session
from simcc.core.logging import get_logger

logger = get_logger('routines')


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

    return session.execute(text(base_query), params).fetchall()


def delete_researcher_production(
    session, researcher_ids=None, lattes_ids=None
):
    if researcher_ids or lattes_ids:
        base_query = 'DELETE FROM researcher_production WHERE 1=1'
        params = {}
        if researcher_ids:
            base_query += ' AND researcher_id IN (:researcher_ids)'
            params['researcher_ids'] = tuple(researcher_ids)
        if lattes_ids:
            base_query += (
                ' AND researcher_id IN (SELECT id FROM researcher '
                'WHERE lattes_id IN (:lattes_ids))'
            )
            params['lattes_ids'] = tuple(lattes_ids)
        session.execute(text(base_query), params)
    else:
        session.execute(text('DELETE FROM researcher_production'))


def bibliographic_production_count(session):
    query = """
        SELECT researcher_id, type, COUNT(*)
        FROM bibliographic_production
        GROUP BY researcher_id, type;
    """
    result = session.execute(text(query)).fetchall()

    columns = ['researcher_id', 'type', 'count']
    bibliographic_production = pd.DataFrame(result, columns=columns)

    if not bibliographic_production.empty:
        bibliographic_production = bibliographic_production.pivot_table(
            index='researcher_id', columns='type', aggfunc='sum', fill_value=0
        )
        bibliographic_production.columns = (
            bibliographic_production.columns.get_level_values(1)
        )
        bibliographic_production = bibliographic_production.reset_index()

    columns = [
        'researcher_id',
        'BOOK',
        'BOOK_CHAPTER',
        'ARTICLE',
        'WORK_IN_EVENT',
        'TEXT_IN_NEWSPAPER_MAGAZINE',
    ]

    bibliographic_production = bibliographic_production.reindex(
        columns, axis='columns', fill_value=0
    )

    bibliographic_production.columns = (
        bibliographic_production.columns.str.lower()
    )

    return bibliographic_production.to_dict(orient='records')


def list_great_area(session):
    query = """
        SELECT researcher_id, STRING_AGG(DISTINCT gae.name, ';') as area
        FROM great_area_expertise gae
        LEFT JOIN researcher_area_expertise r
                ON gae.id = r.great_area_expertise_id
        GROUP BY researcher_id
    """
    return session.execute(text(query)).fetchall()


def list_speciality(session):
    query = """
        SELECT r.researcher_id,
            STRING_AGG(asp.name || ' | ' || ae.name, '; ') AS area_specialty
        FROM researcher_area_expertise r
        RIGHT JOIN area_specialty asp ON asp.id = r.area_specialty_id
        LEFT JOIN area_expertise ae ON r.area_expertise_id = ae.id
        GROUP BY researcher_id;
    """
    return session.execute(text(query)).fetchall()


def list_software(session):
    query = """
        SELECT researcher_id, COUNT(*) AS software
        FROM software
        GROUP BY researcher_id;
    """
    return session.execute(text(query)).fetchall()


def list_brand(session):
    query = """
        SELECT researcher_id, COUNT(*) AS brand
        FROM brand
        GROUP BY researcher_id;
    """
    return session.execute(text(query)).fetchall()


def list_patent(session):
    query = """
        SELECT researcher_id, COUNT(*) AS patent
        FROM patent
        GROUP BY researcher_id;
    """
    return session.execute(text(query)).fetchall()


def list_address(session):
    query = """
        SELECT researcher_id, city, organ
        FROM researcher_address
        ORDER BY researcher_id;
    """
    return session.execute(text(query)).fetchall()


def main(researcher_ids=None, lattes_ids=None):
    session = next(get_sync_session())
    start_time = time.perf_counter()
    logger.info('researcher_production_routine_started')

    try:
        delete_researcher_production(session, researcher_ids, lattes_ids)

        researchers = pd.DataFrame(
            list_researchers(session, researcher_ids, lattes_ids),
            columns=['researcher_id', 'name', 'lattes_id'],
        )

        b_production = bibliographic_production_count(session)
        columns = [
            'researcher_id',
            'book',
            'book_chapter',
            'article',
            'work_in_event',
            'text_in_newspaper_magazine',
        ]
        b_production = pd.DataFrame(b_production, columns=columns)

        a_speciality = pd.DataFrame(
            list_speciality(session),
            columns=['researcher_id', 'area_specialty'],
        )
        great_area = pd.DataFrame(
            list_great_area(session), columns=['researcher_id', 'area']
        )
        software = pd.DataFrame(
            list_software(session), columns=['researcher_id', 'software']
        )
        brand = pd.DataFrame(
            list_brand(session), columns=['researcher_id', 'brand']
        )
        patent = pd.DataFrame(
            list_patent(session), columns=['researcher_id', 'patent']
        )
        address = pd.DataFrame(
            list_address(session), columns=['researcher_id', 'city', 'organ']
        )

        researchers = researchers.merge(
            b_production, how='left', on='researcher_id'
        )
        researchers = researchers.merge(
            a_speciality, how='left', on='researcher_id'
        )
        researchers = researchers.merge(
            great_area, how='left', on='researcher_id'
        )
        researchers = researchers.merge(
            software, how='left', on='researcher_id'
        )
        researchers = researchers.merge(brand, how='left', on='researcher_id')
        researchers = researchers.merge(patent, how='left', on='researcher_id')
        researchers = researchers.merge(
            address, how='left', on='researcher_id'
        )

        researchers = researchers.rename(columns=str.lower)

        numeric_cols = [
            'book',
            'book_chapter',
            'article',
            'work_in_event',
            'text_in_newspaper_magazine',
            'software',
            'brand',
            'patent',
        ]
        text_cols = ['area_specialty', 'area', 'city', 'organ']

        researchers[numeric_cols] = researchers[numeric_cols].fillna(0)
        researchers[text_cols] = (
            researchers[text_cols].fillna(None).replace({np.nan: None})
        )

        insert_query = text("""
            INSERT INTO researcher_production
                (researcher_id, articles, book_chapters,
                book, work_in_event, patent, software, brand, great_area,
                area_specialty, city, organ)
            VALUES
                (:researcher_id, :article, :book_chapter, :book,
                :work_in_event, :patent, :software, :brand,
                :area, :area_specialty, :city, :organ);
        """)

        records = researchers.to_dict(orient='records')

        for researcher in records:
            session.execute(insert_query, researcher)

        session.commit()
        duration = time.perf_counter() - start_time
        logger.info(
            'researcher_production_routine_finished',
            duration=f'{duration:.2f}s',
        )

    except Exception as e:
        session.rollback()
        duration = time.perf_counter() - start_time
        logger.error(
            'researcher_production_routine_failed',
            error=str(e),
            duration=f'{duration:.2f}s',
        )


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
