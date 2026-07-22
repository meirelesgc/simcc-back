import argparse

import polars as pl
from sqlalchemy import text

from simcc.core.db.database import get_sync_session


def list_researchers(session, researcher_ids=None, lattes_ids=None):
    base_query = """
        SELECT id::TEXT AS researcher_id, name, lattes_id
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

    return session.execute(text(base_query), params).mappings().all()


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
        SELECT researcher_id::TEXT, type, COUNT(*) AS count
        FROM bibliographic_production
        GROUP BY researcher_id, type;
    """
    result = session.execute(text(query)).mappings().all()

    target_cols = [
        'researcher_id',
        'BOOK',
        'BOOK_CHAPTER',
        'ARTICLE',
        'WORK_IN_EVENT',
        'TEXT_IN_NEWSPAPER_MAGAZINE',
    ]

    if not result:
        return []

    df = pl.DataFrame(result).with_columns(
        pl.col('researcher_id').cast(pl.String)
    )
    df = df.pivot(
        on='type',
        index='researcher_id',
        values='count',
        aggregate_function='sum',
    ).fill_null(0)

    for col in target_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(0).alias(col))

    df = df.select(target_cols)
    df = df.rename({c: c.lower() for c in df.columns})

    return df.to_dicts()


def list_great_area(session):
    query = """
        SELECT researcher_id::TEXT, STRING_AGG(DISTINCT gae.name, ';') as area
        FROM great_area_expertise gae
        LEFT JOIN researcher_area_expertise r
                ON gae.id = r.great_area_expertise_id
        GROUP BY researcher_id
    """
    return session.execute(text(query)).mappings().all()


def list_speciality(session):
    query = """
        SELECT r.researcher_id::TEXT,
            STRING_AGG(asp.name || ' | ' || ae.name, '; ') AS area_specialty
        FROM researcher_area_expertise r
        RIGHT JOIN area_specialty asp ON asp.id = r.area_specialty_id
        LEFT JOIN area_expertise ae ON r.area_expertise_id = ae.id
        GROUP BY researcher_id;
    """
    return session.execute(text(query)).mappings().all()


def list_software(session):
    query = """
        SELECT researcher_id::TEXT, COUNT(*) AS software
        FROM software
        GROUP BY researcher_id;
    """
    return session.execute(text(query)).mappings().all()


def list_brand(session):
    query = """
        SELECT researcher_id::TEXT, COUNT(*) AS brand
        FROM brand
        GROUP BY researcher_id;
    """
    return session.execute(text(query)).mappings().all()


def list_patent(session):
    query = """
        SELECT researcher_id::TEXT, COUNT(*) AS patent
        FROM patent
        GROUP BY researcher_id;
    """
    return session.execute(text(query)).mappings().all()


def list_address(session):
    query = """
        SELECT researcher_id::TEXT, city, organ
        FROM researcher_address
        ORDER BY researcher_id;
    """
    return session.execute(text(query)).mappings().all()


def main(researcher_ids=None, lattes_ids=None):
    session = next(get_sync_session())

    try:
        delete_researcher_production(session, researcher_ids, lattes_ids)

        def to_df(data, schema):
            return pl.DataFrame(data, schema=schema)

        researchers = to_df(
            list_researchers(session, researcher_ids, lattes_ids),
            {
                'researcher_id': pl.String,
                'name': pl.String,
                'lattes_id': pl.String,
            },
        )

        if researchers.is_empty():
            return

        b_production = to_df(
            bibliographic_production_count(session),
            {
                'researcher_id': pl.String,
                'book': pl.Int64,
                'book_chapter': pl.Int64,
                'article': pl.Int64,
                'work_in_event': pl.Int64,
                'text_in_newspaper_magazine': pl.Int64,
            },
        )

        a_speciality = to_df(
            list_speciality(session),
            {'researcher_id': pl.String, 'area_specialty': pl.String},
        )
        great_area = to_df(
            list_great_area(session),
            {'researcher_id': pl.String, 'area': pl.String},
        )
        software = to_df(
            list_software(session),
            {'researcher_id': pl.String, 'software': pl.Int64},
        )
        brand = to_df(
            list_brand(session),
            {'researcher_id': pl.String, 'brand': pl.Int64},
        )
        patent = to_df(
            list_patent(session),
            {'researcher_id': pl.String, 'patent': pl.Int64},
        )
        address = to_df(
            list_address(session),
            {
                'researcher_id': pl.String,
                'city': pl.String,
                'organ': pl.String,
            },
        )

        researchers = researchers.join(
            b_production, how='left', on='researcher_id'
        )
        researchers = researchers.join(
            a_speciality, how='left', on='researcher_id'
        )
        researchers = researchers.join(
            great_area, how='left', on='researcher_id'
        )
        researchers = researchers.join(
            software, how='left', on='researcher_id'
        )
        researchers = researchers.join(brand, how='left', on='researcher_id')
        researchers = researchers.join(patent, how='left', on='researcher_id')
        researchers = researchers.join(address, how='left', on='researcher_id')

        researchers = researchers.rename({
            c: c.lower() for c in researchers.columns
        })

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

        researchers = researchers.with_columns([
            pl.col(c).fill_null(0) for c in numeric_cols
        ])

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

        records = researchers.to_dicts()

        for researcher in records:
            session.execute(insert_query, researcher)
        session.commit()

    except Exception as E:
        session.rollback()


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
