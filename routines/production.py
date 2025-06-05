import numpy as np
import pandas as pd

from routines.logger import logger_researcher_routine, logger_routine
from simcc.repositories import conn


def list_researchers():
    SCRIPT_SQL = """
        SELECT id AS researcher_id, name, lattes_id
        FROM public.researcher
        """
    result = conn.select(SCRIPT_SQL)
    return result


def delete_researcher_production():
    SCRIPT_SQL = """
        DELETE FROM researcher_production
        """
    conn.exec(SCRIPT_SQL)


def bibliographic_production_count():
    SCRIPT_SQL = """
        SELECT researcher_id, type, COUNT(*)
        FROM bibliographic_production
        GROUP BY researcher_id, type;
        """
    result = conn.select(SCRIPT_SQL)

    columns = ['researcher_id', 'type', 'count']
    bibliographic_production = pd.DataFrame(result, columns=columns)
    bibliographic_production = bibliographic_production.pivot_table(
        index='researcher_id', columns='type', aggfunc='sum', fill_value=0
    )
    bibliographic_production.columns = (
        bibliographic_production.columns.get_level_values(1)
    )
    bibliographic_production = bibliographic_production.reset_index()

    columns = [
        'researcher_id',
        'book',
        'book_chapter',
        'article',
        'work_in_event',
        'text_in_newspaper_magazine',
    ]

    bibliographic_production = bibliographic_production.reindex(
        columns, axis='columns', fill_value=0
    )

    bibliographic_production.columns = (
        bibliographic_production.columns.str.lower()
    )

    return bibliographic_production.to_dict(orient='records')


def list_great_area():
    SCRIPT_SQL = """
        SELECT researcher_id, STRING_AGG(DISTINCT gae.name, ';') as area
        FROM great_area_expertise gae
        LEFT JOIN researcher_area_expertise r
                ON gae.id = r.great_area_expertise_id
        GROUP BY researcher_id
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_speciality():
    SCRIPT_SQL = """
        SELECT r.researcher_id,
            STRING_AGG(asp.name || ' | ' || ae.name, '; ') AS area_specialty
        FROM researcher_area_expertise r
        RIGHT JOIN area_specialty asp ON asp.id = r.area_specialty_id
        LEFT JOIN area_expertise ae ON r.area_expertise_id = ae.id
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_software():
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS software
        FROM software
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_brand():
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS brand
        FROM brand
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_patent():
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS patent
        FROM patent
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_address():
    SCRIPT_SQL = """
        SELECT researcher_id, city, organ
        FROM researcher_address
        ORDER BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    return result


if __name__ == '__main__':
    delete_researcher_production()

    researchers = list_researchers()
    researchers = pd.DataFrame(researchers)
    b_production = bibliographic_production_count()
    columns = ['researcher_id', 'book', 'book_chapter', 'article', 'work_in_event', 'text_in_newspaper_magazine']  # fmt: skip
    b_production = pd.DataFrame(b_production, columns=columns)

    area_speciality = list_speciality()
    columns = ['researcher_id', 'area_specialty']
    area_speciality = pd.DataFrame(area_speciality, columns=columns)

    great_area = list_great_area()
    columns = ['researcher_id', 'area']
    great_area = pd.DataFrame(great_area, columns=columns)
    software = list_software()
    columns = ['researcher_id', 'software']
    software = pd.DataFrame(software, columns=columns)
    brand = list_brand()
    columns = ['researcher_id', 'brand']
    brand = pd.DataFrame(brand, columns=columns)
    patent = list_patent()
    columns = ['researcher_id', 'patent']
    patent = pd.DataFrame(patent, columns=columns)
    address = list_address()
    columns = ['researcher_id', 'city', 'organ']
    address = pd.DataFrame(address, columns=columns)
    # Merge the dataframes
    researchers = researchers.merge(b_production, how='left', on='researcher_id')

    researchers = researchers.merge(
        area_speciality, how='left', on='researcher_id'
    )
    researchers = researchers.merge(great_area, how='left', on='researcher_id')
    researchers = researchers.merge(software, how='left', on='researcher_id')
    researchers = researchers.merge(brand, how='left', on='researcher_id')
    researchers = researchers.merge(patent, how='left', on='researcher_id')
    researchers = researchers.merge(address, how='left', on='researcher_id')

    researchers = researchers.replace(np.nan, None)
    researchers = researchers.rename(columns=str.lower)

    for _, researcher in researchers.iterrows():
        SCRIPT_SQL = """
            INSERT INTO researcher_production
                (researcher_id, articles, book_chapters,
                book, work_in_event, patent, software, brand, great_area,
                area_specialty, city, organ)
            VALUES
                (%(researcher_id)s, %(article)s, %(book_chapter)s, %(book)s,
                %(work_in_event)s, %(patent)s, %(software)s, %(brand)s,
                %(area)s, %(area_specialty)s, %(city)s, %(organ)s);
            """
        print(f'Inserting row for researcher: {_}')
        conn.exec(SCRIPT_SQL, researcher.to_dict())
        logger_researcher_routine(researcher.researcher_id, 'PRODUCTION', False)
    logger_routine('PRODUCTION', False)
