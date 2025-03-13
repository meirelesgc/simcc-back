from datetime import datetime
from pprint import pprint

import pandas as pd

from simcc.repositories import conn


def dim_research_project():
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS project
        FROM research_project
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    return result


def brand_metrics():
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) brand
        FROM public.brand b
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    return result


def patent_metrics():
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS patent
        FROM patent p
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    return result


def software_metrics():
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) software
        FROM public.software s
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    return result


def book_metrics():
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS book
        FROM bibliographic_production
        WHERE type = 'BOOK' OR type = 'BOOK_CHAPTER'
        GROUP BY researcher_id
        """

    result = conn.select(SCRIPT_SQL)
    return result


def list_researchers():
    SCRIPT_SQL = """
        SELECT r.id AS researcher_id, r.name, r.lattes_id
        FROM public.researcher r
        LEFT JOIN institution i ON i.id = r.institution_id
        """
    result = conn.select(SCRIPT_SQL)
    return result


def academic_degree_duration():
    SCRIPT_SQL = """
        SELECT researcher_id, MAX(education_end) AS first_doc
        FROM education
        WHERE degree = 'DOCTORATE'
        GROUP BY researcher_id
        """
    result = conn.select(SCRIPT_SQL)

    academic_degree = pd.DataFrame(result)

    def xpto(x):
        if datetime.now().year - x >= 6:
            return 'A/B'

        if datetime.now().year - x >= 2:
            return 'C'

    academic_degree['duration'] = academic_degree['first_doc'].apply(xpto)

    return academic_degree.to_dict(orient='records')


def openAlex_metrics():
    SCRIPT_SQL = """
        SELECT researcher_id, h_index
        FROM openalex_researcher
        """

    result = conn.select(SCRIPT_SQL)
    return result


def article_metrics():
    SCRIPT_SQL = """
        SELECT COUNT(*) AS article, researcher_id
        FROM public.bibliographic_production bp
            JOIN public.bibliographic_production_article bpa ON
                bp.id = bpa.bibliographic_production_id
        WHERE type = 'ARTICLE'
        GROUP BY researcher_id;
        """

    result = conn.select(SCRIPT_SQL)
    return result


if __name__ == '__main__':
    researchers = list_researchers()
    researchers = pd.DataFrame(researchers)

    duration = academic_degree_duration()
    duration = pd.DataFrame(duration)

    researchers = pd.merge(researchers, duration, on='researcher_id', how='left')

    openAlex = openAlex_metrics()
    openAlex = pd.DataFrame(openAlex)

    researchers = pd.merge(researchers, openAlex, on='researcher_id', how='left')

    articles = article_metrics()
    articles = pd.DataFrame(articles)

    researchers = pd.merge(researchers, articles, on='researcher_id', how='left')

    books = book_metrics()
    books = pd.DataFrame(books)

    researchers = pd.merge(researchers, books, on='researcher_id', how='left')

    softwares = software_metrics()
    softwares = pd.DataFrame(softwares)
    researchers = pd.merge(
        researchers, softwares, on='researcher_id', how='left'
    )

    patent = patent_metrics()
    patent = pd.DataFrame(patent)
    researchers = pd.merge(researchers, patent, on='researcher_id', how='left')

    brand = brand_metrics()
    brand = pd.DataFrame(brand)
    researchers = pd.merge(researchers, brand, on='researcher_id', how='left')

    pprint(researchers)
