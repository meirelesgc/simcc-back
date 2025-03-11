from uuid import uuid4

import pandas as pd

from simcc.repositories import conn


def first_article_metric():
    SCRIPT_SQL = """
        SELECT researcher_id, MIN(year_) AS first_article_year
        FROM bibliographic_production
        WHERE type = 'ARTICLE'
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    return result


def event_organization_metrics(year):
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS event_organization
        FROM event_organization
        WHERE year >= %(year)s
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL, {'year': year})
    return result


def work_in_event_metrics(year):
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS work_in_event
        FROM bibliographic_production
        WHERE type = 'WORK_IN_EVENT'
            AND year_ >= %(year)s
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL, {'year': year})
    return result


def participation_events_metrics(year):
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS participation_events
        FROM participation_events
        WHERE year >= %(year)s
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL, {'year': year})
    return result


def article_metrics(year):
    SCRIPT_SQL = """
        SELECT qualis, COUNT(*) AS count_article, researcher_id
        FROM public.bibliographic_production bp
            JOIN public.bibliographic_production_article bpa ON
                bp.id = bpa.bibliographic_production_id
        WHERE type = 'ARTICLE' AND year_ >= %(year)s
        GROUP BY qualis, researcher_id;
        """

    result = conn.select(SCRIPT_SQL, {'year': year})
    articles = pd.DataFrame(result)

    articles = articles.pivot_table(
        index=['researcher_id'],
        columns='qualis',
        aggfunc='sum',
        fill_value=0,
    )

    articles.columns = articles.columns.get_level_values(1)
    articles = articles.reset_index()

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
    articles = articles.reindex(columns, axis='columns', fill_value=0)
    articles = articles.to_dict(orient='records')
    if articles:
        return articles
    return [{'researcher_id': uuid4, 'A1': 0, 'A2': 0, 'A3': 0, 'A4': 0, 'B1': 0, 'B2': 0, 'B3': 0, 'B4': 0, 'C': 0, 'SQ': 0}]  # noqa: E501 # fmt: skip


def patent_metrics(year):
    SCRIPT_SQL = """
        SELECT researcher_id,
            COUNT(*) FILTER (WHERE p.grant_date IS NULL) AS PATENT_NOT_GRANTED,
            COUNT(*) FILTER (WHERE p.grant_date IS NOT NULL) AS PATENT_GRANTED,
            COUNT(*) AS PATENT
        FROM patent p
        WHERE development_year::INT >= %(year)s
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL, {'year': year})
    if result:
        return result
    return [{'researcher_id': uuid4(), 'patent_not_granted': 0, 'patent_granted': 0}]  # noqa: E501 # fmt: skip


def guidance_metrics(year):
    SCRIPT_SQL = """
        SELECT researcher_id,
            unaccent(lower((g.nature || ' ' || g.status))) AS nature,
            COUNT(*) as count_nature
        FROM guidance g
        WHERE g.year >= %(year)s
        GROUP BY nature, g.status, g.researcher_id;
        """
    result = conn.select(SCRIPT_SQL, {'year': year})
    guidance = pd.DataFrame(result)

    guidance = guidance.pivot_table(
        index=['researcher_id'],
        columns='nature',
        values='count_nature',
        aggfunc='sum',
        fill_value=0,
    ).reset_index()

    rename_dict = {
        'iniciacao cientifica concluida': 'ic_completed',
        'iniciacao cientifica em andamento': 'ic_in_progress',
        'dissertacao de mestrado concluida': 'm_completed',
        'dissertacao de mestrado em andamento': 'm_in_progress',
        'tese de doutorado concluida': 'd_completed',
        'tese de doutorado em andamento': 'd_in_progress',
        'trabalho de conclusao de curso graduacao concluida': 'g_completed',
        'trabalho de conclusao de curso de graduacao em andamento': 'g_in_progress',
        'monografia de conclusao de curso aperfeicoamento e especializacao concluida': 'e_completed',
        'monografia de conclusao de curso aperfeicoamento e especializacao em andamento': 'e_in_progress',
        'orientacao-de-outra-natureza concluida': 'o_completed',
        'supervisao de pos-doutorado concluida': 'sd_completed',
        'supervisao de pos-doutorado em andamento': 'sd_in_progress',
    }

    guidance.rename(columns=rename_dict, inplace=True)

    columns = list(rename_dict.values()) + ['researcher_id']
    guidance = guidance.reindex(columns, axis='columns', fill_value=0)
    return guidance.to_dict(orient='records')


def academic_degree_metrics():
    SCRIPT_SQL = """
        SELECT researcher_id, MAX(education_end) AS first_doc
        FROM education
        WHERE degree = 'DOCTORATE'
        GROUP BY researcher_id
        """
    result = conn.select(SCRIPT_SQL)

    academic_degree = pd.DataFrame(result)

    return academic_degree.to_dict(orient='records')


def book_metrics(year):
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS book
        FROM bibliographic_production
        WHERE type = 'BOOK'
            AND year_ >= %(year)s
        GROUP BY researcher_id
        """

    result = conn.select(SCRIPT_SQL, {'year': year})
    return result


def book_chapter_metrics(year):
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) AS book_chapter
        FROM bibliographic_production
        WHERE type = 'BOOK_CHAPTER'
            AND year_ >= %(year)s
        GROUP BY researcher_id
        """
    result = conn.select(SCRIPT_SQL, {'year': year})
    return result


def brand_metrics(year):
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) brand
        FROM public.brand b
        WHERE b.year >= %(year)s
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL, {'year': year})
    return result


def software_metrics(year):
    SCRIPT_SQL = """
        SELECT researcher_id, COUNT(*) software
        FROM public.software s
        WHERE s.year >= %(year)s
        GROUP BY researcher_id;
        """
    result = conn.select(SCRIPT_SQL, {'year': year})
    return result


def list_researchers():
    SCRIPT_SQL = """
        SELECT r.id AS researcher_id, r.name, r.lattes_id, r.graduation,
            i.acronym
        FROM public.researcher r
        LEFT JOIN institution i ON i.id = r.institution_id
        """
    result = conn.select(SCRIPT_SQL)
    return result


def ind_prod_metrics(year):
    SCRIPT_SQL = """
        SELECT
            researcher_id,
            COALESCE(SUM(ind_prod_article), 0)
            + COALESCE(SUM(ind_prod_book), 0)
            + COALESCE(SUM(ind_prod_book_chapter), 0)
            + COALESCE(SUM(ind_prod_software), 0)
            + COALESCE(SUM(ind_prod_report), 0)
            + COALESCE(SUM(ind_prod_granted_patent), 0)
            + COALESCE(SUM(ind_prod_not_granted_patent), 0)
            + COALESCE(SUM(ind_prod_guidance), 0) AS ind_prod
        FROM public.researcher_ind_prod
        WHERE year >= %(year)s
        GROUP BY researcher_id;
        """

    result = conn.select(SCRIPT_SQL, {'year': year})
    return result


if __name__ == '__main__':
    STRING = """Você está interessado nas métricas dos pesquisadores desde qual ano? """  # noqa: E501
    YEAR_FILTER = int(input(STRING))
    print(f'Filtrando métricas dos pesquisadores desde {YEAR_FILTER}.')

    dataframe = pd.DataFrame(list_researchers())
    articles = pd.DataFrame(article_metrics(YEAR_FILTER))

    dataframe = dataframe.merge(articles, how='left', on=['researcher_id'])

    first_article = pd.DataFrame(first_article_metric())
    dataframe = dataframe.merge(first_article, how='left', on=['researcher_id'])

    patents = pd.DataFrame(patent_metrics(YEAR_FILTER))
    dataframe = dataframe.merge(patents, how='left', on=['researcher_id'])

    guidances = pd.DataFrame(guidance_metrics(YEAR_FILTER))
    dataframe = dataframe.merge(guidances, how='left', on=['researcher_id'])

    softwares = pd.DataFrame(software_metrics(YEAR_FILTER))
    dataframe = dataframe.merge(softwares, how='left', on=['researcher_id'])

    book = pd.DataFrame(book_metrics(YEAR_FILTER))
    dataframe = dataframe.merge(book, how='left', on=['researcher_id'])

    book_chapter = pd.DataFrame(book_chapter_metrics(YEAR_FILTER))
    dataframe = dataframe.merge(book_chapter, how='left', on=['researcher_id'])

    degree = pd.DataFrame(academic_degree_metrics())
    dataframe = dataframe.merge(degree, how='left', on=['researcher_id'])

    brand = pd.DataFrame(brand_metrics(YEAR_FILTER))
    dataframe = dataframe.merge(brand, how='left', on=['researcher_id'])

    work_in_event = pd.DataFrame(work_in_event_metrics(YEAR_FILTER))
    dataframe = dataframe.merge(work_in_event, how='left', on=['researcher_id'])

    participation_events = pd.DataFrame(
        participation_events_metrics(YEAR_FILTER)
    )
    dataframe = dataframe.merge(
        participation_events, how='left', on=['researcher_id']
    )

    ind_prod = pd.DataFrame(ind_prod_metrics(YEAR_FILTER))
    dataframe = dataframe.merge(ind_prod, how='left', on=['researcher_id'])

    dataframe.columns = [f'{col}_PAQ' for col in dataframe.columns]

    dataframe.to_csv(
        f'storage/csv/researcher_class_{YEAR_FILTER}.csv',
        index=False,
        decimal=',',
        sep=';',
    )
