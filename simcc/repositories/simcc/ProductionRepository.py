from typing import Literal
from uuid import UUID

from simcc.core.connection import Connection
from simcc.repositories import conn
from simcc.repositories.util import pagination, webseatch_filter
from simcc.schemas import ArticleOptions, QualisOptions
from simcc.schemas.Production.Article import (
    ArticleMetric,
)


def get_book_metrics(
    term: str,
    researcher_id: UUID,
    program_id: UUID,
    year: int,
    distinct: int = 0,
):
    params = {}

    filter_distinct = str()
    if distinct:
        filter_distinct = 'DISTINCT'

    term_filter = str()
    if term:
        term_filter, term = webseatch_filter('bp.title', term)
        params |= term

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND bp.researcher_id = %(researcher_id)s'

    program_join = str()
    program_filter = str()
    if program_id:
        params['program_id'] = program_id
        program_join = """
            LEFT JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            """
        program_filter = """
            AND gpr.graduate_program_id = %(program_id)s
            AND gpr.type_ = 'PERMANENTE'
            """

    year_filter = str()
    if year:
        params['year'] = year
        year_filter = 'AND bp.year::int >= %(year)s'

    SCRIPT_SQL = f"""
        SELECT bp.year, COUNT({filter_distinct} bp.title) AS among
        FROM researcher r
            LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id
            {program_join}
        WHERE 1 = 1
            AND type = 'BOOK'
            {term_filter}
            {program_filter}
            {year_filter}
            {filter_id}
        GROUP BY
            bp.year;
            """
    print(SCRIPT_SQL, params)
    result = conn.select(SCRIPT_SQL, params)
    return result


def get_book_chapter_metrics(
    term: str,
    researcher_id: UUID,
    program_id: UUID,
    year: int,
    distinct: int = 1,
):
    params = {}

    distinct_filter = str()
    if distinct:
        distinct_filter = 'DISTINCT'
    term_filter = str()
    if term:
        term_filter, term = webseatch_filter('bp.title', term)
        params |= term

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND bp.researcher_id = %(researcher_id)s'

    program_join = str()
    program_filter = str()
    if program_id:
        params['program_id'] = program_id
        program_join = """
            LEFT JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            """
        program_filter = """
            AND gpr.graduate_program_id = %(program_id)s
            AND gpr.type_ = 'PERMANENTE'
            """

    year_filter = str()
    if year:
        params['year'] = year
        year_filter = 'AND bp.year::int >= %(year)s'

    SCRIPT_SQL = f"""
        SELECT bp.year, COUNT({distinct_filter} bp.title) AS among
        FROM researcher r
            LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id
            LEFT JOIN openalex_article opa ON opa.article_id = bp.id
            {program_join}
        WHERE 1 = 1
            AND type = 'BOOK_CHAPTER'
            {term_filter}
            {program_filter}
            {year_filter}
            {filter_id}
        GROUP BY
            bp.year;
            """

    result = conn.select(SCRIPT_SQL, params)
    return result


async def get_researcher_metrics(
    conn: Connection,
    term: str = None,
    year: int = 2020,
    type: Literal[
        'BOOK',
        'BOOK_CHAPTER',
        'ARTICLE',
        'WORK_IN_EVENT',
        'TEXT_IN_NEWSPAPER_MAGAZINE',
        'ABSTRACT',
        'PATENT',
        'AREA',
        'EVENT',
    ] = 'ABSTRACT',
    distinct: int = 1,
    institution: str = None,
):
    params = {}
    join_filter = ''
    type_filter = ''
    year_filter = ''

    match type:
        case 'ABSTRACT':
            type_filter, term_params = webseatch_filter('r.abstract', term)
            params |= term_params
        case (
            'BOOK'
            | 'BOOK_CHAPTER'
            | 'ARTICLE'
            | 'WORK_IN_EVENT'
            | 'TEXT_IN_NEWSPAPER_MAGAZINE'
        ):
            join_filter = f"""
                INNER JOIN bibliographic_production bp
                    ON bp.researcher_id = r.id AND bp.type = '{type}'
            """
            type_filter, term_params = webseatch_filter('bp.title', term)
            year_filter = 'AND bp.year::int >= %(year)s'
            params |= term_params
            params['year'] = year
        case 'PATENT':
            join_filter = 'INNER JOIN patent p ON p.researcher_id = r.id'
            type_filter, term_params = webseatch_filter('p.title', term)
            year_filter = 'AND p.year::int >= %(year)s'
            params |= term_params
            params['year'] = year
        case 'AREA':
            join_filter = (
                'INNER JOIN researcher_production rp ON rp.researcher_id = r.id'
            )
            type_filter, term_params = webseatch_filter('rp.great_area', term)
            params |= term_params
        case 'EVENT':
            join_filter = (
                'INNER JOIN event_organization e ON e.researcher_id = r.id'
            )
            type_filter, term_params = webseatch_filter('e.title', term)
            year_filter = 'AND e.year::int >= %(year)s'
            params |= term_params
            params['year'] = year
        case _:
            ...

    filter_institution = ''
    if institution:
        params['institution'] = institution + '%'
        filter_institution = """
            AND r.institution_id IN (
                SELECT id FROM institution WHERE name ILIKE %(institution)s
            )
        """

    SCRIPT_SQL = f"""
        SELECT COUNT(DISTINCT r.id) AS researcher_count,
               COUNT(DISTINCT r.orcid) AS orcid_count,
               COUNT(DISTINCT opr.scopus) AS scopus_count
        FROM researcher r
        LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
        {join_filter}
        WHERE 1 = 1
            {type_filter}
            {filter_institution}
            {year_filter}
    """

    return await conn.select(SCRIPT_SQL, params)


def list_article_metrics(
    term: str,
    researcher_id: UUID,
    program_id: UUID,
    year: int,
    distinct: int = 1,
) -> list[ArticleMetric]:
    params = {}

    distinct_filter = str()

    if distinct:
        distinct_filter = 'DISTINCT'

    term_filter = str()
    if term:
        term_filter, term = webseatch_filter('bp.title', term)
        params |= term

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND bp.researcher_id = %(researcher_id)s'

    program_join = str()
    program_filter = str()
    if program_id:
        params['program_id'] = program_id
        program_join = """
            LEFT JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            """
        program_filter = """
            AND gpr.graduate_program_id = %(program_id)s
            AND gpr.type_ = 'PERMANENTE'
            """

    year_filter = str()
    if year:
        params['year'] = year
        year_filter = 'AND bp.year::int >= %(year)s'

    SCRIPT_SQL = f"""
        SELECT bp.year, SUM(opa.citations_count) AS citations,
            ARRAY_AGG(bpa.qualis) AS qualis, ARRAY_AGG(bpa.jcr) AS jcr,
            COUNT({distinct_filter} bp.title) AS among
        FROM researcher r
            LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id
            INNER JOIN bibliographic_production_article bpa
                ON bpa.bibliographic_production_id = bp.id
            LEFT JOIN openalex_article opa ON opa.article_id = bp.id
            {program_join}
        WHERE 1 = 1
            {term_filter}
            {program_filter}
            {year_filter}
            {filter_id}
        GROUP BY
            bp.year;
            """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_patent_metrics(
    term,
    researcher_id: UUID,
    program_id: UUID,
    year: int,
    distinct: int = 1,
):
    params = {}
    distinct_filter = str()
    if distinct:
        distinct_filter = 'DISTINCT'
    term_filter = str()
    if term:
        term_filter, term = webseatch_filter('p.title', term)
        params |= term

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND p.researcher_id = %(researcher_id)s'

    filter_program = str()
    join_program = str()
    if program_id:
        params['program_id'] = program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = p.researcher_id
            """
        filter_program = 'AND gpr.graduate_program_id = %(program_id)s'

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND p.development_year::INT >= %(year)s'

    SCRIPT_SQL = f"""
        SELECT development_year AS year,
            COUNT({distinct_filter} p.title) FILTER (WHERE p.grant_date IS NULL) AS NOT_GRANTED,
            COUNT({distinct_filter} p.title) FILTER (WHERE p.grant_date IS NOT NULL) AS GRANTED
        FROM patent p
            {join_program}
        WHERE 1 = 1
            {filter_id}
            {term_filter}
            {filter_year}
            {filter_program}
        GROUP BY development_year;
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def list_guidance_metrics(
    term,
    researcher_id: UUID,
    program_id: UUID,
    year: int,
    distinct: int = 1,
):
    params = {}

    distinct_filter = str()
    if distinct:
        distinct_filter = 'DISTINCT'

    term_filter = str()
    if term:
        term_filter, term = webseatch_filter('g.title', term)
        params |= term

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND g.researcher_id = %(researcher_id)s'

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND g.year >= %(year)s'

    filter_program = str()
    join_program = str()
    if program_id:
        params['program_id'] = program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = g.researcher_id
            """
        filter_program = 'AND gpr.graduate_program_id = %(program_id)s'

    SCRIPT_SQL = f"""
        SELECT g.year AS year,
            unaccent(lower((g.nature || ' ' || g.status))) AS nature,
            COUNT({distinct_filter} g.oriented) as count_nature
        FROM guidance g
            {join_program}
        WHERE 1 = 1
            {filter_id}
            {filter_year}
            {term_filter}
            {filter_program}
        GROUP BY g.year, nature, g.status;
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_academic_degree_metrics(
    researcher_id: UUID, program_id: UUID, year: int
):
    params = {}

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND e.researcher_id = %(researcher_id)s'

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = """
            AND (e.education_start >= %(year)s OR e.education_end >= %(year)s)
            """

    filter_program = str()
    if program_id:
        params['program_id'] = program_id

        filter_program = """
             AND e.researcher_id IN
             (SELECT researcher_id
             FROM graduate_program_researcher
             WHERE graduate_program_id = %(program_id)s)
             """

    SCRIPT_SQL = f"""
        SELECT e.education_start AS year, COUNT(e.degree) AS among,
            REPLACE(degree || '-START', '-', '_') as degree
        FROM education e
        WHERE 1 = 1
            {filter_year}
            {filter_id}
            {filter_program}
        GROUP BY year, degree

        UNION

        SELECT e.education_end AS year, COUNT(e.degree) AS among,
            REPLACE(degree || '-END', '-', '_') as degree
        FROM education e
        WHERE 1 = 1
            {filter_year}
            {filter_id}
            {filter_program}
        GROUP BY year, degree
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_software_metrics(
    term,
    researcher_id: UUID,
    program_id: UUID,
    year: int,
    distinct: int = 1,
):
    params = {}
    distinct_filter = str()
    if distinct:
        distinct_filter = 'DISTINCT'

    term_filter = str()
    if term:
        term_filter, term = webseatch_filter('s.title', term)
        params |= term

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND s.researcher_id = %(researcher_id)s'

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = """AND s.year >= %(year)s"""

    filter_program = str()
    join_program = str()
    if program_id:
        params['program_id'] = program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = s.researcher_id
            """
        filter_program = 'AND gpr.graduate_program_id = %(program_id)s'

    SCRIPT_SQL = f"""
        SELECT s.year, COUNT({distinct_filter} s.title) among
        FROM public.software s
            {join_program}
        WHERE 1 = 1
            {filter_id}
            {term_filter}
            {filter_year}
            {filter_program}
        GROUP BY s.year;
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_distinct_patent(
    term: str,
    researcher_id: UUID,
    year: int,
    institution_id: UUID,
    page: int,
    lenght: int,
):
    params = {}
    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND p.researcher_id = %(researcher_id)s'

    filter_terms = str()
    if term:
        filter_terms, term = webseatch_filter('p.title', term)
        params |= term

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND p.development_year::INT >= %(year)s'

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    filter_institution = str()
    if institution_id:
        params['institution_id'] = institution_id
        filter_institution = 'AND r.institution_id = %(institution_id)s'

    SCRIPT_SQL = f"""
        SELECT p.title AS title, MAX(p.development_year) as year,
            MAX(p.grant_date) AS grant_date, BOOL_OR(p.has_image) AS has_image,
            BOOL_OR(p.relevance) AS relevance, ARRAY_AGG(r.id) AS researcher,
            ARRAY_AGG(r.lattes_id) AS lattes_id, ARRAY_AGG(r.name) AS name,
            ARRAY_REMOVE(ARRAY_AGG(
                CASE WHEN
                    p.has_image = true
                    OR p.relevance = true
                THEN p.id END), NULL) AS id
        FROM patent p
            INNER JOIN researcher r ON r.id = p.researcher_id
        WHERE 1 = 1
            {filter_id}
            {filter_year}
            {filter_terms}
            {filter_institution}
        GROUP BY p.title
        ORDER BY year desc
        {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_patent(
    term: str,
    researcher_id: UUID,
    year: int,
    institution_id: UUID,
    page: int,
    lenght: int,
):
    params = {}
    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND p.researcher_id = %(researcher_id)s'

    filter_terms = str()
    if term:
        filter_terms, term = webseatch_filter('p.title', term)
        params |= term

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND p.development_year::INT >= %(year)s'

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    filter_institution = str()
    if institution_id:
        params['institution_id'] = institution_id
        filter_institution = 'AND r.institution_id = %(institution_id)s'

    SCRIPT_SQL = f"""
        SELECT p.title AS title, p.development_year as year,
            p.grant_date AS grant_date, p.id AS id,
            p.has_image, p.relevance,
            r.id AS researcher, r.lattes_id AS lattes_id, r.name
        FROM patent p
            INNER JOIN researcher r ON r.id = p.researcher_id
        WHERE 1 = 1
            {filter_id}
            {filter_year}
            {filter_terms}
            {filter_institution}
        ORDER BY year desc
        {filter_pagination};
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def list_brand(
    term: str,
    researcher_id: UUID,
    year: int,
    institution_id: UUID,
    page: int,
    lenght: int,
):
    params = {}

    filter_institution = str()
    if institution_id:
        params['institution_id'] = institution_id
        filter_institution = 'AND r.institution_id = %(institution_id)s'

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND b.researcher_id = %(researcher_id)s'

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND b.year >= %(year)s'

    filter_terms = str()
    if term:
        filter_terms, term = webseatch_filter('b.title', term)
        params |= term

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT DISTINCT b.title as title, b.year as year, b.has_image,
            b.relevance, r.lattes_id, r.name, b.id
        FROM brand b
            INNER JOIN researcher r
                ON b.researcher_id = r.id
        WHERE 1 = 1
            {filter_id}
            {filter_year}
            {filter_terms}
            {filter_institution}
        ORDER BY year desc
        {filter_pagination};
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def list_distinct_brand(
    term: str,
    researcher_id: UUID,
    year: int,
    institution_id: UUID,
    page: int,
    lenght: int,
):
    params = {}

    filter_institution = str()
    if institution_id:
        params['institution_id'] = institution_id
        filter_institution = 'AND r.institution_id = %(institution_id)s'

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND b.researcher_id = %(researcher_id)s'

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND b.year >= %(year)s'

    filter_terms = str()
    if term:
        filter_terms, term = webseatch_filter('b.title', term)
        params |= term

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT b.title, MIN(b.year) as year, BOOL_OR(b.has_image) AS has_image,
            BOOL_OR(b.relevance) AS relevance, ARRAY_AGG(r.lattes_id)
            AS lattes_id, ARRAY_AGG(r.name) AS name,
            ARRAY_REMOVE(ARRAY_AGG(
                CASE WHEN
                    b.has_image = true
                    OR b.relevance = true
                THEN b.id END), NULL) AS id
        FROM brand b
            INNER JOIN researcher r
                ON b.researcher_id = r.id
        WHERE 1 = 1
            {filter_id}
            {filter_year}
            {filter_terms}
            {filter_institution}
        GROUP BY b.title
        ORDER BY year desc
        {filter_pagination};
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def list_distinct_book(
    term: str,
    researcher_id: UUID,
    year: int,
    institution_id: UUID,
    page: int,
    lenght: int,
):
    params = {}

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND bp.researcher_id = %(researcher_id)s'

    filter_terms = str()
    if term:
        filter_terms, term = webseatch_filter('bp.title', term)
        params |= term

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND year::INT >= %(year)s'

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    filter_institution = str()
    if institution_id:
        params['institution_id'] = institution_id
        filter_institution = 'AND r.institution_id = %(institution_id)s'

    SCRIPT_SQL = f"""
        SELECT bp.title, year, bpb.isbn AS isbn,
            MAX(bpb.publishing_company) AS publishing_company,
            ARRAY_AGG(bp.researcher_id) AS researcher,
            ARRAY_AGG(r.lattes_id) AS lattes_id, BOOL_OR(bp.relevance)
            AS relevance, BOOL_OR(bp.has_image) AS has_image, ARRAY_AGG(r.name)
            AS name,
            ARRAY_REMOVE(ARRAY_AGG(
                CASE WHEN
                    bp.has_image = true
                    OR bp.relevance = true
                THEN bp.id END), NULL) AS id
        FROM bibliographic_production bp
            INNER JOIN bibliographic_production_book bpb
                ON bp.id = bpb.bibliographic_production_id
            INNER JOIN researcher r
                ON r.id = bp.researcher_id
        WHERE 1 = 1
            {filter_id}
            {filter_terms}
            {filter_year}
            {filter_institution}
        GROUP BY bp.title, bpb.isbn, year
        ORDER BY year desc
        {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_book(
    term: str,
    researcher_id: UUID,
    year: int,
    institution_id: UUID,
    page: int,
    lenght: int,
):
    params = {}

    filter_institution = str()
    if institution_id:
        params['institution_id'] = institution_id
        filter_institution = 'AND r.institution_id = %(institution_id)s'

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND bp.researcher_id = %(researcher_id)s'

    filter_terms = str()
    if term:
        filter_terms, term = webseatch_filter('bp.title', term)
        params |= term

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND year::INT >= %(year)s'

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT bp.title, year, bpb.isbn AS isbn,
            bpb.publishing_company AS publishing_company,
            bp.researcher_id AS researcher,
            r.lattes_id AS lattes_id, bp.relevance,
            bp.has_image, bp.id, r.name
        FROM bibliographic_production bp
            INNER JOIN bibliographic_production_book bpb
                ON bp.id = bpb.bibliographic_production_id
            INNER JOIN researcher r
                ON r.id = bp.researcher_id
        WHERE 1 = 1
            {filter_id}
            {filter_terms}
            {filter_institution}
            {filter_year}
        ORDER BY year desc
        {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_bibliographic_production(
    terms: str = None,
    researcher_id: UUID | str = None,
    year: int | str = 2020,
    type: ArticleOptions = 'ARTICLE',
    qualis: QualisOptions = None,
    institution_id: UUID = None,
    page: int = None,
    lenght: int = None,
):
    params = {}

    filter_institution = str()
    if institution_id:
        params['institution_id'] = institution_id
        filter_institution = 'AND r.institution_id = %(institution_id)s'

    filter_type = str()
    if type == 'ARTICLE':
        filter_type = "AND type = 'ARTICLE'"

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND r.id = %(researcher_id)s'

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND year_ >= %(year)s'

    filter_terms = str()
    if terms:
        filter_terms, terms = webseatch_filter('b.title', terms)
        params |= terms

    filter_qualis = str()
    if qualis:
        params['qualis'] = qualis.split(';')
        filter_qualis = 'AND bpa.qualis = ANY(%(qualis)s)'

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT
            b.id AS id, title, year, type, doi, bpa.qualis,
            periodical_magazine_name AS magazine, r.name AS researcher,
            r.lattes_10_id, r.lattes_id, jcr AS jif,
            jcr_link, r.id AS researcher_id, opa.abstract,
            opa.article_institution, opa.authors, opa.authors_institution,
            COALESCE (opa.citations_count, 0) AS citations_count, bpa.issn,
            opa.keywords, opa.landing_page_url, opa.language, opa.pdf,
            b.has_image, b.relevance
        FROM bibliographic_production b
            LEFT JOIN bibliographic_production_article bpa
                ON b.id = bpa.bibliographic_production_id
            LEFT JOIN researcher r
                ON r.id = b.researcher_id
            LEFT JOIN institution i
                ON r.institution_id = i.id
            LEFT JOIN openalex_article opa
                ON opa.article_id = b.id
        WHERE 1 = 1
            {filter_id}
            {filter_year}
            {filter_terms}
            {filter_type}
            {filter_qualis}
            {filter_institution}
            {filter_pagination}
        ORDER BY
            year DESC
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_article_production(  # noqa: PLR0914
    terms: str = None,
    university: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    year: int | str = 2020,
    type: ArticleOptions = 'ARTICLE',
    qualis: QualisOptions = None,
    page: int = None,
    lenght: int = None,
    dep_id: str = None,
):
    params = {}

    filter_university = str()
    join_university = str()
    if university:
        join_university = """
            LEFT JOIN institution i
                ON r.institution_id = i.id
            """
        filter_university = 'AND i.name = %(university)s'
        params['university'] = university

    filter_program = str()
    join_program = str()
    if graduate_program_id:
        params['program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON r.id = gpr.researcher_id
            """
        filter_program = 'AND gpr.graduate_program_id = %(program_id)s'

    filter_type = str()
    if type == 'ARTICLE':
        filter_type = "AND type = 'ARTICLE'"

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND r.id = %(researcher_id)s'

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND year_ >= %(year)s'

    filter_terms = str()
    if terms:
        filter_terms, terms = webseatch_filter('b.title', terms)
        params |= terms

    filter_qualis = str()
    if qualis:
        params['qualis'] = qualis.split(';')
        filter_qualis = 'AND bpa.qualis = ANY(%(qualis)s)'

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    filter_dep = str()
    join_dep = str()
    if dep_id:
        params['dep_id'] = dep_id
        join_dep = """
            INNER JOIN ufmg.departament_researcher dr
                ON r.id = dr.researcher_id
            """
        filter_dep = 'AND dr.dep_id = %(dep_id)s'

    SCRIPT_SQL = f"""
        SELECT
            b.id AS id, title, b.year, type, doi, bpa.qualis,
            periodical_magazine_name AS magazine, r.name AS researcher,
            r.lattes_10_id, r.lattes_id, jcr AS jif,
            jcr_link, r.id AS researcher_id, opa.abstract,
            opa.article_institution, opa.authors,
            opa.authors_institution, opa.citations_count, bpa.issn, opa.keywords,
            opa.landing_page_url, opa.language, opa.pdf, b.has_image, b.relevance
        FROM bibliographic_production b
            INNER JOIN bibliographic_production_article bpa
                ON b.id = bpa.bibliographic_production_id
            INNER JOIN researcher r
                ON r.id = b.researcher_id
            LEFT JOIN openalex_article opa
                ON opa.article_id = b.id
            {join_university}
            {join_program}
            {join_dep}
        WHERE 1 = 1
            {filter_id}
            {filter_year}
            {filter_terms}
            {filter_type}
            {filter_qualis}
            {filter_pagination}
            {filter_program}
            {filter_university}
            {filter_dep}
        ORDER BY
            year DESC
        """
    result = conn.select(SCRIPT_SQL, params)
    print(result)
    return result


def list_distinct_article_production(  # noqa: PLR0914
    terms: str = None,
    university: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    year: int | str = 2020,
    type: ArticleOptions = 'ARTICLE',
    qualis: QualisOptions = None,
    page: int = None,
    lenght: int = None,
    dep_id: str = None,
):
    params = {}

    filter_university = str()
    join_university = str()
    if university:
        join_university = """
            LEFT JOIN institution i
                ON r.institution_id = i.id
            """
        filter_university = 'AND i.name = %(university)s'
        params['university'] = university

    filter_program = str()
    join_program = str()
    if graduate_program_id:
        params['program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON r.id = gpr.researcher_id
            """
        filter_program = 'AND gpr.graduate_program_id = %(program_id)s'

    filter_type = str()
    if type == 'ARTICLE':
        filter_type = "AND type = 'ARTICLE'"

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND r.id = %(researcher_id)s'

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND year_ >= %(year)s'

    filter_terms = str()
    if terms:
        filter_terms, terms = webseatch_filter('b.title', terms)
        params |= terms

    filter_qualis = str()
    if qualis:
        params['qualis'] = qualis.split(';')
        filter_qualis = 'AND bpa.qualis = ANY(%(qualis)s)'

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    filter_dep = str()
    join_dep = str()
    if dep_id:
        params['dep_id'] = dep_id
        join_dep = """
            INNER JOIN ufmg.departament_researcher dr
                ON r.id = dr.researcher_id
            """
        filter_dep = 'AND dr.dep_id = %(dep_id)s'

    SCRIPT_SQL = f"""
        SELECT title, MAX(b.year) AS year, MAX(type) AS type,
            MAX(doi) AS doi, MAX(bpa.qualis) AS qualis,
            MAX(periodical_magazine_name) AS magazine,
            ARRAY_AGG(r.name) AS researcher,
            ARRAY_AGG(r.lattes_10_id) AS lattes_10_id,
            ARRAY_AGG(r.lattes_id) AS lattes_id, MAX(jcr) AS jif,
            MAX(jcr_link) AS jcr_link, ARRAY_AGG(r.id) AS researcher_id,
            MAX(opa.abstract) AS abstract,
            MAX(opa.article_institution) AS article_institution,
            MAX(opa.authors) AS authors,
            MAX(opa.authors_institution) AS authors_institution,
            MAX(opa.citations_count) AS citations_count,
            MAX(bpa.issn) AS issn, MAX(opa.keywords) AS keywords,
            MAX(opa.landing_page_url) AS landing_page_url,
            MAX(opa.language) AS language, MAX(opa.pdf) AS pdf,
            BOOL_OR(b.has_image) AS has_image,
            BOOL_OR(b.relevance) AS relevance,
            ARRAY_REMOVE(ARRAY_AGG(
                CASE WHEN
                    b.has_image = true
                    OR b.relevance = true
                THEN b.id END), NULL) AS id
        FROM bibliographic_production b
            INNER JOIN bibliographic_production_article bpa
                ON b.id = bpa.bibliographic_production_id
            INNER JOIN researcher r
                ON r.id = b.researcher_id
            INNER JOIN institution i
                ON r.institution_id = i.id
            LEFT JOIN openalex_article opa
                ON opa.article_id = b.id
            {join_program}
            {join_dep}
            {join_university}
        WHERE 1 = 1
            {filter_id}
            {filter_year}
            {filter_terms}
            {filter_type}
            {filter_qualis}
            {filter_pagination}
            {filter_program}
            {filter_university}
            {filter_dep}
        GROUP BY b.title
        ORDER BY year DESC;
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_book_chapter(
    term: str = None,
    researcher_id: UUID | str = None,
    year: int | str = 2020,
    institution_id: UUID | str = None,
    page: int = None,
    lenght: int = None,
):
    params = {}

    filter_institution = str()
    if institution_id:
        params['institution_id'] = institution_id
        filter_institution = 'AND r.id = %(institution_id)s'

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND r.id = %(researcher_id)s'

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)
    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND year_ >= %(year)s'

    filter_terms = str()
    if term:
        filter_terms, terms = webseatch_filter('bp.title', term)
        params |= terms

    SCRIPT_SQL = f"""
        SELECT bp.title, bp.year, bpc.isbn, bpc.publishing_company,
            bp.researcher_id AS researcher, bp.id, r.lattes_id, bp.relevance,
            bp.has_image, r.name
        FROM bibliographic_production bp
            INNER JOIN bibliographic_production_book_chapter bpc
                ON bpc.bibliographic_production_id = bp.id
            LEFT JOIN researcher r
                ON r.id = bp.researcher_id
        WHERE 1 = 1
            {filter_id}
            {filter_pagination}
            {filter_year}
            {filter_institution}
            {filter_terms}
        ORDER BY bp.year DESC;
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def list_distinct_book_chapter(
    term: str = None,
    researcher_id: UUID | str = None,
    year: int | str = 2020,
    institution: UUID | str = None,
    page: int = None,
    lenght: int = None,
):
    params = {}

    filter_institution = str()
    if institution:
        params['institution'] = institution
        filter_institution = 'AND r.institution_id = %(institution)s'

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND r.id = %(researcher_id)s'

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND year_ >= %(year)s'

    filter_terms = str()
    if term:
        filter_terms, terms = webseatch_filter('bp.title', term)
        params |= terms

    SCRIPT_SQL = f"""
        SELECT bp.title, MIN(bp.year) AS year, MAX(bpc.isbn) AS isbn,
            MAX(bpc.publishing_company) AS publishing_company,
            ARRAY_AGG(bp.researcher_id) AS researcher, ARRAY_AGG(r.lattes_id)
            AS lattes_id, BOOL_OR(bp.relevance) AS relevance,
            BOOL_OR(bp.has_image) AS has_image, ARRAY_AGG(r.name) AS name,
            ARRAY_REMOVE(ARRAY_AGG(
                CASE WHEN
                    bp.has_image = true
                    OR bp.relevance = true
                THEN bp.id END), NULL) AS id
        FROM bibliographic_production bp
            INNER JOIN bibliographic_production_book_chapter bpc
                ON bpc.bibliographic_production_id = bp.id
            LEFT JOIN researcher r
                ON r.id = bp.researcher_id
        WHERE 1 = 1
            {filter_id}
            {filter_pagination}
            {filter_year}
            {filter_institution}
            {filter_terms}
        GROUP BY bp.title
        ORDER BY year DESC;
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def list_distinct_software(
    researcher_id: UUID | str = None, year: int | str = 2020
):
    params = {}

    filter_id = str()
    if researcher_id:
        filter_id = 'AND r.id = %(researcher_id)s'
        params['researcher_id'] = researcher_id

    if year:
        filter_year = 'AND s.year >= %(year)s'
        params['year'] = year

    SCRIPT_SQL = f"""
        SELECT s.title, MIN(s.year) AS year, BOOL_OR(s.has_image) AS has_image,
            BOOL_OR(s.relevance) AS relevance, ARRAY_AGG(r.name) AS name,
            ARRAY_REMOVE(ARRAY_AGG(
                CASE WHEN
                    s.has_image = true
                    OR s.relevance = true
                THEN s.id END), NULL) AS id
        FROM software s
            INNER JOIN researcher r ON s.researcher_id = r.id
        WHERE 1 = 1
            {filter_id}
            {filter_year}
        GROUP BY s.title
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_software(researcher_id: UUID | str = None, year: int | str = 2020):
    params = {}

    filter_id = str()
    if researcher_id:
        filter_id = 'AND r.id = %(researcher_id)s'
        params['researcher_id'] = researcher_id

    filter_year = str()
    if year:
        filter_year = 'AND s.year >= %(year)s'
        params['year'] = year

    SCRIPT_SQL = f"""
        SELECT s.id, s.title, s.year AS year, s.has_image, s.relevance, r.name
        FROM software s
        LEFT JOIN researcher r ON s.researcher_id = r.id
        WHERE 1 = 1
            {filter_id}
            {filter_year}
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_distinct_researcher_report(
    researcher_id: UUID | str = None,
    year: int | str = 2020,
    page: int = None,
    lenght: int = None,
):
    params = {}

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    filter_id = str()
    if researcher_id:
        filter_id = 'AND r.id = %(researcher_id)s'
        params['researcher_id'] = researcher_id

    filter_year = str()
    if year:
        filter_year = 'AND rr.year >= %(year)s'
        params['year'] = year

    SCRIPT_SQL = f"""
        SELECT ARRAY_AGG(rr.id) AS id, ARRAY_AGG(r.name) AS name, rr.title,
            MIN(rr.year) AS year, MAX(project_name) AS project_name,
            MAX(financing_institutionc) AS financing
        FROM research_report rr
        LEFT JOIN researcher r ON rr.researcher_id = r.id
        WHERE 1 = 1
            {filter_id}
            {filter_year}
        GROUP BY rr.title
        ORDER BY year desc
        {filter_pagination}
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_researcher_report(
    researcher_id: UUID | str = None,
    year: int | str = 2020,
    page: int = None,
    lenght: int = None,
):
    params = {}

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    filter_id = str()
    if researcher_id:
        filter_id = 'AND r.id = %(researcher_id)s'
        params['researcher_id'] = researcher_id

    filter_year = str()
    if year:
        filter_year = 'AND rr.year >= %(year)s'
        params['year'] = year

    SCRIPT_SQL = f"""
        SELECT rr.id, r.name, rr.title, rr.year, project_name,
            financing_institutionc AS financing
        FROM research_report rr
        LEFT JOIN researcher r ON rr.researcher_id = r.id
        WHERE 1 = 1
            {filter_id}
            {filter_year}
        ORDER BY year desc
        {filter_pagination}
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_distinct_guidance_production(
    researcher_id: UUID | str = None, year: int | str = 2020
):
    params = {}

    filter_id = str()
    if researcher_id:
        filter_id = 'AND r.id = %(researcher_id)s'
        params['researcher_id'] = researcher_id

    filter_year = str()
    if year:
        filter_year = 'AND g.year >= %(year)s'
        params['year'] = year

    SCRIPT_SQL = f"""
        SELECT g.title, MAX(nature) AS nature,
            g.oriented, MAX(g.type) AS type,
            MAX(g.status) AS status, MAX(g.year) AS year,
            ARRAY_AGG(r.name) AS name
        FROM guidance g
        INNER JOIN researcher r ON g.researcher_id = r.id
        WHERE 1 = 1
            {filter_year}
            {filter_id}
        GROUP BY g.title, g.oriented
        ORDER BY year desc
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_guidance_production(
    researcher_id: UUID | str = None, year: int | str = 2020
):
    params = {}

    filter_id = str()
    if researcher_id:
        filter_id = 'AND r.id = %(researcher_id)s'
        params['researcher_id'] = researcher_id

    filter_year = str()
    if year:
        filter_year = 'AND g.year >= %(year)s'
        params['year'] = year

    SCRIPT_SQL = f"""
        SELECT g.id, g.title, nature, g.oriented, g.type, g.status,
            g.year, r.name
        FROM guidance g
            INNER JOIN researcher r ON g.researcher_id = r.id
        WHERE 1 = 1
            {filter_year}
            {filter_id}
        ORDER BY year desc
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_researcher_production_events(
    researcher_id: UUID | str = None, year: int | str = 2020
):
    params = {}

    filter_id = str()
    if researcher_id:
        filter_id = 'AND r.id = %(researcher_id)s'
        params['researcher_id'] = researcher_id

    filter_year = str()
    if year:
        filter_year = 'AND bp.year_ >= %(year)s'
        params['year'] = year

    SCRIPT_SQL = f"""
        SELECT bp.title, bp.title_en, bp.nature, bp.language,
            bp.means_divulgation, bp.homepage, bp.relevance,
            bp.scientific_divulgation, bp.authors, bp.year_,
            r.name, bp.id
        FROM bibliographic_production bp
            LEFT JOIN researcher r ON r.id = bp.researcher_id
        WHERE type = 'WORK_IN_EVENT'
            {filter_id}
            {filter_year}
        ORDER BY year_ desc;
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_distinct_researcher_production_events(
    researcher_id: UUID | str = None, year: int | str = 2020
):
    params = {}

    filter_id = str()
    if researcher_id:
        filter_id = 'AND r.id = %(researcher_id)s'
        params['researcher_id'] = researcher_id

    filter_year = str()
    if year:
        filter_year = 'AND bp.year_ >= %(year)s'
        params['year'] = year

    SCRIPT_SQL = f"""
        SELECT bp.title, MAX(bp.title_en) AS title_en, MAX(bp.nature) AS nature,
            MAX(bp.language) AS language, ARRAY_AGG(r.name) AS name,
            MAX(bp.means_divulgation) AS means_divulgation,
            MAX(bp.homepage) AS homepage, BOOL_OR(bp.relevance) AS relevance,
            BOOL_OR(bp.has_image) AS has_image, BOOL_OR(bp.scientific_divulgation)
            AS scientific_divulgation, MAX(bp.authors) AS authors, MAX(bp.year_)
            AS year_, ARRAY_AGG(bp.id) AS id
        FROM bibliographic_production bp
        LEFT JOIN researcher r ON r.id = bp.researcher_id
        WHERE type = 'WORK_IN_EVENT'
            {filter_id}
            {filter_year}
        GROUP BY bp.title
        ORDER BY year_ desc;
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_research_projects(
    term: str = None,
    researcher_id: UUID | str = None,
    year: int | str = 2020,
    program_id: UUID | str = None,
):
    params = {}

    filter_terms = str()
    if term:
        filter_terms, terms = webseatch_filter('title', term)
        params |= terms

    filter_year = str()
    if year:
        filter_year = 'AND start_year >= %(year)s'
        params['year'] = year

    filter_program = str()
    join_program = str()
    if program_id:
        params['program_id'] = program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            """
        filter_program = 'AND gpr.graduate_program_id = %(program_id)s'

    filter_id = str()
    if researcher_id:
        filter_id = 'AND r.id = %(researcher_id)s'
        params['researcher_id'] = researcher_id

    SCRIPT_SQL = f"""
        SELECT rp.id, rp.researcher_id, r.name, rp.start_year, rp.end_year,
            rp.agency_code, rp.agency_name, rp.project_name, rp.status,
            rp.nature, rp.number_undergraduates, rp.number_specialists,
            rp.number_academic_masters, rp.number_phd, rp.description,
            rpp.production, rpf.foment, rpc.components
        FROM public.research_project rp
            LEFT JOIN researcher r ON r.id = rp.researcher_id
            LEFT JOIN (SELECT project_id, JSONB_AGG(JSONB_BUILD_OBJECT('title', title, 'type', type)) AS production
                FROM public.research_project_production
                WHERE 1 = 1
                    {filter_terms}
                GROUP BY project_id) rpp ON rpp.project_id = rp.id
            LEFT JOIN (SELECT project_id, JSONB_AGG(JSONB_BUILD_OBJECT('agency_name', agency_name, 'agency_code', agency_code, 'nature', nature)) AS foment
                FROM public.research_project_foment
                GROUP BY project_id) rpf ON rpf.project_id = rp.id
            LEFT JOIN (SELECT project_id, JSONB_AGG(JSONB_BUILD_OBJECT('name', name, 'lattes_id', lattes_id, 'citations', citations)) AS components
                FROM public.research_project_components
                GROUP BY project_id) rpc ON rpc.project_id = rp.id
            {join_program}
        WHERE 1 = 1
            {filter_year}
            {filter_program}
            {filter_id}
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def list_distinct_research_projects(
    term: str = None,
    researcher_id: UUID | str = None,
    year: int | str = 2020,
    program_id: UUID | str = None,
):
    params = {}

    filter_terms = str()
    if term:
        filter_terms, terms = webseatch_filter('title', term)
        params |= terms

    filter_year = str()
    if year:
        filter_year = 'AND start_year >= %(year)s'
        params['year'] = year

    filter_program = str()
    join_program = str()
    if program_id:
        params['program_id'] = program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            """
        filter_program = 'AND gpr.graduate_program_id = %(program_id)s'

    filter_id = str()
    if researcher_id:
        filter_id = 'AND r.id = %(researcher_id)s'
        params['researcher_id'] = researcher_id

    SCRIPT_SQL = f"""
        SELECT
            rp.project_name, MAX(rp.start_year) AS start_year, MAX(rp.end_year)
            AS end_year, MAX(rp.agency_code) AS agency_code, MAX(rp.agency_name)
            AS agency_name, MAX(rp.status) AS status, MAX(rp.nature) AS nature,
            MAX(rp.number_undergraduates) AS number_undergraduates,
            MAX(rp.number_specialists) AS number_specialists,
            MAX(rp.number_academic_masters) AS number_academic_masters,
            MAX(rp.number_phd) AS number_phd, MAX(rp.description) AS description,
            ARRAY_AGG(rp.id) AS id, ARRAY_AGG(rp.researcher_id)
            AS researcher_id, ARRAY_AGG(r.name) AS name,
            JSONB_AGG(DISTINCT rpp.production)
                FILTER (WHERE rpp.production IS NOT NULL) AS production,
            JSONB_AGG(DISTINCT rpf.foment)
                FILTER (WHERE rpf.foment IS NOT NULL) AS foment,
            JSONB_AGG(DISTINCT rpc.components)
                FILTER (WHERE rpc.components IS NOT NULL) AS components
        FROM public.research_project rp
            LEFT JOIN researcher r ON r.id = rp.researcher_id
            LEFT JOIN (SELECT project_id, JSONB_AGG(JSONB_BUILD_OBJECT('title', title, 'type', type)) AS production
                FROM public.research_project_production
                WHERE 1 = 1
                    {filter_terms}
                GROUP BY project_id) rpp ON rpp.project_id = rp.id
            LEFT JOIN (SELECT project_id, JSONB_AGG(JSONB_BUILD_OBJECT('agency_name', agency_name, 'agency_code', agency_code, 'nature', nature)) AS foment
                FROM public.research_project_foment
                GROUP BY project_id) rpf ON rpf.project_id = rp.id
            LEFT JOIN (SELECT project_id, JSONB_AGG(JSONB_BUILD_OBJECT('name', name, 'lattes_id', lattes_id, 'citations', citations)) AS components
                FROM public.research_project_components
                GROUP BY project_id) rpc ON rpc.project_id = rp.id
            {join_program}
        WHERE 1 = 1
            {filter_year}
            {filter_program}
            {filter_id}
        GROUP BY rp.project_name
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def list_papers_magazine(
    researcher_id: UUID | str = None,
    year: int | str = 2020,
):
    params = {}

    filter_year = str()
    if year:
        filter_year = 'AND year_ >= %(year)s'
        params['year'] = year

    filter_id = str()
    if researcher_id:
        filter_id = 'AND r.id = %(researcher_id)s'
        params['researcher_id'] = researcher_id

    SCRIPT_SQL = f"""
        SELECT title, title_en, nature, language, means_divulgation, homepage,
            relevance, scientific_divulgation, authors, year_, r.name
        FROM public.bibliographic_production bp
            INNER JOIN researcher r ON bp.researcher_id = r.id
        WHERE type = 'TEXT_IN_NEWSPAPER_MAGAZINE'
            {filter_year}
            {filter_id}
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def list_distinct_papers_magazine(
    researcher_id: UUID | str = None,
    year: int | str = 2020,
):
    params = {}

    filter_year = str()
    if year:
        filter_year = 'AND year_ >= %(year)s'
        params['year'] = year

    filter_id = str()
    if researcher_id:
        filter_id = 'AND r.id = %(researcher_id)s'
        params['researcher_id'] = researcher_id

    SCRIPT_SQL = f"""
        SELECT bp.title, MAX(bp.title_en) AS title_en, MAX(bp.nature) AS nature,
            MAX(bp.language) AS language, MAX(bp.means_divulgation)
            AS means_divulgation, MAX(bp.homepage) AS homepage,
            BOOL_OR(bp.relevance) AS relevance, ARRAY_AGG(r.id)
            AS researcher_ids, BOOL_OR(bp.scientific_divulgation)
            AS scientific_divulgation, ARRAY_AGG(bp.authors) AS authors,
            MAX(bp.year_) AS year_, ARRAY_AGG(r.name) AS name
        FROM public.bibliographic_production bp
            INNER JOIN researcher r ON bp.researcher_id = r.id
        WHERE bp.type = 'TEXT_IN_NEWSPAPER_MAGAZINE'
            {filter_year}
            {filter_id}
        GROUP BY bp.title
        """

    result = conn.select(SCRIPT_SQL, params)
    return result
