from typing import Literal
from uuid import UUID

from simcc.core.connection import Connection
from simcc.repositories import conn
from simcc.repositories.util import pagination, webseatch_filter
from simcc.schemas import ArticleOptions, QualisOptions
from simcc.schemas.Production.Article import (
    ArticleMetric,
)


def professional_experience(
    researcher_id,
    graduate_program_id,
    dep_id,
    dep,
    year,
    distinct,
    institution,
    graduate_program,
    city,
    area,
    modality,
    graduation,
    page,
    lenght,
):
    params = {}

    join_researcher = str()
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    filters = str()
    filter_pagination = str()

    if year:
        params['year'] = year
        filters += """
            AND rpe.start_year >= %(year)s OR rpe.end_year >= %(year)s
            """

    if dep_id or dep:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = rpe.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if dep_id:
        params['dep_id'] = dep_id
        filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if dep:
        params['dep'] = dep.split(';')
        filters += """
            AND dp.dep_nom = ANY(%(dep)s)
            """

    if distinct:
        filter_distinct = 'DISTINCT'

    if researcher_id:
        params['researcher_id'] = researcher_id
        join_researcher = """
            LEFT JOIN researcher r
                ON r.id = rpe.researcher_id
            """
        filters += """
            AND rpe.researcher_id = %(researcher_id)s
            """

    if institution:
        params['institution'] = institution.split(';')
        join_researcher = """
            LEFT JOIN researcher r
                ON r.id = rpe.researcher_id
            """
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        filters += """
            AND i.name = ANY(%(institution)s)
            """

    if graduate_program_id:
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = rpe.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = rpe.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if city:
        params['city'] = city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = rpe.researcher_id
            """
        filters += """
            AND rp.city = ANY(%(city)s)
            """
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = rpe.researcher_id
            """
        filters += """
            AND great_area_ && %(area)s
            """

    if modality:
        filter_distinct = 'DISTINCT'
        params['modality'] = modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = rpe.researcher_id
            """
        filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if graduation:
        params['graduation'] = graduation.split(';')
        join_researcher = """
            LEFT JOIN researcher r
                ON r.id = rpe.researcher_id
            """
        filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            rpe.id, rpe.researcher_id, enterprise, start_year, end_year,
            employment_type, other_employment_type, functional_classification,
            other_functional_classification, workload_hours_weekly,
            exclusive_dedication, additional_info
        FROM public.researcher_professional_experience rpe
            {join_researcher}
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
        WHERE 1 = 1
            {filters}
        ORDER BY start_year
        {filter_pagination}
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def get_book_metrics(
    term: str,
    researcher_id: UUID,
    program_id: UUID,
    dep_id: UUID,
    dep: str,
    year: int,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    params = {}

    join_dep = str()
    filter_dep = str()
    if dep_id:
        distinct = 'DISTINCT'
        params['dep_id'] = dep_id.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_id = ANY(%(dep_id)s)
            """

    if dep:
        distinct = 'DISTINCT'
        params['dep'] = dep.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_nom = ANY(%(dep)s)
            """

    join_program = str()
    filter_program = str()
    if graduate_program:
        distinct = 'DISTINCT'
        params['graduate_program'] = graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filter_program = """
            AND gp.name = ANY(%(graduate_program)s)
            AND gpr.type_ = 'PERMANENTE'
            """

    filter_institution = str()
    join_institution = str()
    if institution:
        params['institution'] = institution + '%'
        join_institution = """
            LEFT JOIN institution i
                ON r.institution_id = i.id
            """
        filter_institution = """
            AND i.name ILIKE %(institution)s
            """

    join_area = str()
    filter_area = str()
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_area = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = r.id
                """
        filter_area = """
            AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s
        """

    join_modality = str()
    filter_modality = str()
    if modality:
        params['modality'] = modality.split(';')
        join_modality = """
            INNER JOIN foment f
                ON f.researcher_id = r.id
        """
        filter_modality = 'AND modality_name = ANY(%(modality)s)'

    join_city = str()
    filter_city = str()
    if city:
        params['city'] = city.split(';')
        join_city = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = r.id
                """
        filter_city = 'AND rp.city = ANY(%(city)s)'

    filter_graduation = str()
    if graduation:
        params['graduation'] = graduation.split(';')
        filter_graduation = 'AND r.graduation = ANY(%(graduation)s)'

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

    if program_id:
        params['program_id'] = program_id
        join_program = """
            LEFT JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            """
        filter_program = """
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
            {join_program}
            {join_modality}
            {join_institution}
            {join_city}
            {join_dep}
            {join_area}
        WHERE 1 = 1
            AND bp.type = 'BOOK'
            {term_filter}
            {filter_dep}
            {filter_institution}
            {filter_area}
            {filter_city}
            {filter_modality}
            {filter_graduation}
            {year_filter}
            {filter_program}
            {filter_id}
        GROUP BY
            bp.year;
            """
    result = conn.select(SCRIPT_SQL, params)
    return result


def get_book_chapter_metrics(
    term: str,
    researcher_id: UUID,
    graduate_program_id: UUID = None,
    dep_id: str = None,
    dep: str = None,
    year: int = 2020,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
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
    if graduate_program_id:
        params['program_id'] = graduate_program_id
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

    join_dep = str()
    filter_dep = str()
    if dep_id:
        distinct_filter = 'DISTINCT'
        params['dep_id'] = dep_id.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        filter_dep = 'AND dp.dep_id = ANY(%(dep_id)s)'

    if dep:
        distinct_filter = 'DISTINCT'
        params['dep'] = dep.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        filter_dep = 'AND dp.dep_nom = ANY(%(dep)s)'

    filter_institution = str()
    if institution:
        params['institution'] = institution + '%'
        filter_institution = """
            AND r.institution_id IN (
                SELECT id FROM institution WHERE name ILIKE %(institution)s
            )
        """

    filter_graduate_program = str()
    if graduate_program:
        params['graduate_program'] = graduate_program + '%'
        filter_graduate_program = """
            AND r.graduate_program_id IN (
                SELECT id FROM graduate_program WHERE name ILIKE %(graduate_program)s
            )
        """

    filter_city = str()
    if city:
        params['city'] = city + '%'
        filter_city = 'AND r.city ILIKE %(city)s'

    filter_area = str()
    if area:
        params['area'] = area + '%'
        filter_area = 'AND r.area ILIKE %(area)s'

    filter_modality = str()
    if modality:
        params['modality'] = modality + '%'
        filter_modality = 'AND r.modality ILIKE %(modality)s'

    filter_graduation = str()
    if graduation:
        params['graduation'] = graduation + '%'
        filter_graduation = 'AND r.graduation ILIKE %(graduation)s'

    SCRIPT_SQL = f"""
        SELECT bp.year, COUNT({distinct_filter} bp.title) AS among
        FROM researcher r
            LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id
            LEFT JOIN openalex_article opa ON opa.article_id = bp.id
            {program_join}
            {join_dep}
        WHERE 1 = 1
            AND type = 'BOOK_CHAPTER'
            {term_filter}
            {program_filter}
            {year_filter}
            {filter_id}
            {filter_dep}
            {filter_institution}
            {filter_graduate_program}
            {filter_city}
            {filter_area}
            {filter_modality}
            {filter_graduation}
        GROUP BY bp.year;
    """

    result = conn.select(SCRIPT_SQL, params)
    return result


async def get_researcher_metrics(
    conn: Connection,
    term: str = None,
    researcher_id: UUID = None,
    graduate_program_id: UUID = None,
    dep_id: str = None,
    dep: str = None,
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
    ] = None,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    params = {}
    join_filter = str()
    type_filter = str()
    year_filter = str()
    join_extra = str()
    where_extra = str()
    join_program = str()
    filter_program = str()

    match type:
        case 'ABSTRACT':
            type_filter, term_params = webseatch_filter('r.abstract', term)
            params |= term_params
        case ('BOOK' | 'BOOK_CHAPTER' | 'ARTICLE' | 'WORK_IN_EVENT' | 'TEXT_IN_NEWSPAPER_MAGAZINE'):  # fmt: skip
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

    join_dep = str()
    filter_dep = str()
    if dep_id:
        params['dep_id'] = dep_id.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_id = ANY(%(dep_id)s)
            """

    if graduate_program_id:
        params['program_id'] = graduate_program_id
        join_program = """
            LEFT JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            """
        filter_program = """
            AND gpr.graduate_program_id = %(program_id)s
            AND gpr.type_ = 'PERMANENTE'
            """

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND r.id = %(researcher_id)s'

    if dep:
        params['dep'] = dep.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_nom = ANY(%(dep)s)
            """

    filter_institution = ''
    if institution:
        params['institution'] = institution + '%'
        filter_institution = """
            AND r.institution_id IN (
                SELECT id FROM institution WHERE name ILIKE %(institution)s
            )
        """

    if graduate_program:
        params['graduate_program'] = graduate_program.split(';')
        join_extra += """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        where_extra += """
            AND gp.name = ANY(%(graduate_program)s)
            AND gpr.type_ = 'PERMANENTE'
        """

    if city:
        params['city'] = city.split(';')
        join_extra += """
            LEFT JOIN researcher_production rp_city ON rp_city.researcher_id = r.id
        """
        where_extra += 'AND rp_city.city = ANY(%(city)s)'

    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_extra += """
            LEFT JOIN researcher_production rp_area ON rp_area.researcher_id = r.id
        """
        where_extra += """
            AND STRING_TO_ARRAY(REPLACE(rp_area.great_area, ' ', '_'), ';') && %(area)s
        """

    if modality:
        params['modality'] = modality.split(';')
        join_extra += """
            INNER JOIN foment f ON f.researcher_id = r.id
        """
        where_extra += 'AND f.modality_name = ANY(%(modality)s)'

    if graduation:
        params['graduation'] = graduation.split(';')
        where_extra += 'AND r.graduation = ANY(%(graduation)s)'

    SCRIPT_SQL = f"""
        SELECT COUNT(DISTINCT r.id) AS researcher_count,
               COUNT(DISTINCT r.orcid) AS orcid_count,
               COUNT(DISTINCT opr.scopus) AS scopus_count
        FROM researcher r
        LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
        {join_filter}
        {join_dep}
        {join_extra}
        {join_program}
        WHERE 1 = 1
            {type_filter}
            {filter_program}
            {filter_dep}
            {filter_id}
            {filter_institution}
            {year_filter}
            {where_extra}
    """

    return await conn.select(SCRIPT_SQL, params)


def list_article_metrics(
    term: str,
    researcher_id: UUID,
    program_id: UUID,
    dep_id: str,
    departament: str,
    year: int,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
) -> list[ArticleMetric]:
    params = {}
    filter_distinct = str()

    if distinct:
        filter_distinct = 'DISTINCT'

    join_dep = str()
    filter_dep = str()

    if departament:
        filter_distinct = 'DISTINCT'
        params['dep'] = departament.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_nom = ANY(%(dep)s)
            """

    if dep_id:
        filter_distinct = 'DISTINCT'
        params['dep_id'] = dep_id.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_id = ANY(%(dep_id)s)
            """

    term_filter = str()
    if term:
        term_filter, term = webseatch_filter('bp.title', term)
        params |= term

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND bp.researcher_id = %(researcher_id)s'

    join_program = str()
    filter_program = str()
    if program_id:
        params['program_id'] = program_id
        join_program = """
            LEFT JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
        """
        filter_program = """
            AND gpr.graduate_program_id = %(program_id)s
            AND gpr.type_ = 'PERMANENTE'
        """

    join_institution = str()
    filter_institution = str()
    if institution:
        params['institution'] = institution + '%'
        join_institution = """
            LEFT JOIN institution i ON r.institution_id = i.id
        """
        filter_institution = """
            AND i.name ILIKE %(institution)s
        """

    join_city = str()
    filter_city = str()
    if city:
        params['city'] = city.split(';')
        join_city = """
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
        """
        filter_city = 'AND rp.city = ANY(%(city)s)'

    join_area = str()
    filter_area = str()
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_area = """
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
        """
        filter_area = """
            AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s
        """

    join_modality = str()
    filter_modality = str()
    if modality:
        params['modality'] = modality.split(';')
        join_modality = """
            INNER JOIN foment f ON f.researcher_id = r.id
        """
        filter_modality = 'AND f.modality_name = ANY(%(modality)s)'

    filter_graduation = str()
    if graduation:
        params['graduation'] = graduation.split(';')
        filter_graduation = 'AND r.graduation = ANY(%(graduation)s)'

    join_graduate_program = str()
    filter_graduate_program = str()
    if graduate_program:
        params['graduate_program'] = graduate_program.split(';')
        join_graduate_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        filter_graduate_program = """
            AND gp.name = ANY(%(graduate_program)s)
            AND gpr.type_ = 'PERMANENTE'
        """

    year_filter = str()
    if year:
        params['year'] = year
        year_filter = 'AND bp.year::int >= %(year)s'

    SCRIPT_SQL = f"""
        SELECT bp.year, SUM(opa.citations_count) AS citations,
            ARRAY_AGG(bpa.qualis) AS qualis, ARRAY_AGG(bpa.jcr) AS jcr,
            COUNT({filter_distinct} bp.title) AS among
        FROM researcher r
            LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id
            INNER JOIN bibliographic_production_article bpa ON bpa.bibliographic_production_id = bp.id
            LEFT JOIN openalex_article opa ON opa.article_id = bp.id
            {join_program}
            {join_graduate_program}
            {join_modality}
            {join_institution}
            {join_city}
            {join_area}
            {join_dep}
        WHERE 1 = 1
            {term_filter}
            {filter_program}
            {filter_graduate_program}
            {filter_modality}
            {filter_dep}
            {filter_institution}
            {filter_city}
            {filter_area}
            {filter_graduation}
            {year_filter}
            {filter_id}
        GROUP BY
            bp.year;
    """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_patent_metrics(
    term,
    researcher_id: UUID = None,
    program_id: UUID = None,
    dep_id: str = None,
    departament: str = None,
    year: int = None,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    params = {}

    join_dep = str()
    filter_dep = str()
    if dep_id:
        filter_distinct = 'DISTINCT'
        params['dep_id'] = dep_id.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = p.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_id = ANY(%(dep_id)s)
            """

    if departament:
        filter_distinct = 'DISTINCT'
        params['dep'] = departament.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = p.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_nom = ANY(%(dep)s)
            """

    filter_distinct = str()
    if distinct:
        filter_distinct = 'DISTINCT'

    term_filter = str()
    if term:
        term_filter, term = webseatch_filter('p.title', term)
        params |= term

    filter_id = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_id = 'AND p.researcher_id = %(researcher_id)s'

    join_program = str()
    filter_program = str()
    if program_id:
        params['program_id'] = program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = p.researcher_id
        """
        filter_program = 'AND gpr.graduate_program_id = %(program_id)s'

    join_graduate_program = str()
    filter_graduate_program = str()
    if graduate_program:
        params['graduate_program'] = graduate_program.split(';')
        join_graduate_program = """
            INNER JOIN graduate_program_researcher gpg ON gpg.researcher_id = p.researcher_id
            INNER JOIN graduate_program gp ON gpg.graduate_program_id = gp.graduate_program_id
        """
        filter_graduate_program = """
            AND gp.name = ANY(%(graduate_program)s)
            AND gpg.type_ = 'PERMANENTE'
        """

    join_institution = str()
    filter_institution = str()
    if institution:
        params['institution'] = institution + '%'
        join_institution = """
            LEFT JOIN researcher r ON r.id = p.researcher_id
            LEFT JOIN institution i ON r.institution_id = i.id
        """
        filter_institution = 'AND i.name ILIKE %(institution)s'

    join_city = str()
    filter_city = str()
    if city:
        params['city'] = city.split(';')
        join_city = """
            LEFT JOIN researcher_production rp ON rp.researcher_id = p.researcher_id
        """
        filter_city = 'AND rp.city = ANY(%(city)s)'

    join_area = str()
    filter_area = str()
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_area = """
            LEFT JOIN researcher_production ra ON ra.researcher_id = p.researcher_id
        """
        filter_area = """
            AND STRING_TO_ARRAY(REPLACE(ra.great_area, ' ', '_'), ';') && %(area)s
        """

    join_modality = str()
    filter_modality = str()
    if modality:
        params['modality'] = modality.split(';')
        join_modality = """
            INNER JOIN foment f ON f.researcher_id = p.researcher_id
        """
        filter_modality = 'AND f.modality_name = ANY(%(modality)s)'

    filter_graduation = str()
    if graduation:
        join_graduation = """
            LEFT JOIN researcher rg ON rg.id = p.researcher_id
        """
        params['graduation'] = graduation.split(';')
        filter_graduation = 'AND rg.graduation = ANY(%(graduation)s)'
    else:
        join_graduation = str()

    filter_year = str()
    if year:
        params['year'] = year
        filter_year = 'AND p.development_year::INT >= %(year)s'

    SCRIPT_SQL = f"""
        SELECT p.development_year AS year,
            COUNT({filter_distinct} p.title) FILTER (WHERE p.grant_date IS NULL) AS NOT_GRANTED,
            COUNT({filter_distinct} p.title) FILTER (WHERE p.grant_date IS NOT NULL) AS GRANTED
        FROM patent p
            {join_program}
            {join_graduate_program}
            {join_institution}
            {join_city}
            {join_area}
            {join_modality}
            {join_dep}
            {join_graduation}
        WHERE 1 = 1
            {filter_id}
            {filter_dep}
            {term_filter}
            {filter_year}
            {filter_program}
            {filter_graduate_program}
            {filter_institution}
            {filter_city}
            {filter_area}
            {filter_modality}
            {filter_graduation}
        GROUP BY p.development_year;
    """

    result = conn.select(SCRIPT_SQL, params)
    return result


def list_guidance_metrics(
    term,
    researcher_id: UUID = None,
    program_id: UUID = None,
    dep_id: str = None,
    departament: str = None,
    year: int = None,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    params = {}
    join_dep = str()
    filter_dep = str()

    if departament:
        distinct = 'DISTINCT'
        params['dep'] = departament.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = g.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_nom = ANY(%(dep)s)
            """

    if dep_id:
        filter_distinct = 'DISTINCT'
        params['dep_id'] = dep_id.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = g.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_id = ANY(%(dep_id)s)
            """
    filter_distinct = str()
    if distinct:
        filter_distinct = 'DISTINCT'

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

    join_program = str()
    filter_program = str()
    if program_id:
        params['program_id'] = program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = g.researcher_id
        """
        filter_program = 'AND gpr.graduate_program_id = %(program_id)s'

    join_graduate_program = str()
    filter_graduate_program = str()
    if graduate_program:
        params['graduate_program'] = graduate_program.split(';')
        join_graduate_program = """
            INNER JOIN graduate_program_researcher gpg ON gpg.researcher_id = g.researcher_id
            INNER JOIN graduate_program gp ON gpg.graduate_program_id = gp.graduate_program_id
        """
        filter_graduate_program = """
            AND gp.name = ANY(%(graduate_program)s)
            AND gpg.type_ = 'PERMANENTE'
        """

    join_institution = str()
    filter_institution = str()
    if institution:
        params['institution'] = institution + '%'
        join_institution = """
            LEFT JOIN researcher r ON r.id = g.researcher_id
            LEFT JOIN institution i ON r.institution_id = i.id
        """
        filter_institution = 'AND i.name ILIKE %(institution)s'

    join_city = str()
    filter_city = str()
    if city:
        params['city'] = city.split(';')
        join_city = """
            LEFT JOIN researcher_production rp ON rp.researcher_id = g.researcher_id
        """
        filter_city = 'AND rp.city = ANY(%(city)s)'

    join_area = str()
    filter_area = str()
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_area = """
            LEFT JOIN researcher_production ra ON ra.researcher_id = g.researcher_id
        """
        filter_area = """
            AND STRING_TO_ARRAY(REPLACE(ra.great_area, ' ', '_'), ';') && %(area)s
        """

    join_modality = str()
    filter_modality = str()
    if modality:
        params['modality'] = modality.split(';')
        join_modality = """
            INNER JOIN foment f ON f.researcher_id = g.researcher_id
        """
        filter_modality = 'AND f.modality_name = ANY(%(modality)s)'

    filter_graduation = str()
    if graduation:
        join_graduation = """
            LEFT JOIN researcher rg ON rg.id = g.researcher_id
        """
        params['graduation'] = graduation.split(';')
        filter_graduation = 'AND rg.graduation = ANY(%(graduation)s)'
    else:
        join_graduation = str()

    SCRIPT_SQL = f"""
        SELECT g.year AS year,
            unaccent(lower((g.nature || ' ' || g.status))) AS nature,
            COUNT({filter_distinct} g.oriented) AS count_nature
        FROM guidance g
            {join_program}
            {join_graduate_program}
            {join_institution}
            {join_dep}
            {join_city}
            {join_area}
            {join_modality}
            {join_graduation}
        WHERE 1 = 1
            {filter_id}
            {filter_year}
            {term_filter}
            {filter_dep}
            {filter_program}
            {filter_graduate_program}
            {filter_institution}
            {filter_city}
            {filter_area}
            {filter_modality}
            {filter_graduation}
        GROUP BY g.year, nature, g.status;
    """

    result = conn.select(SCRIPT_SQL, params)
    return result


def list_academic_degree_metrics(
    researcher_id: UUID = None,
    program_id: UUID = None,
    dep_id: str = None,
    departament: str = None,
    year: int = None,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    params = {}

    join_dep = str()
    filter_dep = str()

    if departament:
        params['dep'] = departament.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = e.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_nom = ANY(%(dep)s)
            """

    if dep_id:
        print(dep_id)
        params['dep_id'] = dep_id.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = e.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_id = ANY(%(dep_id)s)
            """

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
            AND e.researcher_id IN (
                SELECT researcher_id
                FROM graduate_program_researcher
                WHERE graduate_program_id = %(program_id)s
            )
        """

    join_graduate_program = str()
    filter_graduate_program = str()
    if graduate_program:
        params['graduate_program'] = graduate_program.split(';')
        join_graduate_program = """
            INNER JOIN graduate_program_researcher gpg ON gpg.researcher_id = e.researcher_id
            INNER JOIN graduate_program gp ON gpg.graduate_program_id = gp.graduate_program_id
        """
        filter_graduate_program = """
            AND gp.name = ANY(%(graduate_program)s)
            AND gpg.type_ = 'PERMANENTE'
        """

    join_institution = str()
    filter_institution = str()
    if institution:
        params['institution'] = institution + '%'
        join_institution = """
            LEFT JOIN researcher r ON r.id = e.researcher_id
            LEFT JOIN institution i ON r.institution_id = i.id
        """
        filter_institution = 'AND i.name ILIKE %(institution)s'

    join_city = str()
    filter_city = str()
    if city:
        params['city'] = city.split(';')
        join_city = """
            LEFT JOIN researcher_production rp ON rp.researcher_id = e.researcher_id
        """
        filter_city = 'AND rp.city = ANY(%(city)s)'

    join_area = str()
    filter_area = str()
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_area = """
            LEFT JOIN researcher_production ra ON ra.researcher_id = e.researcher_id
        """
        filter_area = """
            AND STRING_TO_ARRAY(REPLACE(ra.great_area, ' ', '_'), ';') && %(area)s
        """

    join_modality = str()
    filter_modality = str()
    if modality:
        params['modality'] = modality.split(';')
        join_modality = """
            INNER JOIN foment f ON f.researcher_id = e.researcher_id
        """
        filter_modality = 'AND f.modality_name = ANY(%(modality)s)'

    join_graduation = str()
    filter_graduation = str()
    if graduation:
        join_graduation = """
            LEFT JOIN researcher rg ON rg.id = e.researcher_id
        """
        params['graduation'] = graduation.split(';')
        filter_graduation = 'AND rg.graduation = ANY(%(graduation)s)'

    SCRIPT_SQL = f"""
        SELECT e.education_start AS year,
            COUNT(e.degree) AS among,
            REPLACE(degree || '-START', '-', '_') AS degree
        FROM education e
            {join_graduate_program}
            {join_institution}
            {join_city}
            {join_area}
            {join_dep}
            {join_modality}
            {join_graduation}
        WHERE 1 = 1
            {filter_year}
            {filter_dep}
            {filter_id}
            {filter_program}
            {filter_graduate_program}
            {filter_institution}
            {filter_city}
            {filter_area}
            {filter_modality}
            {filter_graduation}
        GROUP BY year, degree

        UNION

        SELECT e.education_end AS year,
            COUNT(e.degree) AS among,
            REPLACE(degree || '-END', '-', '_') AS degree
        FROM education e
            {join_graduate_program}
            {join_institution}
            {join_city}
            {join_dep}
            {join_area}
            {join_modality}
            {join_graduation}
        WHERE 1 = 1
            {filter_year}
            {filter_id}
            {filter_program}
            {filter_graduate_program}
            {filter_institution}
            {filter_dep}
            {filter_city}
            {filter_area}
            {filter_modality}
            {filter_graduation}
        GROUP BY year, degree
    """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_software_metrics(
    term,
    researcher_id: UUID = None,
    program_id: UUID = None,
    dep_id: str = None,
    departament: str = None,
    year: int = None,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    params = {}
    filter_distinct = str()
    join_dep = str()
    filter_dep = str()

    if departament:
        filter_distinct = 'DISTINCT'
        params['dep'] = departament.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = s.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_nom = ANY(%(dep)s)
            """

    if dep_id:
        filter_distinct = 'DISTINCT'
        params['dep_id'] = dep_id.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = s.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_id = ANY(%(dep_id)s)
            """

    if distinct:
        filter_distinct = 'DISTINCT'

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
        filter_year = 'AND s.year >= %(year)s'

    filter_program = str()
    join_program = str()
    if program_id:
        params['program_id'] = program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = s.researcher_id
        """
        filter_program = 'AND gpr.graduate_program_id = %(program_id)s'

    join_graduate_program = str()
    filter_graduate_program = str()
    if graduate_program:
        params['graduate_program'] = graduate_program.split(';')
        join_graduate_program = """
            INNER JOIN graduate_program_researcher gpg ON gpg.researcher_id = s.researcher_id
            INNER JOIN graduate_program gp ON gpg.graduate_program_id = gp.graduate_program_id
        """
        filter_graduate_program = """
            AND gp.name = ANY(%(graduate_program)s)
            AND gpg.type_ = 'PERMANENTE'
        """

    join_institution = str()
    filter_institution = str()
    if institution:
        params['institution'] = institution + '%'
        join_institution = """
            LEFT JOIN researcher r ON r.id = s.researcher_id
            LEFT JOIN institution i ON r.institution_id = i.id
        """
        filter_institution = 'AND i.name ILIKE %(institution)s'

    join_city = str()
    filter_city = str()
    if city:
        params['city'] = city.split(';')
        join_city = """
            LEFT JOIN researcher_production rp ON rp.researcher_id = s.researcher_id
        """
        filter_city = 'AND rp.city = ANY(%(city)s)'

    join_area = str()
    filter_area = str()
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_area = """
            LEFT JOIN researcher_production ra ON ra.researcher_id = s.researcher_id
        """
        filter_area = """
            AND STRING_TO_ARRAY(REPLACE(ra.great_area, ' ', '_'), ';') && %(area)s
        """

    join_modality = str()
    filter_modality = str()
    if modality:
        params['modality'] = modality.split(';')
        join_modality = """
            INNER JOIN foment f ON f.researcher_id = s.researcher_id
        """
        filter_modality = 'AND f.modality_name = ANY(%(modality)s)'

    join_graduation = str()
    filter_graduation = str()
    if graduation:
        join_graduation = """
            LEFT JOIN researcher rg ON rg.id = s.researcher_id
        """
        params['graduation'] = graduation.split(';')
        filter_graduation = 'AND rg.graduation = ANY(%(graduation)s)'

    SCRIPT_SQL = f"""
        SELECT s.year, COUNT({filter_distinct} s.title) among
        FROM public.software s
            {join_program}
            {join_graduate_program}
            {join_institution}
            {join_dep}
            {join_city}
            {join_area}
            {join_modality}
            {join_graduation}
        WHERE 1 = 1
            {filter_id}
            {term_filter}
            {filter_year}
            {filter_program}
            {filter_dep}
            {filter_graduate_program}
            {filter_institution}
            {filter_city}
            {filter_area}
            {filter_modality}
            {filter_graduation}
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
    term,
    researcher_id,
    graduate_program_id,
    dep_id,
    dep,
    year,
    institution,
    graduate_program,
    city,
    area,
    modality,
    graduation,
    page,
    lenght,
):
    params = {}
    join_researcher = str()
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    filters = str()
    filter_pagination = str()

    if term:
        # Assumindo que webseatch_filter retorna uma tupla (filter_string, params_dict)
        filter_terms_str, term_params = webseatch_filter('p.title', term)
        filters += filter_terms_str
        params.update(term_params)

    if year:
        params['year'] = year
        filters += """
            AND p.development_year::INT >= %(year)s
            """

    if dep_id or dep:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = p.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if dep_id:
        params['dep_id'] = dep_id
        filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if dep:
        params['dep'] = dep.split(';')
        filters += """
            AND dp.dep_nom = ANY(%(dep)s)
            """

    if researcher_id:
        params['researcher_id'] = researcher_id
        filters += """
            AND p.researcher_id = %(researcher_id)s
            """

    if institution:
        params['institution'] = institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        filters += """
            AND i.name = ANY(%(institution)s)
            """

    if graduate_program_id:
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = p.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if graduate_program:
        filter_distinct = 'DISTINCT'  # Pode ser útil se a intenção for listar patentes únicas associadas a um programa.
        params['graduate_program'] = graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = p.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if city:
        params['city'] = city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = p.researcher_id
            """
        filters += """
            AND rp.city = ANY(%(city)s)
            """
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = p.researcher_id
            """
        filters += """
            AND rp.great_area_ && %(area)s
            """

    if modality:
        filter_distinct = 'DISTINCT'  # Pode ser útil se a intenção for listar patentes únicas associadas a uma modalidade de fomento.
        params['modality'] = modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = p.researcher_id
            """
        filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if graduation:
        params['graduation'] = graduation.split(';')
        # O join_researcher já está na query principal, então não precisa adicionar de novo
        filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            p.id, p.title, p.category, p.relevance, p.has_image,
            p.development_year as year, p.details, p.grant_date, p.deposit_date,
            r.id AS researcher, r.lattes_id, r.name as name
        FROM public.patent p
            INNER JOIN public.researcher r ON r.id = p.researcher_id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
        WHERE 1 = 1
            {filters}
        ORDER BY p.development_year DESC
        {filter_pagination};
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def list_brand(
    term,
    researcher_id,
    graduate_program_id,
    dep_id,
    departament,
    year,
    institution,
    graduate_program,
    city,
    area,
    modality,
    graduation,
    page,
    lenght,
):
    params = {}
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    filters = str()
    filter_pagination = str()

    if term:
        filter_terms_str, term_params = webseatch_filter('b.title', term)
        filters += filter_terms_str
        params.update(term_params)

    if year:
        params['year'] = year
        filters += """
            AND b.year::INT >= %(year)s
            """

    if dep_id or departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = b.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if dep_id:
        params['dep_id'] = dep_id
        filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if departament:
        params['departament'] = departament.split(';')
        filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if researcher_id:
        params['researcher_id'] = researcher_id
        filters += """
            AND b.researcher_id = %(researcher_id)s
            """

    if institution:
        params['institution'] = institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        filters += """
            AND i.name = ANY(%(institution)s)
            """

    if graduate_program_id:
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = b.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = b.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if city:
        params['city'] = city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = b.researcher_id
            """
        filters += """
            AND rp.city = ANY(%(city)s)
            """
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = b.researcher_id
            """
        filters += """
            AND rp.great_area_ && %(area)s
            """

    if modality:
        filter_distinct = 'DISTINCT'
        params['modality'] = modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = b.researcher_id
            """
        filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if graduation:
        params['graduation'] = graduation.split(';')
        filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            b.id, b.title, b.year, b.has_image, b.relevance,
            b.goal, b.nature,
            r.id AS researcher_id, r.lattes_id, r.name
        FROM public.brand b
            INNER JOIN public.researcher r ON r.id = b.researcher_id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
        WHERE 1 = 1
            {filters}
        ORDER BY b.year DESC
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
    term,
    researcher_id,
    graduate_program_id,
    dep_id,
    departament,
    year,
    distinct,
    institution,
    graduate_program,
    city,
    area,
    modality,
    graduation,
    page,
    lenght,
):
    params = {}
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    filters = str()
    filter_pagination = str()

    if term:
        filter_terms_str, term_params = webseatch_filter('bp.title', term)
        filters += filter_terms_str
        params.update(term_params)

    if year:
        params['year'] = year
        filters += """
            AND bp.year::INT >= %(year)s
            """

    if dep_id or departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = bp.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if dep_id:
        params['dep_id'] = dep_id
        filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if departament:
        params['departament'] = departament.split(';')
        filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if distinct:
        filter_distinct = 'DISTINCT'

    if researcher_id:
        params['researcher_id'] = researcher_id
        filters += """
            AND bp.researcher_id = %(researcher_id)s
            """

    if institution:
        params['institution'] = institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        filters += """
            AND i.name = ANY(%(institution)s)
            """

    if graduate_program_id:
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if city:
        params['city'] = city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = bp.researcher_id
            """
        filters += """
            AND rp.city = ANY(%(city)s)
            """
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = bp.researcher_id
            """
        filters += """
            AND rp.great_area_ && %(area)s
            """

    if modality:
        filter_distinct = 'DISTINCT'
        params['modality'] = modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = bp.researcher_id
            """
        filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if graduation:
        params['graduation'] = graduation.split(';')
        filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            bp.title, bp.year, bpb.isbn AS isbn,
            bpb.publishing_company AS publishing_company,
            bp.researcher_id AS researcher,
            r.lattes_id AS lattes_id, bp.relevance,
            bp.has_image, bp.id, r.name
        FROM bibliographic_production bp
            INNER JOIN bibliographic_production_book bpb
                ON bp.id = bpb.bibliographic_production_id
            INNER JOIN researcher r
                ON r.id = bp.researcher_id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
        WHERE 1 = 1
            {filters}
        ORDER BY year desc
        {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_bibliographic_production(
    type,
    qualis,
    term,
    researcher_id,
    graduate_program_id,
    dep_id,
    departament,
    year,
    institution,
    graduate_program,
    city,
    area,
    modality,
    graduation,
    page,
    lenght,
):
    params = {}
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_departament = str()

    filter_distinct = str()
    filters = str()
    filter_pagination = str()

    if term:
        filter_terms_str, term_params = webseatch_filter('b.title', term)
        filters += filter_terms_str
        params.update(term_params)

    if type:
        params['type'] = type.split(';')
        filters += """
            AND b.type = ANY(%(type)s)
            """

    if qualis:
        params['qualis'] = qualis.split(';')
        filters += """
            AND bpa.qualis = ANY(%(qualis)s)
            """

    if year:
        params['year'] = year
        filters += """
            AND b.year::INT >= %(year)s
            """

    if dep_id or departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = b.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if dep_id:
        params['dep_id'] = dep_id
        filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if departament:
        params['departament'] = departament.split(';')
        filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if researcher_id:
        params['researcher_id'] = researcher_id
        filters += """
            AND b.researcher_id = %(researcher_id)s
            """

    if institution:
        params['institution'] = institution.split(';')
        filters += """
            AND i.name = ANY(%(institution)s)
            """

    if graduate_program_id:
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = b.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = b.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if city:
        params['city'] = city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = b.researcher_id
            """
        filters += """
            AND rp.city = ANY(%(city)s)
            """
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = b.researcher_id
            """
        filters += """
            AND rp.great_area_ && %(area)s
            """

    if modality:
        filter_distinct = 'DISTINCT'
        params['modality'] = modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = b.researcher_id
            """
        filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if graduation:
        params['graduation'] = graduation.split(';')
        filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            b.id AS id, title, year, type, doi, bpa.qualis,
            periodical_magazine_name AS magazine, r.name AS researcher,
            r.lattes_10_id, r.lattes_id, jcr AS jif,
            jcr_link, r.id AS researcher_id, opa.abstract,
            opa.article_institution, opa.authors, opa.authors_institution,
            COALESCE (opa.citations_count, 0) AS citations_count, bpa.issn,
            opa.keywords, opa.landing_page_url, opa.language, opa.pdf,
            b.has_image, b.relevance, bpa.created_at AS created_at
        FROM bibliographic_production b
            LEFT JOIN bibliographic_production_article bpa
                ON b.id = bpa.bibliographic_production_id
            LEFT JOIN researcher r
                ON r.id = b.researcher_id
            LEFT JOIN institution i
                ON r.institution_id = i.id
            LEFT JOIN openalex_article opa
                ON opa.article_id = b.id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
        WHERE 1 = 1
            {filters}
        ORDER BY
            year DESC
        {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_article_production(
    qualis,
    term,
    researcher_id,
    graduate_program_id,
    dep_id,
    departament,
    year,
    institution,
    graduate_program,
    city,
    area,
    modality,
    graduation,
    page,
    lenght,
    distinct,
):
    params = {}
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_departament = str()

    filter_distinct = str()
    filters = str()
    filter_pagination = str()

    if qualis:
        params['qualis'] = qualis.split(';')
        filters += 'AND bpa.qualis = ANY(%(qualis)s)'

    if term:
        filter_terms_str, term_params = webseatch_filter('b.title', term)
        filters += filter_terms_str
        params.update(term_params)

    if year:
        params['year'] = year
        filters += """
            AND b.year::INT >= %(year)s
            """

    if dep_id or departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = b.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if dep_id:
        params['dep_id'] = dep_id
        filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if departament:
        params['departament'] = departament.split(';')
        filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if distinct:
        filter_distinct = 'DISTINCT'

    if researcher_id:
        params['researcher_id'] = researcher_id
        filters += """
            AND b.researcher_id = %(researcher_id)s
            """

    if institution:
        params['institution'] = institution.split(';')
        filters += """
            AND i.name = ANY(%(institution)s)
            """

    if graduate_program_id:
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = b.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = b.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if city:
        params['city'] = city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = b.researcher_id
            """
        filters += """
            AND rp.city = ANY(%(city)s)
            """
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = b.researcher_id
            """
        filters += """
            AND rp.great_area_ && %(area)s
            """

    if modality:
        filter_distinct = 'DISTINCT'
        params['modality'] = modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = b.researcher_id
            """
        filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if graduation:
        params['graduation'] = graduation.split(';')
        filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
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
            LEFT JOIN institution i
                ON r.institution_id = i.id
            LEFT JOIN openalex_article opa
                ON opa.article_id = b.id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
        WHERE 1 = 1
            {filters}
        ORDER BY
            b.year DESC
        {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
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
            {filter_program}
            {filter_university}
            {filter_dep}
        GROUP BY b.title
        ORDER BY year DESC
            {filter_pagination}
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_book_chapter(
    term,
    researcher_id,
    graduate_program_id,
    dep_id,
    departament,
    year,
    institution,
    graduate_program,
    city,
    area,
    modality,
    graduation,
    page,
    lenght,
):
    params = {}
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    filters = str()
    filter_pagination = str()

    if term:
        filter_terms_str, term_params = webseatch_filter('bp.title', term)
        filters += filter_terms_str
        params.update(term_params)

    if year:
        params['year'] = year
        filters += """
            AND bp.year::INT >= %(year)s
            """

    if dep_id or departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = bp.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if dep_id:
        params['dep_id'] = dep_id
        filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if departament:
        params['departament'] = departament.split(';')
        filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if researcher_id:
        params['researcher_id'] = researcher_id
        filters += """
            AND bp.researcher_id = %(researcher_id)s
            """

    if institution:
        params['institution'] = institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        filters += """
            AND i.name = ANY(%(institution)s)
            """

    if graduate_program_id:
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if city:
        params['city'] = city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = bp.researcher_id
            """
        filters += """
            AND rp.city = ANY(%(city)s)
            """
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = bp.researcher_id
            """
        filters += """
            AND rp.great_area_ && %(area)s
            """

    if modality:
        filter_distinct = 'DISTINCT'
        params['modality'] = modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = bp.researcher_id
            """
        filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if graduation:
        params['graduation'] = graduation.split(';')
        filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            bp.title, bp.year, bpc.isbn, bpc.publishing_company,
            bp.researcher_id AS researcher, bp.id, r.lattes_id, bp.relevance,
            bp.has_image, r.name
        FROM bibliographic_production bp
            INNER JOIN bibliographic_production_book_chapter bpc
                ON bpc.bibliographic_production_id = bp.id
            LEFT JOIN researcher r
                ON r.id = bp.researcher_id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
        WHERE 1 = 1
            {filters}
        ORDER BY bp.year DESC
        {filter_pagination};
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


def list_software(
    term,
    researcher_id,
    graduate_program_id,
    dep_id,
    departament,
    year,
    institution,
    graduate_program,
    city,
    area,
    modality,
    graduation,
    page,
    lenght,
    distinct,
):
    params = {}

    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    filters = str()
    filter_pagination = str()

    if term:
        filter_terms_str, term_params = webseatch_filter('s.title', term)
        filters += filter_terms_str
        params.update(term_params)

    if year:
        params['year'] = year
        filters += """
            AND s.year::INT >= %(year)s
            """

    if dep_id or departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = s.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if dep_id:
        params['dep_id'] = dep_id
        filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if departament:
        params['departament'] = departament.split(';')
        filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if distinct:
        filter_distinct = 'DISTINCT'

    if researcher_id:
        params['researcher_id'] = researcher_id
        filters += """
            AND s.researcher_id = %(researcher_id)s
            """

    if institution:
        params['institution'] = institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        filters += """
            AND i.name = ANY(%(institution)s)
            """

    if graduate_program_id:
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = s.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = s.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if city:
        params['city'] = city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = s.researcher_id
            """
        filters += """
            AND rp.city = ANY(%(city)s)
            """
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = s.researcher_id
            """
        filters += """
            AND rp.great_area_ && %(area)s
            """

    if modality:
        filter_distinct = 'DISTINCT'
        params['modality'] = modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = s.researcher_id
            """
        filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if graduation:
        params['graduation'] = graduation.split(';')
        filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            s.id, s.title, s.year AS year, s.has_image, s.relevance,
            s.platform, s.goal, s.environment, s.availability, s.financing_institutionc,
            r.name, r.id AS researcher_id, r.lattes_id
        FROM public.software s
        LEFT JOIN public.researcher r ON s.researcher_id = r.id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
        WHERE 1 = 1
            {filters}
        ORDER BY s.year DESC
        {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_researcher_report(
    term,
    researcher_id,
    graduate_program_id,
    dep_id,
    departament,
    year,
    distinct,
    institution,
    graduate_program,
    city,
    area,
    modality,
    graduation,
    page,
    lenght,
):
    params = {}
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    filters = str()
    filter_pagination = str()

    if term:
        filter_terms_str, term_params = webseatch_filter('rr.title', term)
        filters += filter_terms_str
        params.update(term_params)

    if year:
        params['year'] = year
        filters += """
            AND rr.year::INT >= %(year)s
            """

    if dep_id or departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = rr.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if dep_id:
        params['dep_id'] = dep_id
        filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if departament:
        params['departament'] = departament.split(';')
        filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if distinct:
        filter_distinct = 'DISTINCT'

    if researcher_id:
        params['researcher_id'] = researcher_id
        filters += """
            AND rr.researcher_id = %(researcher_id)s
            """

    if institution:
        params['institution'] = institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        filters += """
            AND i.name = ANY(%(institution)s)
            """

    if graduate_program_id:
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = rr.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = rr.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if city:
        params['city'] = city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = rr.researcher_id
            """
        filters += """
            AND rp.city = ANY(%(city)s)
            """
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = rr.researcher_id
            """
        filters += """
            AND rp.great_area_ && %(area)s
            """

    if modality:
        filter_distinct = 'DISTINCT'
        params['modality'] = modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = rr.researcher_id
            """
        filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if graduation:
        params['graduation'] = graduation.split(';')
        filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            rr.id, r.name, rr.title, rr.year, rr.project_name,
            rr.financing_institutionc AS financing
        FROM public.research_report rr
        LEFT JOIN public.researcher r ON rr.researcher_id = r.id
        {join_researcher_production}
        {join_foment}
        {join_program}
        {join_departament}
        {join_institution}
        WHERE 1 = 1
            {filters}
        ORDER BY rr.year DESC
        {filter_pagination};
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
    term,
    researcher_id,
    graduate_program_id,
    dep_id,
    departament,
    year,
    institution,
    graduate_program,
    city,
    area,
    modality,
    graduation,
    page,
    lenght,
    distinct,
):
    params = {}

    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    filters = str()
    filter_pagination = str()

    if term:
        filter_terms_str, term_params = webseatch_filter('g.title', term)
        filters += filter_terms_str
        params.update(term_params)

    if year:
        params['year'] = year
        filters += """
            AND g.year::INT >= %(year)s
            """

    if dep_id or departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = g.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if dep_id:
        params['dep_id'] = dep_id
        filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if departament:
        params['departament'] = departament.split(';')
        filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if distinct:
        filter_distinct = 'DISTINCT'

    if researcher_id:
        params['researcher_id'] = researcher_id
        filters += """
            AND g.researcher_id = %(researcher_id)s
            """

    if institution:
        params['institution'] = institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        filters += """
            AND i.name = ANY(%(institution)s)
            """

    if graduate_program_id:
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = g.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = g.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if city:
        params['city'] = city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = g.researcher_id
            """
        filters += """
            AND rp.city = ANY(%(city)s)
            """
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = g.researcher_id
            """
        filters += """
            AND rp.great_area_ && %(area)s
            """

    if modality:
        filter_distinct = 'DISTINCT'
        params['modality'] = modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = g.researcher_id
            """
        filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if graduation:
        params['graduation'] = graduation.split(';')
        filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            g.id, g.title, nature, g.oriented, g.type, g.status,
            g.year, r.name, r.id AS researcher_id, r.lattes_id
        FROM public.guidance g
            INNER JOIN public.researcher r ON g.researcher_id = r.id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
        WHERE 1 = 1
            {filters}
        ORDER BY g.year DESC
        {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_researcher_production_events(
    term,
    researcher_id,
    graduate_program_id,
    dep_id,
    departament,
    year,
    institution,
    graduate_program,
    city,
    area,
    modality,
    graduation,
    page,
    lenght,
    distinct,
):
    params = {}

    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    filters = "AND bp.type = 'WORK_IN_EVENT'"
    filter_pagination = str()

    if term:
        filter_terms_str, term_params = webseatch_filter('bp.title', term)
        filters += filter_terms_str
        params.update(term_params)

    if year:
        params['year'] = year
        filters += """
            AND bp.year_::INT >= %(year)s
            """

    if dep_id or departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = bp.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if dep_id:
        params['dep_id'] = dep_id
        filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if departament:
        params['departament'] = departament.split(';')
        filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if distinct:
        filter_distinct = 'DISTINCT'

    if researcher_id:
        params['researcher_id'] = researcher_id
        filters += """
            AND bp.researcher_id = %(researcher_id)s
            """

    if institution:
        params['institution'] = institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        filters += """
            AND i.name = ANY(%(institution)s)
            """

    if graduate_program_id:
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if city:
        params['city'] = city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = bp.researcher_id
            """
        filters += """
            AND rp.city = ANY(%(city)s)
            """
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = bp.researcher_id
            """
        filters += """
            AND rp.great_area_ && %(area)s
            """

    if modality:
        filter_distinct = 'DISTINCT'
        params['modality'] = modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = bp.researcher_id
            """
        filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if graduation:
        params['graduation'] = graduation.split(';')
        filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            bp.id, bp.title, bp.title_en, bp.nature, bp.language,
            bp.means_divulgation, bp.homepage, bp.relevance,
            bp.scientific_divulgation, bp.authors, bp.year_,
            r.name, r.id AS researcher_id, r.lattes_id, r.lattes_10_id
        FROM public.bibliographic_production bp
            LEFT JOIN public.researcher r ON r.id = bp.researcher_id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
        WHERE 1 = 1
            {filters}
        ORDER BY bp.year_ DESC
        {filter_pagination};
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
    term,
    researcher_id,
    graduate_program_id,
    dep_id,
    departament,
    year,
    institution,
    graduate_program,
    city,
    area,
    modality,
    graduation,
    page,
    lenght,
    distinct,
):
    params = {}

    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    filters = str()
    filter_pagination = str()

    if term:
        filter_terms_str, term_params = webseatch_filter('rp.project_name', term)
        filters += filter_terms_str
        params.update(term_params)

    if year:
        params['year'] = year
        filters += """
            AND rp.start_year >= %(year)s
            """

    if dep_id or departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = rp.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if dep_id:
        params['dep_id'] = dep_id
        filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if departament:
        params['departament'] = departament.split(';')
        filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if distinct:
        filter_distinct = 'DISTINCT'

    if researcher_id:
        params['researcher_id'] = researcher_id
        filters += """
            AND rp.researcher_id = %(researcher_id)s
            """

    if institution:
        params['institution'] = institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        filters += """
            AND i.name = ANY(%(institution)s)
            """

    if graduate_program_id:
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = rp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = rp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if city:
        params['city'] = city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp_prod
                ON rp_prod.researcher_id = rp.researcher_id
            """
        filters += """
            AND rp_prod.city = ANY(%(city)s)
            """
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp_prod
                ON rp_prod.researcher_id = rp.researcher_id
            """
        filters += """
            AND rp_prod.great_area_ && %(area)s
            """

    if modality:
        filter_distinct = 'DISTINCT'
        params['modality'] = modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = rp.researcher_id
            """
        filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if graduation:
        params['graduation'] = graduation.split(';')
        filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            rp.id, rp.researcher_id, r.name, rp.start_year, rp.end_year,
            rp.agency_code, rp.agency_name, rp.project_name, rp.status,
            rp.nature, rp.number_undergraduates, rp.number_specialists,
            rp.number_academic_masters, rp.number_phd, rp.description,
            rpp.production, rpf.foment, rpc.components
        FROM public.research_project rp
            LEFT JOIN researcher r ON r.id = rp.researcher_id
            LEFT JOIN (SELECT project_id, JSONB_AGG(JSONB_BUILD_OBJECT('title', title, 'type', type)) AS production
                FROM public.research_project_production
                GROUP BY project_id) rpp ON rpp.project_id = rp.id
            LEFT JOIN (SELECT project_id, JSONB_AGG(JSONB_BUILD_OBJECT('agency_name', agency_name, 'agency_code', agency_code, 'nature', nature)) AS foment
                FROM public.research_project_foment
                GROUP BY project_id) rpf ON rpf.project_id = rp.id
            LEFT JOIN (SELECT project_id, JSONB_AGG(JSONB_BUILD_OBJECT('name', name, 'lattes_id', lattes_id, 'citations', citations)) AS components
                FROM public.research_project_components
                GROUP BY project_id) rpc ON rpc.project_id = rp.id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
        WHERE 1 = 1
            {filters}
        ORDER BY rp.start_year DESC
        {filter_pagination};
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
    term,
    researcher_id,
    graduate_program_id,
    dep_id,
    departament,
    year,
    institution,
    graduate_program,
    city,
    area,
    modality,
    graduation,
    page,
    lenght,
    distinct,
):
    params = {}

    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    filters = str()
    filter_pagination = str()

    if term:
        filter_terms_str, term_params = webseatch_filter('bp.title', term)
        filters += filter_terms_str
        params.update(term_params)

    if year:
        params['year'] = year
        filters += """
            AND bp.year_::INT >= %(year)s
            """

    if dep_id or departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = bp.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if dep_id:
        params['dep_id'] = dep_id
        filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if departament:
        params['departament'] = departament.split(';')
        filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if distinct:
        filter_distinct = 'DISTINCT'

    if researcher_id:
        params['researcher_id'] = researcher_id
        filters += """
            AND bp.researcher_id = %(researcher_id)s
            """

    if institution:
        params['institution'] = institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        filters += """
            AND i.name = ANY(%(institution)s)
            """

    if graduate_program_id:
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if city:
        params['city'] = city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = bp.researcher_id
            """
        filters += """
            AND rp.city = ANY(%(city)s)
            """
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = bp.researcher_id
            """
        filters += """
            AND rp.great_area_ && %(area)s
            """

    if modality:
        filter_distinct = 'DISTINCT'
        params['modality'] = modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = bp.researcher_id
            """
        filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if graduation:
        params['graduation'] = graduation.split(';')
        filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            title, title_en, nature, language, means_divulgation, homepage,
            relevance, scientific_divulgation, authors, year_, r.name,
            bp.id, bp.researcher_id, r.lattes_id
        FROM public.bibliographic_production bp
            INNER JOIN researcher r ON bp.researcher_id = r.id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
        WHERE type = 'TEXT_IN_NEWSPAPER_MAGAZINE'
            {filters}
        ORDER BY year_ DESC
        {filter_pagination};
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
