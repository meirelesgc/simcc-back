from uuid import UUID

import nltk

from simcc.core.connection import Connection
from simcc.repositories import conn
from simcc.repositories.tools import names_filter, pagination, websearch_filter
from simcc.schemas import DefaultFilters, YearBarema


async def get_magazine(conn, issn, initials, page, lenght):
    params = {}
    filters = str()
    if initials:
        params['initials'] = initials.lower() + '%'
        filters += 'AND LOWER(name) like %(initials)s'
    if issn:
        params['issn'] = issn
        filters += "AND translate(issn, '-', '')= %(issn)s"
    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT m.id as id, m.name as magazine, issn,jcr,jcr_link,qualis
        FROM periodical_magazine m
        WHERE 1 = 1
            {filters}
        ORDER BY jcr asc
        {filter_pagination}
        """
    return await conn.select(SCRIPT_SQL, params)


async def lattes_update(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}
    join_departament = str()
    join_institution = str()
    join_program = str()
    join_researcher_production = str()
    join_foment = str()
    join_type_specific = str()
    general_filters = str()
    type_specific_filters = str()

    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        general_filters += ' AND r.id = %(researcher_id)s'

    if filters.dep_id or filters.departament:
        join_departament = """
            INNER JOIN public.departament_researcher dpr ON dpr.researcher_id = r.id
            INNER JOIN public.departament dp ON dp.dep_id = dpr.dep_id
        """
        if filters.dep_id:
            params['dep_id'] = filters.dep_id.split(';')
            general_filters += ' AND dp.dep_id = ANY(%(dep_id)s)'
        if filters.departament:
            params['departament'] = filters.departament.split(';')
            general_filters += ' AND dp.dep_nom = ANY(%(departament)s)'

    if filters.institution:
        join_institution = (
            ' INNER JOIN public.institution i ON r.institution_id = i.id'
        )
        params['institution'] = filters.institution.split(';')
        general_filters += ' AND i.name = ANY(%(institution)s)'

    if filters.graduate_program_id or filters.graduate_program:
        join_program = """
            INNER JOIN public.graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN public.graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        if filters.graduate_program_id:
            params['graduate_program_id'] = str(filters.graduate_program_id)
            general_filters += (
                ' AND gpr.graduate_program_id = %(graduate_program_id)s'
            )
        if filters.graduate_program:
            params['graduate_program'] = filters.graduate_program.split(';')
            general_filters += ' AND gp.name = ANY(%(graduate_program)s)'

    if filters.city:
        join_researcher_production = ' LEFT JOIN public.researcher_production rp ON rp.researcher_id = r.id'
        params['city'] = filters.city.split(';')
        general_filters += ' AND rp.city = ANY(%(city)s)'

    if filters.area:
        if not join_researcher_production:
            join_researcher_production = ' LEFT JOIN public.researcher_production rp ON rp.researcher_id = r.id'
        params['area'] = filters.area.replace(' ', '_').split(';')
        general_filters += " AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s"

    if filters.modality:
        join_foment = ' INNER JOIN public.foment f ON f.researcher_id = r.id'
        params['modality'] = filters.modality.split(';')
        general_filters += ' AND f.modality_name = ANY(%(modality)s)'

    if filters.graduation:
        params['graduation'] = filters.graduation.split(';')
        general_filters += ' AND r.graduation = ANY(%(graduation)s)'

    if filters.type:
        match filters.type:
            case 'ABSTRACT':
                if filters.term:
                    term_filter_str, term_params = websearch_filter(
                        'r.abstract', filters.term
                    )
                    type_specific_filters += term_filter_str
                    params.update(term_params)

            case (
                'BOOK'
                | 'BOOK_CHAPTER'
                | 'ARTICLE'
                | 'WORK_IN_EVENT'
                | 'TEXT_IN_NEWSPAPER_MAGAZINE'
            ):
                join_type_specific = f"INNER JOIN public.bibliographic_production bp ON bp.researcher_id = r.id AND bp.type = '{filters.type}'"
                if filters.term:
                    term_filter_str, term_params = websearch_filter(
                        'bp.title', filters.term
                    )
                    type_specific_filters += term_filter_str
                    params.update(term_params)
                if filters.year:
                    type_specific_filters += ' AND bp.year_ >= %(year)s'
                    params['year'] = filters.year

            case 'PATENT':
                join_type_specific = (
                    'INNER JOIN public.patent p ON p.researcher_id = r.id'
                )
                if filters.term:
                    term_filter_str, term_params = websearch_filter(
                        'p.title', filters.term
                    )
                    type_specific_filters += term_filter_str
                    params.update(term_params)
                if filters.year:
                    type_specific_filters += (
                        ' AND p.development_year::INT >= %(year)s'
                    )
                    params['year'] = filters.year

            case 'AREA':
                if not join_researcher_production:
                    join_researcher_production = 'LEFT JOIN public.researcher_production rp ON rp.researcher_id = r.id'
                if filters.term:
                    term_filter_str, term_params = websearch_filter(
                        'rp.great_area', filters.term
                    )
                    type_specific_filters += term_filter_str
                    params.update(term_params)

            case 'EVENT':
                join_type_specific = 'INNER JOIN public.event_organization e ON e.researcher_id = r.id'
                if filters.term:
                    term_filter_str, term_params = websearch_filter(
                        'e.title', filters.term
                    )
                    type_specific_filters += term_filter_str
                    params.update(term_params)
                if filters.year:
                    type_specific_filters += ' AND e.year >= %(year)s'
                    params['year'] = filters.year

            case 'NAME':
                if filters.term:
                    name_filter_str, term_params = names_filter(
                        'r.name', filters.term
                    )
                    type_specific_filters += name_filter_str
                    params.update(term_params)

    SCRIPT_SQL = f"""
        SELECT
            COUNT(DISTINCT r.id) AS total,
            COUNT(DISTINCT r.id) FILTER (WHERE r.last_update < CURRENT_DATE - INTERVAL '3 months') AS over_3_months,
            COUNT(DISTINCT r.id) FILTER (WHERE r.last_update < CURRENT_DATE - INTERVAL '6 months') AS over_6_months
        FROM
            researcher r
        {join_departament}
        {join_institution}
        {join_program}
        {join_researcher_production}
        {join_foment}
        {join_type_specific}
        WHERE 1=1
            {general_filters}
            {type_specific_filters}
    """

    return await conn.select(SCRIPT_SQL, params)


def lattes_list(names: list = None, lattes: list = None) -> dict:
    one = False
    params = {}

    filter_lattes = str()
    if lattes:
        params['lattes'] = lattes
        filter_lattes = 'AND r.lattes_id = ANY(%(lattes)s)'

    filter_names = str()
    if names:
        params['names'] = names
        filter_names = 'AND r.name = ANY(%(names)s)'

    SCRIPT_SQL = f"""
        SELECT r.id AS researcher_id, r.lattes_id, r.name AS researcher,
            r.lattes_10_id, r.graduation, rp.city, i.name as university,
            rp.great_area AS area
        FROM researcher r
        LEFT JOIN researcher_production rp
            ON r.id = rp.researcher_id
        LEFT JOIN institution i
            ON r.institution_id = i.id
        WHERE 1 = 1
            {filter_names}
            {filter_lattes}
        """
    result = conn.select(SCRIPT_SQL, params, one)
    return result


def production(lattes_list: list, year: YearBarema): ...


def get_researcher_foment(institution_id: UUID):
    params = {}
    filter_institution = str()
    if institution_id:
        params['institution_id'] = institution_id
        filter_institution = 'AND r.institution_id = %(institution_id)s'

    SCRIPT_SQL = f"""
        SELECT s.researcher_id, r.name, s.modality_code, s.modality_name,
            s.call_title, s.category_level_code, s.funding_program_name,
            s.institute_name, s.aid_quantity, s.scholarship_quantity
        FROM foment s
            LEFT JOIN researcher r
                ON s.researcher_id = r.id
        WHERE 1 = 1
            AND s.researcher_id IS NOT NULL
            {filter_institution}
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def get_logs():
    SCRIPT_SQL = """
        SELECT DISTINCT ON (type) type AS routine_type, error, detail,
            created_at
        FROM logs.routine
        ORDER BY type, created_at DESC;
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_words(term: str):
    params = {'term': term + '%'}

    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    params['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        WITH words AS (
            SELECT regexp_split_to_table(translate(b.title,'-\.:,;''', ' '), '\s+') AS word
            FROM bibliographic_production b
            WHERE type = 'ARTICLE'),
        words_count AS (
            SELECT COUNT(*) AS frequency, LOWER(word) AS word
            FROM words
            WHERE word ~ '\w+'
            GROUP BY LOWER(word))
        SELECT word, frequency AS freq
        FROM words_count
        WHERE CHAR_LENGTH(word) > 3
            AND TRIM(word) <> ALL(%(stopwords)s)
            AND unaccent(word) ILIKE %(term)s
        ORDER BY frequency DESC
        LIMIT 10;
        """
    result = conn.select(SCRIPT_SQL, params)
    return result
