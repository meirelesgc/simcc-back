from datetime import datetime
from uuid import UUID

import nltk
import pandas as pd

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
        if filters.graduate_program_id:
            params['graduate_program_id'] = str(filters.graduate_program_id)
            general_filters += """
                AND EXISTS (
                    SELECT 1 FROM public.graduate_program_researcher gpr
                    WHERE gpr.researcher_id = r.id
                    AND gpr.graduate_program_id = %(graduate_program_id)s
                )
            """
        if filters.graduate_program:
            join_program = 'INNER JOIN public.graduate_program gp ON gp.graduate_program_id IN (SELECT graduate_program_id FROM public.graduate_program_researcher gpr WHERE gpr.researcher_id = r.id)'
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


async def get_researchers_by_city(conn, city):
    SQL = """
        SELECT city
        FROM researcher_production
        WHERE city ILIKE %(city)s
        OR similarity(city, %(city)s) > 0.3
        ORDER BY
            (city ILIKE %(city)s) DESC,
            similarity(city, %(city)s) DESC
        LIMIT 1;
    """
    result = await conn.select(SQL, {'city': city + '%'}, True)
    return result.get('city', None)


async def generic_data(
    conn,
    year: int | None = None,
    graduate_program_id: UUID | None = None,
    dep_id: UUID | None = None,
):
    params = {}
    FILTERS = ''
    params['year'] = year

    years = list(range(int(year), datetime.now().year + 1))
    data_frame = pd.DataFrame(years, columns=['year'])

    if graduate_program_id:
        FILTERS += """
            AND researcher_id IN (
                SELECT researcher_id
                FROM graduate_program_researcher
                WHERE graduate_program_id = %(graduate_program_id)s
            )
        """
        params['graduate_program_id'] = graduate_program_id

    if dep_id:
        FILTERS += """
            AND researcher_id IN (
                SELECT researcher_id
                FROM ufmg.departament_researcher
                WHERE dep_id = %(dep_id)s
            )
        """
        params['dep_id'] = dep_id

    SQL = f"""
        SELECT g.year, COUNT(*) as count_guidance,
            COUNT(CASE WHEN g.status = 'Concluída' THEN 1 ELSE NULL END) as count_guidance_complete,
            COUNT(CASE WHEN g.status = 'Em andamento' THEN 1 ELSE NULL END) as count_guidance_in_progress
        FROM guidance g
        WHERE g.year >= %(year)s
            {FILTERS}
        GROUP BY g.year
        ORDER BY g.year;
    """
    result = await conn.select(SQL, params)
    df = pd.DataFrame(
        result,
        columns=[
            'year',
            'count_guidance',
            'count_guidance_complete',
            'count_guidance_in_progress',
        ],
    )
    data_frame = pd.merge(data_frame, df, on='year', how='left')

    SQL = f"""
        SELECT bp.year::smallint, COUNT(DISTINCT title) AS count_book
        FROM public.bibliographic_production bp
        WHERE type = 'BOOK'
            AND bp.year::smallint >= %(year)s
            {FILTERS}
        GROUP BY bp.year
        ORDER BY bp.year;
    """
    result = await conn.select(SQL, params)
    df = pd.DataFrame(result, columns=['year', 'count_book'])
    data_frame = pd.merge(data_frame, df, on='year', how='left')

    SQL = f"""
        SELECT bp.year::smallint, COUNT(DISTINCT title) AS count_book_chapter
        FROM public.bibliographic_production bp
        WHERE type = 'BOOK_CHAPTER'
            AND bp.year::smallint >= %(year)s
            {FILTERS}
        GROUP BY bp.year
        ORDER BY bp.year;
    """
    result = await conn.select(SQL, params)
    df = pd.DataFrame(result, columns=['year', 'count_book_chapter'])
    data_frame = pd.merge(data_frame, df, on='year', how='left')

    SQL = f"""
        SELECT p.development_year::smallint AS year,
            COUNT(CASE WHEN p.grant_date IS NULL THEN 1 ELSE NULL END) count_not_granted_patent,
            COUNT(CASE WHEN p.grant_date IS NOT NULL THEN 1 ELSE NULL END) as count_granted_patent,
            COUNT(*) as count_total
        FROM patent p
        WHERE p.development_year::smallint >= %(year)s
            {FILTERS}
        GROUP BY p.development_year
        ORDER BY p.development_year;
    """
    result = await conn.select(SQL, params)
    df = pd.DataFrame(
        result,
        columns=[
            'year',
            'count_not_granted_patent',
            'count_granted_patent',
            'count_total',
        ],
    )
    data_frame = pd.merge(data_frame, df, on='year', how='left')

    SQL = f"""
        SELECT sw.year::smallint, COUNT(DISTINCT title) AS count_software
        FROM public.software sw
        WHERE sw.year::smallint >= %(year)s
            {FILTERS}
        GROUP BY sw.year
        ORDER BY sw.year;
    """
    result = await conn.select(SQL, params)
    df = pd.DataFrame(result, columns=['year', 'count_software'])
    data_frame = pd.merge(data_frame, df, on='year', how='left')

    SQL = f"""
        SELECT rr.year::smallint, COUNT(DISTINCT title) AS count_report
        FROM research_report rr
        WHERE rr.year::smallint >= %(year)s
            {FILTERS}
        GROUP BY rr.year
        ORDER BY rr.year;
    """
    result = await conn.select(SQL, params)
    df = pd.DataFrame(result, columns=['year', 'count_report'])
    data_frame = pd.merge(data_frame, df, on='year', how='left')

    SQL = f"""
        SELECT bpa.qualis, bp.year::smallint AS year,
            COUNT(DISTINCT title) AS count_article
        FROM public.bibliographic_production bp
        RIGHT JOIN bibliographic_production_article bpa
            ON bpa.bibliographic_production_id = bp.id
        WHERE type = 'ARTICLE'
            AND bp.year::smallint >= %(year)s
            {FILTERS}
        GROUP BY bpa.qualis, bp.year
        ORDER BY bpa.qualis, bp.year;
    """
    result = await conn.select(SQL, params)
    df = pd.DataFrame(result, columns=['qualis', 'year', 'count_article'])
    if not df.empty:
        df = df.pivot_table(
            index='year', columns='qualis', values='count_article', fill_value=0
        )
        df['count_article'] = df.sum(axis=1)
        df.reset_index(inplace=True)
        for q in ['A1', 'A2', 'A3', 'A4', 'B1', 'B2', 'B3', 'B4', 'C', 'SQ']:
            if q not in df.columns:
                df[q] = 0
        data_frame = pd.merge(data_frame, df, on='year', how='left')

    SQL = f"""
        SELECT br.year::smallint, COUNT(DISTINCT br.title) AS count_brand
        FROM brand br
        WHERE br.year::smallint >= %(year)s
            {FILTERS}
        GROUP BY br.year
        ORDER BY br.year;
    """
    result = await conn.select(SQL, params)
    df = pd.DataFrame(result, columns=['year', 'count_brand'])
    data_frame = pd.merge(data_frame, df, on='year', how='left')

    return data_frame.to_dict(orient='records')
