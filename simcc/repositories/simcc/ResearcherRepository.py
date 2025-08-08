from uuid import UUID

from unidecode import unidecode

from simcc.core.connection import Connection
from simcc.repositories import conn, tools
from simcc.schemas import DefaultFilters
from simcc.schemas.Researcher import ResearcherArticleProduction


async def search_in_area_specialty(
    conn: Connection,
    default_filters: DefaultFilters,
):
    params = {}
    filter_distinct = str()
    query_filters = str()
    filter_pagination = str()

    join_program = str()
    join_institution = str()
    join_departament = str()
    join_modality = str()

    if default_filters.graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = default_filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
        """
        query_filters += """
            AND gp.name = ANY(%(graduate_program)s)
            AND gpr.type_ = 'PERMANENTE'
        """

    if (
        default_filters.graduate_program_id
        and str(default_filters.graduate_program_id) != '0'
    ):
        params['graduate_program_id'] = str(default_filters.graduate_program_id)
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
        """
        query_filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
        """

    if default_filters.institution:
        params['institution'] = default_filters.institution.split(';')
        join_institution = """
            LEFT JOIN institution i ON i.id = r.institution_id
        """
        query_filters += """
            AND i.name = ANY(%(institution)s)
        """
    else:
        join_institution = """
            LEFT JOIN institution i ON i.id = r.institution_id
        """

    if default_filters.dep_id or default_filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
        """

    if default_filters.dep_id:
        filter_distinct = 'DISTINCT'
        params['dep_id'] = default_filters.dep_id.split(';')
        query_filters += """
            AND dp.dep_id = ANY(%(dep_id)s)
        """

    if default_filters.departament:
        filter_distinct = 'DISTINCT'
        params['dep'] = default_filters.departament.split(';')
        query_filters += """
            AND dp.dep_nom = ANY(%(dep)s)
        """

    if default_filters.graduation:
        params['graduation'] = default_filters.graduation.split(';')
        query_filters += """
            AND r.graduation = ANY(%(graduation)s)
        """

    if default_filters.term:
        filter_terms_str, term_params = tools.websearch_filter(
            'rp.area_specialty', default_filters.term
        )
        query_filters += filter_terms_str
        params.update(term_params)

    if default_filters.page and default_filters.lenght:
        filter_pagination = tools.pagination(
            default_filters.page, default_filters.lenght
        )

    if default_filters.modality:
        filter_distinct = 'DISTINCT'
        params['modality'] = default_filters.modality.split(';')
        join_modality = """
            INNER JOIN foment f
                ON f.researcher_id = r.id
        """
        query_filters += """
            AND f.modality_name = ANY(%(modality)s)
        """

    if default_filters.city:
        params['city'] = default_filters.city.split(';')
        query_filters += """
            AND rp.city = ANY(%(city)s)
        """

    if default_filters.area:
        params['area'] = default_filters.area.replace(' ', '_').split(';')
        query_filters += """
            AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s
        """

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract, r.orcid,
            r.graduation, r.last_update AS lattes_update,
            REPLACE(rp.great_area, '_', ' ') AS area, rp.city,
            i.image AS image_university, i.name AS university,
            1 AS among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex, r.classification, r.status, r.institution_id,
            r.abstract_ai
        FROM researcher r
            {join_institution}
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
            {join_modality}
            {join_program}
            {join_departament}
        WHERE 1 = 1
            {query_filters}
        ORDER BY
            among DESC
            {filter_pagination};
    """
    result = await conn.select(SCRIPT_SQL, params)
    return result


def search_in_participation_event(
    type: str = None,
    term: str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    dep: str = None,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
    page: int = None,
    lenght: int = None,
):
    params = {}
    filter_terms = str()
    filter_graduation = str()
    filter_pagination = str()
    join_program = str()
    filter_program = str()
    filter_distinct = str()

    if graduate_program:
        filter_distinct = 'DISTINCT'
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

    join_dep = str()
    filter_dep = str()
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

    if dep:
        filter_distinct = 'DISTINCT'
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

    filter_institution = str()
    if institution:
        params['institution'] = institution.split(';')
        filter_institution = """
            AND i.name = ANY(%(institution)s)
            """

    if graduation:
        params['graduation'] = graduation.split(';')
        filter_graduation = 'AND r.graduation = ANY(%(graduation)s)'

    if term:
        filter_terms, term = tools.websearch_filter('pe.event_name', term)
        params |= term

    if page and lenght:
        filter_pagination = tools.pagination(page, lenght)

    join_modality = str()
    filter_modality = str()
    if modality:
        params['modality'] = modality.split(';')
        join_modality = """
            INNER JOIN foment f
                ON f.researcher_id = r.id
        """
        filter_modality = 'AND modality_name = ANY(%(modality)s)'

    if graduate_program_id and graduate_program_id != '0':
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
        """
        filter_program = 'AND gpr.graduate_program_id = %(graduate_program_id)s'

    filter_city = str()
    if city:
        params['city'] = city.split(';')
        filter_city = 'AND rp.city = ANY(%(city)s)'

    filter_area = str()
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        filter_area = """
            AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s
        """

    filter_type = str()
    if type:
        params['type'] = type.split(';')
        filter_type = 'AND bp.type = ANY(%(type)s)'

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract, r.orcid,
            r.graduation, r.last_update AS lattes_update,
            REPLACE(rp.great_area, '_', ' ') AS area, rp.city,
            i.image AS image_university, i.name AS university,
            pe.among AS among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex, r.classification, r.status, r.institution_id,
            r.abstract_ai
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
            {join_modality}
            {join_program}
            {join_dep}
            INNER JOIN (
                SELECT pe.researcher_id, COUNT(*) AS among
                FROM participation_events pe
                WHERE 1 = 1
                    AND type_participation in ('Apresentação Oral', 'Conferencista','Moderador','Simposista')
                    {filter_terms}
                    {filter_type}
                GROUP BY researcher_id
            ) pe ON pe.researcher_id = r.id
        WHERE 1 = 1
            {filter_graduation}
            {filter_modality}
            {filter_program}
            {filter_institution}
            {filter_dep}
            {filter_city}
            {filter_area}
        ORDER BY
            among DESC
            {filter_pagination};
    """
    result = conn.select(SCRIPT_SQL, params)
    return result


def search_in_book(
    type,
    term,
    graduate_program_id,
    dep_id,
    dep,
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
    filter_terms = str()
    filter_graduation = str()
    filter_pagination = str()
    join_dep = str()
    filter_dep = str()
    filter_institution = str()
    filter_distinct = str()

    if institution:
        params['institution'] = institution.split(';')
        filter_institution = """
            AND i.name = ANY(%(institution)s)
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

    if dep:
        filter_distinct = 'DISTINCT'
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
    if graduation:
        params['graduation'] = graduation.split(';')
        filter_graduation = 'AND r.graduation = ANY(%(graduation)s)'

    if term:
        filter_terms, term = tools.websearch_filter('bp.title', term)
        params |= term

    if page and lenght:
        filter_pagination = tools.pagination(page, lenght)

    join_modality = str()
    filter_modality = str()
    if modality:
        params['modality'] = modality.split(';')
        join_modality = """
            INNER JOIN foment f
                ON f.researcher_id = r.id
        """
        filter_modality = 'AND modality_name = ANY(%(modality)s)'

    join_program = str()
    filter_program = str()
    if graduate_program_id and graduate_program_id != '0':
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
        """
        filter_program = 'AND gpr.graduate_program_id = %(graduate_program_id)s'

    if graduate_program:
        filter_distinct = 'DISTINCT'
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

    filter_city = str()
    if city:
        params['city'] = city.split(';')
        filter_city = 'AND rp.city = ANY(%(city)s)'

    filter_area = str()
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        filter_area = """
            AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s
        """

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract, r.orcid,
            r.graduation, r.last_update AS lattes_update,
            REPLACE(rp.great_area, '_', ' ') AS area, rp.city,
            i.image AS image_university, i.name AS university,
            1 AS among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex, r.classification, r.status, r.institution_id,
            r.abstract_ai
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
            {join_modality}
            {join_program}
            {join_dep}
            INNER JOIN (
                SELECT bp.researcher_id, COUNT(*) AS among
                FROM bibliographic_production bp
                WHERE 1 = 1
                    AND TYPE = 'BOOK'
                    {filter_terms}
                    {filter_institution}
                GROUP BY researcher_id
            ) bp ON bp.researcher_id = r.id
        WHERE 1 = 1
            {filter_graduation}
            {filter_dep}
            {filter_modality}
            {filter_program}
            {filter_city}
            {filter_area}
        ORDER BY
            among DESC
            {filter_pagination};
    """
    result = conn.select(SCRIPT_SQL, params)
    return result


async def get_area_filter(conn):
    sql = """
        SELECT ARRAY_AGG(DISTINCT REPLACE(gae.name, '_', ' ')) as area
        FROM great_area_expertise gae
            INNER JOIN researcher_area_expertise r
                ON gae.id = r.great_area_expertise_id;
    """
    result = await conn.select(sql, one=True)
    return result.get('area')


async def get_graduation_filter(conn):
    sql = """
        SELECT ARRAY_AGG(DISTINCT graduation) AS graduation
        FROM researcher;
    """
    result = await conn.select(sql, one=True)
    return result.get('graduation')


async def get_city_filter(conn):
    sql = """
        SELECT ARRAY_AGG(DISTINCT bp.city) AS city
        FROM researcher_production bp
        WHERE bp.city IS NOT NULL;
    """
    result = await conn.select(sql, one=True)
    return result.get('city')


async def get_institution_filter(conn):
    sql = """
        SELECT ARRAY_AGG(DISTINCT institution.name) AS institution
        FROM institution
        INNER JOIN researcher
            ON researcher.institution_id = institution.id;
    """
    result = await conn.select(sql, one=True)
    return result.get('institution')


async def get_modality_filter(conn):
    sql = """
        SELECT ARRAY_AGG(DISTINCT modality_name) AS modality
        FROM foment;
    """
    result = await conn.select(sql, one=True)
    return result.get('modality')


async def get_graduate_program_filter(conn):
    sql = """
        SELECT ARRAY_AGG(DISTINCT graduate_program.name) AS graduate_program
        FROM graduate_program
        INNER JOIN graduate_program_researcher
            ON graduate_program_researcher.graduate_program_id =
               graduate_program.graduate_program_id;
    """
    result = await conn.select(sql, one=True)
    return result.get('graduate_program')


async def get_departament_filter(conn):
    sql = """
        SELECT ARRAY_AGG(DISTINCT dep_nom) AS departament FROM ufmg.departament;
    """
    result = await conn.select(sql, one=True)
    return result.get('departament')


async def get_researcher_filter(conn):
    return {
        'area': await get_area_filter(conn),
        'graduation': await get_graduation_filter(conn),
        'city': await get_city_filter(conn),
        'institution': await get_institution_filter(conn),
        'modality': await get_modality_filter(conn),
        'graduate_program': await get_graduate_program_filter(conn),
        'departament': await get_departament_filter(conn),
    }


def list_article_production(
    program_id: UUID, dep_id: str, year: int
) -> list[ResearcherArticleProduction]:
    params = {}
    filters = str()

    join_program = str()
    join_departament = str()

    if dep_id:
        params['dep_id'] = dep_id
        filters += """
            AND dpr.dep_id = %(dep_id)s
            """
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
            ON dpr.researcher_id = r.id
            """

    if program_id:
        params['program_id'] = program_id
        filters += """
            AND gpr.graduate_program_id = %(program_id)s
            AND gpr.type_ = 'PERMANENTE'
            """
        join_program = """
            INNER JOIN graduate_program_researcher gpr
            ON gpr.researcher_id = r.id
            """

    if year:
        params['year'] = year
        filters += 'AND bp.year::int >= %(year)s'

    SCRIPT_SQL = f"""
        SELECT r.name, bpa.qualis, COUNT(*) AS among, bp.year,
            COALESCE(SUM(opa.citations_count), 0) AS citations
        FROM researcher r
            LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id
            RIGHT JOIN bibliographic_production_article bpa
                ON bpa.bibliographic_production_id = bp.id
            LEFT JOIN openalex_article opa ON opa.article_id = bp.id
            {join_program}
            {join_departament}
        WHERE 1 = 1
            {filters}
        GROUP BY r.id, bpa.qualis, bp.year;
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


async def search_in_articles(conn: Connection, default_filters: DefaultFilters):
    params = {}
    filters = str()
    join_dep = str()
    join_program = str()
    join_modality = str()
    filter_distinct = str()
    filter_pagination = str()

    if default_filters.graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = default_filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
        """
        filters += """
            AND gp.name = ANY(%(graduate_program)s)
            AND gpr.type_ = 'PERMANENTE'
        """

    if (
        default_filters.graduate_program_id
        and default_filters.graduate_program_id != '0'
    ):
        params['graduate_program_id'] = default_filters.graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
        """
        filters += 'AND gpr.graduate_program_id = %(graduate_program_id)s\n'

    if default_filters.dep_id:
        filter_distinct = 'DISTINCT'
        params['dep_id'] = default_filters.dep_id.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
        """
        filters += 'AND dp.dep_id = ANY(%(dep_id)s)\n'

    if default_filters.departament:
        filter_distinct = 'DISTINCT'
        params['dep'] = default_filters.departament.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
        """
        filters += 'AND dp.dep_nom = ANY(%(dep)s)\n'

    if default_filters.institution:
        params['institution'] = default_filters.institution.split(';')
        filters += 'AND i.name = ANY(%(institution)s)\n'

    if default_filters.city:
        params['city'] = default_filters.city.split(';')
        filters += 'AND rp.city = ANY(%(city)s)\n'

    if default_filters.area:
        params['area'] = default_filters.area.replace(' ', '_').split(';')
        filters += """
            AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s
        """

    if default_filters.modality:
        params['modality'] = default_filters.modality.split(';')
        join_modality = """
            INNER JOIN foment f
                ON f.researcher_id = r.id
        """
        filters += 'AND f.modality_name = ANY(%(modality)s)\n'

    if default_filters.graduation:
        params['graduation'] = default_filters.graduation.split(';')
        filters += 'AND r.graduation = ANY(%(graduation)s)\n'

    if default_filters.page and default_filters.lenght:
        filter_pagination = tools.pagination(
            default_filters.page, default_filters.lenght
        )

    filter_terms = str()
    if default_filters.term:
        filter_terms, terms_params = tools.websearch_filter(
            'bp.title', default_filters.term
        )
        params.update(terms_params)

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract, r.orcid,
            r.graduation, r.last_update AS lattes_update,
            REPLACE(rp.great_area, '_', ' ') AS area, rp.city,
            i.image AS image_university, i.name AS university,
            bp.among AS among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex, r.classification, r.status, r.institution_id,
            r.abstract_ai
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
            {join_modality}
            INNER JOIN (
                SELECT bp.researcher_id, COUNT(*) AS among
                FROM bibliographic_production bp
                WHERE 1 = 1
                    AND type = 'ARTICLE'
                    {filter_terms}
                GROUP BY researcher_id
            ) bp ON bp.researcher_id = r.id
            {join_program}
            {join_dep}
        WHERE 1 = 1
            {filters}
        ORDER BY
            among DESC
            {filter_pagination};
    """
    result = await conn.select(SCRIPT_SQL, params)
    return result


async def search_in_abstracts(conn: Connection, default_filters: DefaultFilters):
    params = {}
    filters = str()
    join_dep = str()
    join_program = str()
    join_modality = str()
    filter_distinct = str()
    filter_pagination = str()

    if default_filters.departament:
        filter_distinct = 'DISTINCT'
        params['dep'] = default_filters.departament.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
        """
        filters += 'AND dp.dep_nom = ANY(%(dep)s)\n'

    if default_filters.dep_id:
        filter_distinct = 'DISTINCT'
        params['dep_id'] = default_filters.dep_id.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
        """
        filters += 'AND dp.dep_id = ANY(%(dep_id)s)\n'

    if default_filters.graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = default_filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
        """
        filters += """
            AND gp.name = ANY(%(graduate_program)s)
            AND gpr.type_ = 'PERMANENTE'
        """

    if (
        default_filters.graduate_program_id
        and default_filters.graduate_program_id != '0'
    ):
        params['graduate_program_id'] = default_filters.graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
        """
        filters += 'AND gpr.graduate_program_id = %(graduate_program_id)s\n'

    if default_filters.institution:
        params['institution'] = default_filters.institution.split(';')
        filters += 'AND i.name = ANY(%(institution)s)\n'

    if default_filters.page and default_filters.lenght:
        filter_pagination = tools.pagination(
            default_filters.page, default_filters.lenght
        )

    if default_filters.term:
        term_filter, term_params = tools.websearch_filter(
            'r.abstract', default_filters.term
        )
        filters += term_filter
        params.update(term_params)

    if default_filters.city:
        params['city'] = default_filters.city.split(';')
        filters += 'AND rp.city = ANY(%(city)s)\n'

    if default_filters.area:
        params['area'] = default_filters.area.replace(' ', '_').split(';')
        filters += """
            AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s)
        """

    if default_filters.modality:
        params['modality'] = default_filters.modality.split(';')
        join_modality = """
            INNER JOIN foment f
                ON f.researcher_id = r.id
        """
        filters += 'AND f.modality_name = ANY(%(modality)s)\n'

    if default_filters.graduation:
        params['graduation'] = default_filters.graduation.split(';')
        filters += 'AND r.graduation = ANY(%(graduation)s)\n'

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract, r.orcid,
            r.graduation, r.last_update AS lattes_update,
            REPLACE(rp.great_area, '_', ' ') AS area, rp.city,
            i.image AS image_university, i.name AS university,
            1 AS among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex, r.classification, r.status, r.institution_id,
            r.abstract_ai
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
            {join_modality}
            {join_program}
            {join_dep}
        WHERE 1 = 1
            {filters}
        ORDER BY
            among DESC
        {filter_pagination};
    """
    result = await conn.select(SCRIPT_SQL, params)
    return result


async def search_in_name(
    conn: Connection, default_filters: DefaultFilters, name
):
    params = {}
    filters = ''
    join_dep = ''
    join_program = ''
    join_modality = ''
    filter_distinct = ''
    filter_pagination = ''

    if default_filters.dep_id:
        filter_distinct = 'DISTINCT'
        params['dep_id'] = default_filters.dep_id.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
        """
        filters += 'AND dp.dep_id = ANY(%(dep_id)s)\n'

    if default_filters.departament:
        filter_distinct = 'DISTINCT'
        params['dep'] = default_filters.departament.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
        """
        filters += 'AND dp.dep_nom = ANY(%(dep)s)\n'

    if default_filters.graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = default_filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
        """
        filters += 'AND gp.name = ANY(%(graduate_program)s)\n'

    if default_filters.graduate_program_id:
        params['graduate_program_id'] = default_filters.graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            """
        filters += 'AND gpr.graduate_program_id = %(graduate_program_id)s\n'

    if default_filters.institution:
        params['institution'] = default_filters.institution.split(';')
        filters += 'AND i.name = ANY(%(institution)s)\n'

    if default_filters.city:
        params['city'] = default_filters.city.split(';')
        filters += 'AND rp.city = ANY(%(city)s)\n'

    if default_filters.area:
        params['area'] = default_filters.area.replace(' ', '_').split(';')
        filters += """
            AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s
        """

    if default_filters.modality:
        params['modality'] = default_filters.modality.split(';')
        join_modality = """
            INNER JOIN foment f
                ON f.researcher_id = r.id
        """
        filters += 'AND f.modality_name = ANY(%(modality)s)\n'

    if default_filters.graduation:
        params['graduation'] = default_filters.graduation.split(';')
        filters += 'AND r.graduation = ANY(%(graduation)s)\n'

    if default_filters.page and default_filters.lenght:
        filter_pagination = tools.pagination(
            default_filters.page, default_filters.lenght
        )

    if name:
        name_filter, name_params = tools.names_filter('r.name', name)
        filters += name_filter
        params.update(name_params)

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            r.id,
            r.name,
            r.lattes_id,
            r.lattes_10_id,
            r.abstract,
            r.orcid,
            r.graduation,
            r.last_update AS lattes_update,
            REPLACE(rp.great_area, '_', ' ') AS area,
            rp.city,
            i.image AS image_university,
            i.name  AS university,
            1       AS among,
            rp.articles,
            rp.book_chapters,
            rp.book,
            rp.patent,
            rp.software,
            rp.brand,
            opr.h_index,
            opr.relevance_score,
            opr.works_count,
            opr.cited_by_count,
            opr.i10_index,
            opr.scopus,
            opr.openalex,
            r.classification,
            r.status,
            r.institution_id,
            r.abstract_ai
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
            {join_program}
            {join_modality}
            {join_dep}
        WHERE 1=1
            {filters}
        ORDER BY among DESC
        {filter_pagination};
    """
    return await conn.select(SCRIPT_SQL, params)


def list_outstanding_researchers(
    name: str,
    graduate_program_id: UUID,
    dep_id: UUID,
    page: int = None,
    lenght: int = None,
):
    params = {}

    join_departament = str()
    filter_departament = str()
    if dep_id:
        params['dep_id'] = dep_id
        join_departament = """
            LEFT JOIN ufmg.departament_researcher dr ON dr.researcher_id = r.id
            """
        filter_departament = 'AND dr.dep_id = %(dep_id)s'

    filter_name = str()
    if name:
        filter_name, terms = tools.names_filter('r.name', name)
        params |= terms

    filter_pagination = str()
    if page and lenght:
        filter_pagination = tools.pagination(page, lenght)

    join_program = str()
    filter_program = str()
    if graduate_program_id:
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            """
        filter_program = 'AND gpr.graduate_program_id = %(graduate_program_id)s'

    SCRIPT_SQL = f"""
        WITH production AS (
            SELECT researcher_id, COUNT(*) AS among
            FROM bibliographic_production
            WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY researcher_id

            UNION ALL

            SELECT researcher_id, COUNT(*) AS among
            FROM patent
            WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY researcher_id

            UNION ALL

            SELECT researcher_id, COUNT(*) AS among
            FROM software
            WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY researcher_id

            UNION ALL

            SELECT researcher_id, COUNT(*) AS among
            FROM brand
            WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY researcher_id),
        outstanding_researchers AS (
            SELECT researcher_id, SUM(among) AS among
            FROM production
            GROUP BY researcher_id
            ORDER BY among DESC
            LIMIT 10)
        SELECT
            r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract, r.orcid,
            r.graduation, r.last_update AS lattes_update,
            REPLACE(rp.great_area, '_', ' ') AS area, rp.city,
            i.image AS image_university, i.name AS university,
            ors.among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex, r.classification, r.status, r.institution_id,
            r.abstract_ai
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
            RIGHT JOIN outstanding_researchers ors ON r.id = ors.researcher_id
            {join_program}
            {join_departament}
        WHERE 1 = 1
            {filter_program}
            {filter_name}
            {filter_departament}
        ORDER BY
            among DESC
            {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_graduate_programs():
    SCRIPT_SQL = """
        SELECT gpr.researcher_id AS id,
            JSONB_AGG(JSONB_BUILD_OBJECT(
                        'graduate_program_id', gp.graduate_program_id,
                        'code', gp.code,
                        'name', gp.name,
                        'name_en', gp.name_en,
                        'basic_area', gp.basic_area,
                        'cooperation_project', gp.cooperation_project,
                        'area', gp.area,
                        'modality', gp.modality,
                        'type', gp.type,
                        'rating', gp.rating,
                        'institution_id', gp.institution_id,
                        'state', gp.state,
                        'city', gp.city,
                        'region', gp.region,
                        'url_image', gp.url_image,
                        'acronym', gp.acronym,
                        'description', gp.description,
                        'visible', gp.visible,
                        'site', gp.site,
                        'coordinator', gp.coordinator,
                        'email', gp.email,
                        'start', gp.start,
                        'phone', gp.phone,
                        'periodicity', gp.periodicity,
                        'created_at', gp.created_at,
                        'updated_at', gp.updated_at
                    )) AS graduate_programs
        FROM graduate_program_researcher gpr
        LEFT JOIN graduate_program gp
        ON gpr.graduate_program_id = gp.graduate_program_id
        GROUP BY gpr.researcher_id
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_research_groups():
    SCRIPT_SQL = """
        SELECT r.id AS id,
            JSONB_AGG(JSONB_BUILD_OBJECT(
                'group_id', rg.id,
                'name', rg.name,
                'area', rg.area,
                'census',rg.census,
                'start_of_collection', rg.start_of_collection,
                'end_of_collection', rg.end_of_collection,
                'group_identifier', rg.group_identifier,
                'year', rg.year,
                'institution_name', rg.institution_name,
                'category', rg.category
                )) AS research_groups
        FROM researcher r
        INNER JOIN research_group rg
            ON rg.second_leader_id = r.id OR rg.first_leader_id = r.id
        GROUP BY r.id
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_foment_data():
    SCRIPT_SQL = """
        SELECT s.researcher_id AS id,
            JSONB_AGG(JSONB_BUILD_OBJECT(
                'id', s.id,
                'modality_code', s.modality_code,
                'modality_name', s.modality_name,
                'call_title', s.call_title,
                'category_level_code', s.category_level_code,
                'funding_program_name', s.funding_program_name,
                'institute_name', s.institute_name,
                'aid_quantity', s.aid_quantity,
                'scholarship_quantity', s.scholarship_quantity
            )) AS subsidy
        FROM foment s
        GROUP BY s.researcher_id
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_departament_data():
    SCRIPT_SQL = """
        SELECT dpr.researcher_id AS id,
            JSONB_AGG(JSONB_BUILD_OBJECT(
                'dep_id', dp.dep_id,
                'org_cod', dp.org_cod,
                'dep_nom', dp.dep_nom,
                'dep_des', dp.dep_des,
                'dep_email', dp.dep_email,
                'dep_site', dp.dep_site,
                'dep_sigla', dp.dep_sigla,
                'dep_tel', dp.dep_tel
            )) AS departments
        FROM ufmg.departament_researcher dpr
            LEFT JOIN ufmg.departament dp ON dpr.dep_id = dp.dep_id
        GROUP BY dpr.researcher_id;
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_user_data():
    SCRIPT_SQL = """
        SELECT u.lattes_id,
            JSONB_BUILD_OBJECT(
                'lattes_id', u.lattes_id,
                'uid', u.uid,
                'email', u.email,
                'gender', u.gender,
                'verify', u.verify,
                'shib_id', u.shib_id,
                'user_id', u.user_id,
                'linkedin', u.linkedin,
                'provider', u.provider,
                'last_name', u.last_name,
                'photo_url', u.photo_url,
                'shib_code', u.shib_code,
                'birth_date', u.birth_date,
                'created_at', u.created_at,
                'first_name', u.first_name,
                'updated_at', u.updated_at,
                'course_level', u.course_level,
                'display_name', u.display_name,
                'email_status', u.email_status,
                'registration', u.registration,
                'institution_id', u.institution_id,
                'visible_email', u.visible_email
            ) AS user
        FROM admin.users u
        WHERE u.lattes_id IS NOT NULL;
    """
    result = conn.select(SCRIPT_SQL)
    return result


def list_ufmg_data():
    SCRIPT_SQL = """
        SELECT researcher_id AS id, full_name, gender, status_code,
            work_regime, job_class, job_title, job_rank,
            job_reference_code, academic_degree, organization_entry_date,
            last_promotion_date, employment_status_description, department_name,
            career_category, academic_unit, unit_code, function_code,
            position_code, leadership_start_date, leadership_end_date,
            current_function_name, function_location, registration_number,
            ufmg_registration_number, semester_reference
        FROM ufmg.researcher;
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_co_authorship(researcher_id: UUID):
    params = {'researcher_id': researcher_id}
    SCRIPT_SQL = """
        WITH co_authorship AS (
            SELECT r.name, i.name AS institution, COUNT(*) AS among
            FROM researcher r
            RIGHT JOIN (
                SELECT UNNEST(ARRAY_AGG(bp.researcher_id)) AS researcher_id
                FROM bibliographic_production bp
                GROUP BY bp.title
                HAVING COUNT(bp.title) > 1
                AND %(researcher_id)s = ANY(ARRAY_AGG(bp.researcher_id))
            ) co_authorship ON r.id = co_authorship.researcher_id
            LEFT JOIN institution i ON i.id = r.institution_id
            WHERE r.id != %(researcher_id)s
            GROUP BY r.name, i.name

            UNION

            SELECT TRIM(UNNEST(string_to_array(opa.authors, ';'))) AS name,
                TRIM(UNNEST(string_to_array(opa.authors_institution, ';')))
                AS institution, COUNT(*) AS among
            FROM openalex_article opa
            INNER JOIN bibliographic_production bp
                ON opa.article_id = bp.id
            WHERE bp.researcher_id = %(researcher_id)s
            GROUP BY name, institution
        )
        SELECT ca.name, ca.institution, ca.among
        FROM co_authorship ca
        JOIN researcher r ON r.id = %(researcher_id)s
        WHERE similarity(ca.name, r.name) < 0.2;
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def list_openalex_co_authorship(researcher_id: UUID):
    params = {'researcher_id': researcher_id}

    SCRIPT_SQL = """
        SELECT TRIM(unnest(string_to_array(opa.authors, ';'))) AS name,
            TRIM(unnest(string_to_array(opa.authors_institution, ';')))
            AS institution, COUNT(*) AS among
        FROM openalex_article opa
        INNER JOIN bibliographic_production bp
            ON opa.article_id = bp.id
        WHERE bp.researcher_id = %(researcher_id)s
        GROUP BY name, institution;
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def get_institution_id(researcher_id: UUID):
    one = True
    params = {'researcher_id': researcher_id}
    SCRIPT_SQL = """
        SELECT institution_id
        FROM researcher
        WHERE id = %(researcher_id)s
        """
    result = conn.select(SCRIPT_SQL, params, one)
    return result


def get_id(name: str = None):
    one = True
    params = {'name': unidecode(name).lower()}

    SCRIPT_SQL = """
        SELECT id
        FROM researcher
        WHERE UNACCENT(name) ILIKE %(name)s;
        """

    result = conn.select(SCRIPT_SQL, params, one)
    return result


def get_institutions(institutions: list[str] = None):
    one = True
    institutions = [unidecode(i).strip().lower() for i in institutions]
    params = {'names': institutions}
    SCRIPT_SQL = """
        SELECT id
        FROM institution
        WHERE UNACCENT(name) = ANY(%(names)s);
        """
    result = conn.select(SCRIPT_SQL, params, one)
    return result


def search_in_patents(
    term,
    graduate_program_id,
    dep_id,
    dep,
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
    join_dep = str()
    filter_dep = str()
    join_program = str()
    filter_program = str()

    if graduate_program:
        filter_distinct = 'DISTINCT'
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

    filter_city = str()
    if city:
        params['city'] = city.split(';')
        filter_city = 'AND rp.city = ANY(%(city)s)'
    filter_area = str()
    if area:
        params['area'] = area.replace(' ', '_').split(';')
        filter_area = """
            AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s
        """

    filter_institution = str()

    join_modality = str()
    filter_modality = str()
    if modality:
        params['modality'] = modality.split(';')
        join_modality = """
            INNER JOIN foment f
                ON f.researcher_id = r.id
        """
        filter_modality = 'AND modality_name = ANY(%(modality)s)'

    if institution:
        params['institution'] = institution.split(';')
        filter_institution = """
            AND i.name = ANY(%(institution)s)
            """

    filter_distinct = str()

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

    if dep:
        filter_distinct = 'DISTINCT'
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

    filter_graduation = str()
    if graduation:
        params['graduation'] = graduation.split(';')
        filter_graduation = 'AND r.graduation = ANY(%(graduation)s)'

    filter_pagination = str()
    if page and lenght:
        filter_pagination = tools.pagination(page, lenght)

    filter_terms = str()

    if term:
        filter_terms, terms = tools.websearch_filter('p.title', term)
        params |= terms

    join_program = str()
    filter_program = str()
    if graduate_program_id and graduate_program_id != '0':
        params['graduate_program_id'] = graduate_program_id
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            """
        filter_program = 'AND gpr.graduate_program_id = %(graduate_program_id)s'

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract, r.orcid,
            r.graduation, r.last_update AS lattes_update,
            REPLACE(rp.great_area, '_', ' ') AS area, rp.city,
            i.image AS image_university, i.name AS university,
            p.among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex, r.classification, r.status, r.institution_id,
            r.abstract_ai
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
			INNER JOIN
				(SELECT p.researcher_id, COUNT(*) AS among
				FROM patent p
				WHERE 1 = 1
					{filter_terms}
				GROUP BY researcher_id) p ON p.researcher_id = r.id
            {join_program}
            {join_dep}
            {join_modality}
            {join_program}
        WHERE 1 = 1
            {filter_dep}
            {filter_program}
            {filter_modality}
            {filter_program}
            {filter_area}
            {filter_institution}
            {filter_graduation}
            {filter_city}
        ORDER BY
            among DESC
            {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


async def list_foment_researchers(default_filters, conn):
    params = {}

    query_filters = str()
    join_departament = str()
    join_program = str()

    if default_filters.term:
        filter_terms, term = tools.websearch_filter(
            'r.name', default_filters.term
        )
        query_filters += filter_terms
        params.update(term)

    if default_filters.researcher_id:
        params['researcher_id'] = str(default_filters.researcher_id)
        query_filters += """
            AND r.id = %(researcher_id)s
            """

    if default_filters.dep_id or default_filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if default_filters.dep_id:
        params['dep_id'] = default_filters.dep_id
        query_filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if default_filters.departament:
        params['departament'] = default_filters.departament.split(';')
        query_filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if default_filters.institution:
        params['institution'] = default_filters.institution.split(';')
        query_filters += """
            AND i.name = ANY(%(institution)s)
            """

    if default_filters.graduate_program_id or default_filters.graduate_program:
        join_program = """
            INNER JOIN public.graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            INNER JOIN public.graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
    if default_filters.graduate_program_id:
        params['graduate_program_id'] = str(default_filters.graduate_program_id)
        query_filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if default_filters.graduate_program:
        params['graduate_program'] = default_filters.graduate_program.split(';')
        query_filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if default_filters.city:
        params['city'] = default_filters.city.split(';')
        query_filters += ' AND rp.city = ANY(%(city)s)'

    if default_filters.area:
        params['area'] = default_filters.area.replace(' ', '_').split(';')
        query_filters += ' AND rp.great_area && %(area)s'

    if default_filters.modality:
        params['modality'] = default_filters.modality.split(';')
        query_filters += ' AND f.modality_name = ANY(%(modality)s)'

    if default_filters.graduation:
        params['graduation'] = default_filters.graduation.split(';')
        query_filters += ' AND r.graduation = ANY(%(graduation)s)'

    SCRIPT_SQL = f"""
        SELECT
            r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract, r.orcid,
            r.graduation, r.last_update AS lattes_update,
            REPLACE(rp.great_area, '_', ' ') AS area, rp.city,
            i.image AS image_university, i.name AS university,
            1 AS among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex, r.classification, r.status, r.institution_id,
            r.abstract_ai
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
            INNER JOIN foment f ON f.researcher_id = r.id
            {join_departament}
            {join_program}
        WHERE 1 = 1
            {query_filters}
        ORDER BY
            among DESC;
        """

    result = await conn.select(SCRIPT_SQL, params)
    return result


def get_researcher(researcher_id: UUID) -> dict:
    one = True
    params = {'researcher_id': researcher_id}
    SCRIPT_SQL = """
        SELECT * FROM researcher r WHERE r.id = %(researcher_id)s;
        """
    result = conn.select(SCRIPT_SQL, params, one)
    return result


async def academic_degree(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}

    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    join_type_specific = str()
    type_specific_filters = str()

    general_filters = str()

    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        general_filters += ' AND r.id = %(researcher_id)s'

    if filters.dep_id or filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
            """
    if filters.dep_id:
        params['dep_id'] = filters.dep_id.split(';')
        general_filters += ' AND dp.dep_id = ANY(%(dep_id)s)'

    if filters.departament:
        params['departament'] = filters.departament.split(';')
        general_filters += ' AND dp.dep_nom = ANY(%(departament)s)'

    if filters.institution:
        params['institution'] = filters.institution.split(';')
        join_institution = """
            INNER JOIN public.institution i ON r.institution_id = i.id
            """
        general_filters += ' AND i.name = ANY(%(institution)s)'

    if filters.graduate_program_id or filters.graduate_program:
        if not join_program:
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
        params['city'] = filters.city.split(';')
        join_researcher_production = ' LEFT JOIN public.researcher_production rp ON rp.researcher_id = r.id'
        general_filters += ' AND rp.city = ANY(%(city)s)'

    if filters.area:
        params['area'] = filters.area.replace(' ', '_').split(';')
        if not join_researcher_production:
            join_researcher_production = ' LEFT JOIN public.researcher_production rp ON rp.researcher_id = r.id'
        general_filters += " AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s"

    if filters.modality:
        params['modality'] = filters.modality.split(';')
        join_foment = ' INNER JOIN public.foment f ON f.researcher_id = r.id'
        if filters.modality != '*':
            general_filters += ' AND f.modality_name = ANY(%(modality)s)'

    if filters.graduation:
        params['graduation'] = filters.graduation.split(';')
        general_filters += ' AND r.graduation = ANY(%(graduation)s)'

    if filters.type:
        match filters.type:
            case 'ABSTRACT':
                if filters.term:
                    term_filter_str, term_params = tools.websearch_filter(
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
                join_type_specific = f"INNER JOIN bibliographic_production bp ON bp.researcher_id = r.id AND bp.type = '{filters.type}'"
                if filters.term:
                    term_filter_str, term_params_bp = tools.websearch_filter(
                        'bp.title', filters.term
                    )
                    type_specific_filters += term_filter_str
                    params.update(term_params_bp)
                if filters.year:
                    type_specific_filters += ' AND bp.year_ >= %(year)s'
                    params['year'] = filters.year

            case 'PATENT':
                join_type_specific = (
                    'INNER JOIN patent p ON p.researcher_id = r.id'
                )
                if filters.term:
                    term_filter_str, term_params_p = tools.websearch_filter(
                        'p.title', filters.term
                    )
                    type_specific_filters += term_filter_str
                    params.update(term_params_p)
                if filters.year:
                    type_specific_filters += """
                        AND p.development_year::INT >= %(year)s
                        """
                    params['year'] = filters.year

            case 'AREA':
                if not join_researcher_production:
                    join_researcher_production = 'INNER JOIN researcher_production rp ON rp.researcher_id = r.id'
                if filters.term:
                    term_filter_str, term_params_rp = tools.websearch_filter(
                        'rp.great_area', filters.term
                    )
                    type_specific_filters += term_filter_str
                    params.update(term_params_rp)

            case 'EVENT':
                join_type_specific = (
                    'INNER JOIN event_organization e ON e.researcher_id = r.id'
                )
                if filters.term:
                    term_filter_str, term_params_e = tools.websearch_filter(
                        'e.title', filters.term
                    )
                    type_specific_filters += term_filter_str
                    params.update(term_params_e)
                if filters.year:
                    type_specific_filters += ' AND e.year >= %(year)s'
                    params['year'] = filters.year

            case 'NAME':
                if filters.term:
                    name_filter_str, term_params_name = tools.names_filter(
                        'r.name', filters.term
                    )
                    type_specific_filters += name_filter_str
                    params.update(term_params_name)

    SCRIPT_SQL = f"""
        SELECT r.graduation, COUNT(DISTINCT r.id) AS among
        FROM public.researcher r
            {join_departament}
            {join_institution}
            {join_program}
            {join_researcher_production}
            {join_foment}
            {join_type_specific}
        WHERE r.graduation IS NOT NULL
            {general_filters}
            {type_specific_filters}
        GROUP BY r.graduation;
        """

    result = await conn.select(SCRIPT_SQL, params)
    return result


async def get_great_area(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}
    join_departament = str()
    join_institution = str()
    join_program = str()
    join_foment = str()
    general_filters = str()
    join_type_specific = str()
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
            general_filters += """
                AND gpr.graduate_program_id = %(graduate_program_id)s
                """
        if filters.graduate_program:
            params['graduate_program'] = filters.graduate_program.split(';')
            general_filters += ' AND gp.name = ANY(%(graduate_program)s)'

    if filters.city:
        params['city'] = filters.city.split(';')
        general_filters += ' AND rp.city = ANY(%(city)s)'

    if filters.area:
        params['area'] = filters.area.replace(' ', '_').split(';')
        general_filters += " AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s"

    if filters.modality:
        join_foment = ' INNER JOIN public.foment f ON f.researcher_id = r.id'
        params['modality'] = filters.modality.split(';')
        if filters.modality != '*':
            general_filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:
        params['graduation'] = filters.graduation.split(';')
        general_filters += ' AND r.graduation = ANY(%(graduation)s)'

    if filters.type:
        match filters.type:
            case (
                'BOOK'
                | 'BOOK_CHAPTER'
                | 'ARTICLE'
                | 'WORK_IN_EVENT'
                | 'TEXT_IN_NEWSPAPER_MAGAZINE'
            ):
                join_type_specific = 'INNER JOIN public.bibliographic_production bp ON bp.researcher_id = r.id'
                params['type'] = filters.type
                type_specific_filters += ' AND bp.type = %(type)s '
                if filters.term:
                    term_filter_str, term_params = tools.websearch_filter(
                        'bp.title', filters.term
                    )
                    type_specific_filters += term_filter_str
                    params.update(term_params)
                if filters.year:
                    params['year'] = filters.year
                    type_specific_filters += ' AND bp.year_ >= %(year)s'
            case 'PATENT':
                join_type_specific = (
                    'INNER JOIN public.patent p ON p.researcher_id = r.id'
                )
                if filters.term:
                    term_filter_str, term_params = tools.websearch_filter(
                        'p.title', filters.term
                    )
                    type_specific_filters += term_filter_str
                    params.update(term_params)
                if filters.year:
                    params['year'] = filters.year
                    type_specific_filters += (
                        ' AND p.development_year::INT >= %(year)s'
                    )
            case 'EVENT':
                join_type_specific = 'INNER JOIN public.event_organization e ON e.researcher_id = r.id'
                if filters.term:
                    term_filter_str, term_params = tools.websearch_filter(
                        'e.title', filters.term
                    )
                    type_specific_filters += term_filter_str
                    params.update(term_params)
                if filters.year:
                    params['year'] = filters.year
                    type_specific_filters += ' AND e.year >= %(year)s'

    SCRIPT_SQL = f"""
        WITH areas AS (
            SELECT DISTINCT ON (r.id, ga)
                r.id AS researcher_id,
                UNNEST(rp.great_area_) AS ga
            FROM
                public.researcher_production rp
            INNER JOIN public.researcher r ON r.id = rp.researcher_id
            {join_departament}
            {join_institution}
            {join_program}
            {join_foment}
            {join_type_specific}
            WHERE 1 = 1
                {general_filters}
                {type_specific_filters}
        )
        SELECT
            REPLACE(areas.ga, '_', ' ') AS great_area,
            COUNT(areas.researcher_id) AS count
        FROM
            areas
        WHERE
            areas.ga IS NOT NULL AND areas.ga <> ''
        GROUP BY
            great_area
        ORDER BY
            count DESC;
        """

    result = await conn.select(SCRIPT_SQL, params)
    return result
