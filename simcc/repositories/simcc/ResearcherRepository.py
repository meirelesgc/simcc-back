from uuid import UUID

from unidecode import unidecode

from simcc.repositories import conn
from simcc.repositories.util import names_filter, pagination, webseatch_filter
from simcc.schemas.Researcher import ResearcherArticleProduction


def search_in_area_specialty(
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
    filter_distinct = str()

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

    filter_institution = str()
    if institution:
        params['institution'] = institution.split(';')
        filter_institution = """
            AND i.name = ANY(%(institution)s)
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

    if graduation:
        params['graduation'] = graduation.split(';')
        filter_graduation = 'AND r.graduation = ANY(%(graduation)s)'

    if term:
        filter_terms, term = webseatch_filter('rp.area_specialty', term)
        params |= term

    if page and lenght:
        filter_pagination = pagination(page, lenght)

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

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract, r.orcid,
            r.graduation, r.last_update AS lattes_update,
            REPLACE(rp.great_area, '_', ' ') AS area, rp.city,
            i.image AS image_university, i.name AS university,
            1 AS among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex, r.classification, r.status, r.institution_id
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
            {join_modality}
            {join_program}
            {join_dep}
        WHERE 1 = 1
            {filter_graduation}
            {filter_modality}
            {filter_institution}
            {filter_dep}
            {filter_terms}
            {filter_program}
            {filter_city}
            {filter_area}
        ORDER BY
            among DESC
            {filter_pagination};
    """
    result = conn.select(SCRIPT_SQL, params)
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
        filter_terms, term = webseatch_filter('pe.event_name', term)
        params |= term

    if page and lenght:
        filter_pagination = pagination(page, lenght)

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
            opr.openalex, r.classification, r.status, r.institution_id
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
        filter_terms, term = webseatch_filter('bp.title', term)
        params |= term

    if page and lenght:
        filter_pagination = pagination(page, lenght)

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
            1 AS among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex, r.classification, r.status, r.institution_id
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
                    {filter_type}
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
    program_id: UUID, year: int
) -> list[ResearcherArticleProduction]:
    params = {}

    program_filter = str()
    if program_id:
        params['program_id'] = program_id
        program_filter = """
            AND gpr.graduate_program_id = %(program_id)s
            AND gpr.type_ = 'PERMANENTE'
            """

    year_filter = str()
    if year:
        params['year'] = year
        year_filter = 'AND bp.year::int >= %(year)s'

    SCRIPT_SQL = f"""
        SELECT r.name, bpa.qualis, COUNT(*) AS among, bp.year,
            COALESCE(SUM(opa.citations_count), 0) AS citations
        FROM researcher r
            LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id
            RIGHT JOIN bibliographic_production_article bpa
                ON bpa.bibliographic_production_id = bp.id
            LEFT JOIN openalex_article opa ON opa.article_id = bp.id
            LEFT JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
        WHERE 1 = 1
            {program_filter}
            {year_filter}
        GROUP BY r.id, bpa.qualis, bp.year
        HAVING 1 = 1;
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


def search_in_articles(
    terms,
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
    filter_distinct = str()
    filter_graduation = str()
    filter_institution = str()
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
    if graduation:
        params['graduation'] = graduation.split(';')
        filter_graduation = 'AND r.graduation = ANY(%(graduation)s)'

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

    if institution:
        params['institution'] = institution.split(';')
        filter_institution = """
            AND i.name = ANY(%(institution)s)
            """

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    filter_terms = str()
    if terms:
        filter_terms, terms = webseatch_filter('bp.title', terms)
        params |= terms

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

    join_modality = str()
    filter_modality = str()
    if modality:
        params['modality'] = modality.split(';')
        join_modality = """
            INNER JOIN foment f
                ON f.researcher_id = r.id
            """
        filter_modality = 'AND modality_name = ANY(%(modality)s)'

    filter_graduation = str()
    if graduation:
        params['graduation'] = graduation.split(';')
        filter_graduation = 'AND r.graduation = ANY(%(graduation)s)'

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract, r.orcid,
            r.graduation, r.last_update AS lattes_update,
            REPLACE(rp.great_area, '_', ' ') AS area, rp.city,
            i.image AS image_university, i.name AS university,
            bp.among AS among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex, r.classification, r.status, r.institution_id
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
            {join_modality}
            INNER JOIN (
                SELECT bp.researcher_id, COUNT(*) AS among
                FROM bibliographic_production bp
                WHERE 1 = 1
                    AND TYPE = 'ARTICLE'
                    {filter_terms}
                GROUP BY researcher_id
            ) bp ON bp.researcher_id = r.id
            {join_program}
            {join_dep}
        WHERE 1 = 1
            {filter_program}
            {filter_dep}
            {filter_institution}
            {filter_city}
            {filter_area}
            {filter_modality}
            {filter_graduation}
        ORDER BY
            among DESC
            {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def search_in_abstracts(
    terms,
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
    filter_institution = str()
    filter_distinct = str()
    join_dep = str()
    filter_dep = str()

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

    if institution:
        params['institution'] = institution.split(';')
        filter_institution = """
            AND i.name = ANY(%(institution)s)
            """

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    filter_terms = str()
    if terms:
        filter_terms, terms = webseatch_filter('r.abstract', terms)
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

    join_modality = str()
    filter_modality = str()
    if modality:
        params['modality'] = modality.split(';')
        join_modality = """
            INNER JOIN foment f
                ON f.researcher_id = r.id
            """
        filter_modality = 'AND modality_name = ANY(%(modality)s)'

    filter_graduation = str()
    if graduation:
        params['graduation'] = graduation.split(';')
        filter_graduation = 'AND r.graduation = ANY(%(graduation)s)'

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract, r.orcid,
            r.graduation, r.last_update AS lattes_update,
            REPLACE(rp.great_area, '_', ' ') AS area, rp.city,
            i.image AS image_university, i.name AS university,
            1 AS among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex, r.classification, r.status, r.institution_id
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
            {join_modality}
            {join_program}
            {join_dep}
        WHERE 1 = 1
            {filter_terms}
            {filter_dep}
            {filter_program}
            {filter_institution}
            {filter_city}
            {filter_area}
            {filter_modality}
            {filter_graduation}
        ORDER BY
            among DESC
            {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def search_in_name(
    name,
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
    distinct = str()
    filter_area = str()
    filter_city = str()
    filter_institution = str()
    join_modality = str()
    filter_modality = str()
    filter_graduation = str()
    filter_distinct = str()
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
    if graduation:
        params['graduation'] = graduation.split(';')
        filter_graduation = 'AND r.graduation = ANY(%(graduation)s)'

    if modality:
        params['modality'] = modality.split(';')
        join_modality = """
            INNER JOIN foment f
                ON f.researcher_id = r.id
            """
        filter_modality = """
            AND modality_name = ANY(%(modality)s)
            """

    if institution:
        params['institution'] = institution.split(';')
        filter_institution = 'AND i.name = ANY(%(institution)s)'

    if city:
        params['city'] = city.split(';')
        filter_city = 'AND rp.city = ANY(%(city)s)'

    if area:
        params['area'] = area.replace(' ', '_').split(';')
        filter_area = """
            AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s
        """

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
        filter_program = 'AND gp.name = ANY(%(graduate_program)s)'

    filter_name = str()
    if name:
        filter_name, terms = names_filter('r.name', name)
        params |= terms

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    if graduate_program_id:
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
            1 AS among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex, r.classification, r.status, r.institution_id
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
            {join_program}
            {join_modality}
            {join_dep}
        WHERE 1 = 1
            {filter_program}
            {filter_dep}
            {filter_modality}
            {filter_city}
            {filter_institution}
            {filter_graduation}
            {filter_name}
            {filter_area}
        ORDER BY
            among DESC
            {filter_pagination};
        """

    result = conn.select(SCRIPT_SQL, params)
    return result


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
        filter_name, terms = names_filter('r.name', name)
        params |= terms

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

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
            opr.openalex, r.classification, r.status, r.institution_id
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
                    'name',gp.name
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
        filter_pagination = pagination(page, lenght)

    filter_terms = str()

    if term:
        filter_terms, terms = webseatch_filter('p.title', term)
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
            opr.openalex, r.classification, r.status, r.institution_id
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


def list_foment_researchers(
    page: int = None,
    lenght: int = None,
):
    params = {}

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    SCRIPT_SQL = f"""
        SELECT
            r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract, r.orcid,
            r.graduation, r.last_update AS lattes_update,
            REPLACE(rp.great_area, '_', ' ') AS area, rp.city,
            i.image AS image_university, i.name AS university,
            1 AS among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex, r.classification, r.status, r.institution_id
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
            INNER JOIN foment f ON f.researcher_id = r.id
        WHERE 1 = 1
            AND i.acronym != 'UICL'
        ORDER BY
            among DESC
            {filter_pagination};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def get_researcher(researcher_id: UUID) -> dict:
    one = True
    params = {'researcher_id': researcher_id}
    SCRIPT_SQL = """
        SELECT * FROM researcher r WHERE r.id = %(researcher_id)s;
        """
    result = conn.select(SCRIPT_SQL, params, one)
    return result


def academic_degree():
    SCRIPT_SQL = """
        SELECT graduation, COUNT(*) as among
        FROM researcher
        WHERE graduation IS NOT NULL
        GROUP BY graduation;
        """
    result = conn.select(SCRIPT_SQL)
    return result
