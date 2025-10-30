from simcc.core.connection import Connection
from simcc.repositories import tools
from simcc.schemas import DefaultFilters


async def list_participation_event(conn, filters, nature):
    PARAMS = {}
    DISTINCT_SQL = ''
    FILTERS_SQL = ''
    FILTER_PAGINATION = ''

    join_researcher_production = ''
    join_foment = ''
    join_program = ''
    join_institution = ''
    join_group = ''
    join_departament = ''
    if filters.star:
        PARAMS['star'] = filters.star
        FILTERS_SQL += ' AND p.id = ANY(%(star)s)'

    if filters.collection_id:
        PARAMS['collection_id'] = filters.collection_id
        FILTERS_SQL += ' AND p.id = ANY(%(collection_id)s)'

    if filters.term:
        filter_terms_str, term_params = tools.websearch_filter(
            'p.title', filters.term
        )
        FILTERS_SQL += filter_terms_str
        PARAMS.update(term_params)

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL += ' AND p.researcher_id = %(researcher_id)s'

    if filters.year:
        PARAMS['year'] = filters.year
        FILTERS_SQL += ' AND p.year >= %(year)s'

    if nature:
        PARAMS['nature'] = nature.split(';')
        FILTERS_SQL += ' AND p.nature = ANY(%(nature)s)'

    if filters.dep_id:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        PARAMS['dep_id'] = filters.dep_id
        FILTERS_SQL += ' AND dp.dep_id = %(dep_id)s'

    if filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        PARAMS['departament'] = filters.departament.split(';')
        FILTERS_SQL += ' AND dp.dep_nom = ANY(%(departament)s)'

    if filters.group_id or filters.group:
        join_group = """
            INNER JOIN research_group_researcher rgr ON rgr.researcher_id = r.id
            INNER JOIN research_group rg ON rg.id = rgr.research_group_id
        """
        if filters.group_id:
            PARAMS['group_id'] = filters.group_id
            FILTERS_SQL += ' AND rgr.research_group_id = %(group_id)s'
        if filters.group:
            PARAMS['group'] = filters.group.split(';')
            FILTERS_SQL += ' AND rg.name = ANY(%(group)s)'
    if filters.institution or filters.institution_id:
        join_institution = 'INNER JOIN institution i ON r.institution_id = i.id'
        if filters.institution:
            PARAMS['institution'] = filters.institution.split(';')
            FILTERS_SQL += ' AND i.name = ANY(%(institution)s)'
        if filters.institution_id:
            PARAMS['institution_id'] = filters.institution_id
            FILTERS_SQL += ' AND i.id = %(institution_id)s'
    if filters.graduate_program_id:
        filters.distinct = '1'
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
        FILTERS_SQL += ' AND gpr.graduate_program_id = %(graduate_program_id)s'

    if filters.graduate_program:
        filters.distinct = '1'
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        PARAMS['graduate_program'] = filters.graduate_program.split(';')
        FILTERS_SQL += ' AND gp.name = ANY(%(graduate_program)s)'

    if filters.city:
        join_researcher_production = (
            'LEFT JOIN researcher_production rp ON rp.researcher_id = r.id'
        )
        PARAMS['city'] = filters.city.split(';')
        FILTERS_SQL += ' AND rp.city = ANY(%(city)s)'

    if filters.area:
        join_researcher_production = (
            'LEFT JOIN researcher_production rp ON rp.researcher_id = r.id'
        )
        PARAMS['area'] = filters.area.replace(' ', '_').split(';')
        FILTERS_SQL += ' AND rp.great_area_ && %(area)s'

    if filters.modality:
        join_foment = 'INNER JOIN foment f ON f.researcher_id = r.id'
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL += ' AND f.modality_name = ANY(%(modality)s)'

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL += ' AND r.graduation = ANY(%(graduation)s)'

    if filters.page and filters.lenght:
        FILTER_PAGINATION = tools.pagination(filters.page, filters.lenght)
    if filters.lattes_id:
        PARAMS['lattes_id'] = filters.lattes_id
        FILTERS_SQL += ' AND r.lattes_id = %(lattes_id)s'

    if filters.distinct == '1':
        DISTINCT_SQL = 'DISTINCT ON (p.title)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
            r.name,
            p.id,
            p.title,
            p.event_name,
            p.nature,
            p.form_participation,
            p.year
        FROM public.participation_events p
            LEFT JOIN researcher r ON r.id = p.researcher_id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
            {join_group}
        WHERE 1 = 1
            {FILTERS_SQL}
        ORDER BY p.title, p.year DESC
        {FILTER_PAGINATION}
        """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def list_professional_experience(conn, filters):
    PARAMS = {}
    DISTINCT_SQL = ''
    FILTERS_SQL = ''
    FILTER_PAGINATION = ''
    FILTER_TERMS = ''

    join_researcher = 'LEFT JOIN researcher r ON r.id = rpe.researcher_id'
    join_researcher_production = ''
    join_foment = ''
    join_program = ''
    join_institution = ''
    join_departament = ''
    join_group = ''

    if filters.star:
        PARAMS['star'] = filters.star
        FILTERS_SQL += ' AND rpe.id = ANY(%(star)s)'

    if filters.collection_id:
        PARAMS['collection_id'] = filters.collection_id
        FILTERS_SQL += ' AND rpe.id = ANY(%(collection_id)s)'

    if filters.term or filters.terms:
        term_value = filters.term or filters.terms
        FILTER_TERMS, term_params = tools.websearch_filter(
            'rpe.enterprise', term_value
        )
        PARAMS.update(term_params)

    if filters.year:
        PARAMS['year'] = filters.year
        FILTERS_SQL += ' AND COALESCE(rpe.end_year, rpe.start_year) >= %(year)s'

    if filters.dep_id or filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = rpe.researcher_id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id
            FILTERS_SQL += ' AND dp.dep_id = %(dep_id)s'
        if filters.departament:
            PARAMS['departament'] = filters.departament.split(';')
            FILTERS_SQL += ' AND dp.dep_nom = ANY(%(departament)s)'

    if filters.researcher_id:
        PARAMS['researcher_id'] = filters.researcher_id
        FILTERS_SQL += ' AND rpe.researcher_id = %(researcher_id)s'

    if filters.lattes_id:
        PARAMS['lattes_id'] = filters.lattes_id
        FILTERS_SQL += ' AND r.lattes_id = %(lattes_id)s'

    if filters.institution or filters.institution_id:
        join_institution = 'INNER JOIN institution i ON r.institution_id = i.id'
        if filters.institution:
            PARAMS['institution'] = filters.institution.split(';')
            FILTERS_SQL += ' AND i.name = ANY(%(institution)s)'
        if filters.institution_id:
            PARAMS['institution_id'] = filters.institution_id
            FILTERS_SQL += ' AND i.id = %(institution_id)s'

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL += ' AND r.graduation = ANY(%(graduation)s)'

    if filters.graduate_program_id or filters.graduate_program:
        DISTINCT_SQL = 'DISTINCT'
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = rpe.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        if filters.graduate_program_id:
            PARAMS['graduate_program_id'] = filters.graduate_program_id
            FILTERS_SQL += (
                ' AND gpr.graduate_program_id = %(graduate_program_id)s'
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL += ' AND gp.name = ANY(%(graduate_program)s)'

    if filters.city or filters.area:
        join_researcher_production = 'LEFT JOIN researcher_production rp ON rp.researcher_id = rpe.researcher_id'
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL += ' AND rp.city = ANY(%(city)s)'
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL += ' AND rp.great_area_ && %(area)s'

    if filters.modality:
        DISTINCT_SQL = 'DISTINCT'
        join_foment = (
            'INNER JOIN foment f ON f.researcher_id = rpe.researcher_id'
        )
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL += ' AND f.modality_name = ANY(%(modality)s)'

    if filters.group_id or filters.group:
        join_group = """
            INNER JOIN research_group_researcher rgr ON rgr.researcher_id = rpe.researcher_id
            INNER JOIN research_group rg ON rg.id = rgr.research_group_id
            """
        if filters.group_id:
            PARAMS['group_id'] = filters.group_id
            FILTERS_SQL += ' AND rgr.research_group_id = %(group_id)s'
        if filters.group:
            PARAMS['group'] = filters.group.split(';')
            FILTERS_SQL += ' AND rg.name = ANY(%(group)s)'

    if filters.page and filters.lenght:
        FILTER_PAGINATION = tools.pagination(filters.page, filters.lenght)

    if filters.distinct == '1':
        DISTINCT_SQL = 'DISTINCT ON (rpe.researcher_id, rpe.enterprise)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
            rpe.id, rpe.researcher_id, enterprise, start_year, end_year,
            employment_type, other_employment_type, functional_classification,
            other_functional_classification, workload_hours_weekly,
            exclusive_dedication, additional_info,
            r.lattes_id, r.name as researcher_name, r.graduation
        FROM public.researcher_professional_experience rpe
            {join_researcher}
            {join_institution}
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_group}
        WHERE 1 = 1
            {FILTERS_SQL}
            {FILTER_TERMS}
        ORDER BY {'rpe.researcher_id, rpe.enterprise, rpe.start_year DESC' if DISTINCT_SQL else 'rpe.start_year DESC'}
        {FILTER_PAGINATION}
        """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def list_patent(conn, filters):
    PARAMS = {}
    DISTINCT_SQL = ''
    FILTERS_SQL = ''
    FILTER_PAGINATION = ''
    FILTER_TERMS = ''
    FILTER_YEAR = ''

    join_researcher_production = ''
    join_foment = ''
    join_program = ''
    join_institution = ''
    join_departament = ''
    join_group = ''

    if filters.star:
        PARAMS['star'] = filters.star
        FILTERS_SQL += ' AND p.id = ANY(%(star)s)'

    if filters.collection_id:
        PARAMS['collection_id'] = filters.collection_id
        FILTERS_SQL += ' AND p.id = ANY(%(collection_id)s)'

    if filters.term:
        FILTER_TERMS, term_params = tools.websearch_filter(
            'p.title', filters.term
        )
        PARAMS.update(term_params)

    if filters.year:
        PARAMS['year'] = filters.year
        FILTER_YEAR = 'AND p.development_year::INT >= %(year)s'

    if filters.dep_id or filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = p.researcher_id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
    if filters.dep_id:
        PARAMS['dep_id'] = filters.dep_id
        FILTERS_SQL += ' AND dp.dep_id = %(dep_id)s'

    if filters.departament:
        PARAMS['departament'] = filters.departament.split(';')
        FILTERS_SQL += ' AND dp.dep_nom = ANY(%(departament)s)'

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL += ' AND p.researcher_id = %(researcher_id)s'

    if filters.lattes_id:
        PARAMS['lattes_id'] = filters.lattes_id
        FILTERS_SQL += ' AND r.lattes_id = %(lattes_id)s'

    if filters.group_id or filters.group:
        join_group = """
            INNER JOIN research_group_researcher rgr ON rgr.researcher_id = r.id
            INNER JOIN research_group rg ON rg.id = rgr.research_group_id
        """
        if filters.group_id:
            PARAMS['group_id'] = filters.group_id
            FILTERS_SQL += ' AND rgr.research_group_id = %(group_id)s'
        if filters.group:
            PARAMS['group'] = filters.group.split(';')
            FILTERS_SQL += ' AND rg.name = ANY(%(group)s)'
    if filters.institution or filters.institution_id:
        join_institution = 'INNER JOIN institution i ON r.institution_id = i.id'
        if filters.institution:
            PARAMS['institution'] = filters.institution.split(';')
            FILTERS_SQL += ' AND i.name = ANY(%(institution)s)'
        if filters.institution_id:
            PARAMS['institution_id'] = filters.institution_id
            FILTERS_SQL += ' AND i.id = %(institution_id)s'
    if filters.graduate_program_id:
        DISTINCT_SQL = 'DISTINCT'
        PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = p.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        FILTERS_SQL += ' AND gpr.graduate_program_id = %(graduate_program_id)s'

    if filters.graduate_program:
        DISTINCT_SQL = 'DISTINCT'
        PARAMS['graduate_program'] = filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = p.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        FILTERS_SQL += ' AND gp.name = ANY(%(graduate_program)s)'

    if filters.city:
        PARAMS['city'] = filters.city.split(';')
        join_researcher_production = 'LEFT JOIN researcher_production rp ON rp.researcher_id = p.researcher_id'
        FILTERS_SQL += ' AND rp.city = ANY(%(city)s)'

    if filters.area:
        PARAMS['area'] = filters.area.replace(' ', '_').split(';')
        if not join_researcher_production:
            join_researcher_production = 'LEFT JOIN researcher_production rp ON rp.researcher_id = p.researcher_id'
        FILTERS_SQL += ' AND rp.great_area_ && %(area)s'

    if filters.modality:
        DISTINCT_SQL = 'DISTINCT'
        PARAMS['modality'] = filters.modality.split(';')
        join_foment = 'INNER JOIN foment f ON f.researcher_id = p.researcher_id'
        FILTERS_SQL += ' AND f.modality_name = ANY(%(modality)s)'

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL += ' AND r.graduation = ANY(%(graduation)s)'

    if filters.page and filters.lenght:
        FILTER_PAGINATION = tools.pagination(filters.page, filters.lenght)

    if filters.distinct == '1':
        DISTINCT_SQL = 'DISTINCT ON (p.title)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
            p.id, p.title, p.category, p.relevance, p.has_image,
            p.development_year AS year, p.details, p.grant_date, p.deposit_date,
            r.id AS researcher, r.lattes_id, r.name AS name, p.code, p.stars
        FROM public.patent p
            INNER JOIN public.researcher r ON r.id = p.researcher_id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
            {join_group}
        WHERE 1 = 1
            {FILTERS_SQL}
            {FILTER_TERMS}
            {FILTER_YEAR}
        ORDER BY {'p.title DESC, p.development_year DESC' if DISTINCT_SQL else 'p.development_year DESC'}
        {FILTER_PAGINATION};
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def list_brand(conn, filters):
    PARAMS = {}
    DISTINCT_SQL = ''
    FILTERS_SQL = ''
    FILTER_PAGINATION = ''

    join_researcher_production = ''
    join_foment = ''
    join_program = ''
    join_institution = ''
    join_departament = ''
    join_group = ''

    if filters.star:
        PARAMS['star'] = filters.star
        FILTERS_SQL += ' AND b.id = ANY(%(star)s)'

    if filters.collection_id:
        PARAMS['collection_id'] = filters.collection_id
        FILTERS_SQL += ' AND b.id = ANY(%(collection_id)s)'

    if filters.term:
        filter_terms_str, term_params = tools.websearch_filter(
            'b.title', filters.term
        )
        FILTERS_SQL += filter_terms_str
        PARAMS.update(term_params)

    if filters.year:
        PARAMS['year'] = filters.year
        FILTERS_SQL += ' AND b.year::INT >= %(year)s'

    if filters.dep_id:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = b.researcher_id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        PARAMS['dep_id'] = filters.dep_id
        FILTERS_SQL += ' AND dp.dep_id = %(dep_id)s'

    if filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = b.researcher_id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        PARAMS['departament'] = filters.departament.split(';')
        FILTERS_SQL += ' AND dp.dep_nom = ANY(%(departament)s)'

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL += ' AND b.researcher_id = %(researcher_id)s'

    if filters.group_id or filters.group:
        join_group = """
            INNER JOIN research_group_researcher rgr ON rgr.researcher_id = r.id
            INNER JOIN research_group rg ON rg.id = rgr.research_group_id
        """
        if filters.group_id:
            PARAMS['group_id'] = filters.group_id
            FILTERS_SQL += ' AND rgr.research_group_id = %(group_id)s'
        if filters.group:
            PARAMS['group'] = filters.group.split(';')
            FILTERS_SQL += ' AND rg.name = ANY(%(group)s)'
    if filters.institution or filters.institution_id:
        join_institution = 'INNER JOIN institution i ON r.institution_id = i.id'
        if filters.institution:
            PARAMS['institution'] = filters.institution.split(';')
            FILTERS_SQL += ' AND i.name = ANY(%(institution)s)'
        if filters.institution_id:
            PARAMS['institution_id'] = filters.institution_id
            FILTERS_SQL += ' AND i.id = %(institution_id)s'
    if filters.graduate_program_id:
        DISTINCT_SQL = 'DISTINCT'
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = b.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
        FILTERS_SQL += ' AND gpr.graduate_program_id = %(graduate_program_id)s'

    if filters.graduate_program:
        DISTINCT_SQL = 'DISTINCT'
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = b.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        PARAMS['graduate_program'] = filters.graduate_program.split(';')
        FILTERS_SQL += ' AND gp.name = ANY(%(graduate_program)s)'

    if filters.city:
        join_researcher_production = 'LEFT JOIN researcher_production rp ON rp.researcher_id = b.researcher_id'
        PARAMS['city'] = filters.city.split(';')
        FILTERS_SQL += ' AND rp.city = ANY(%(city)s)'

    if filters.area:
        join_researcher_production = 'LEFT JOIN researcher_production rp ON rp.researcher_id = b.researcher_id'
        PARAMS['area'] = filters.area.replace(' ', '_').split(';')
        FILTERS_SQL += ' AND rp.great_area_ && %(area)s'

    if filters.modality:
        DISTINCT_SQL = 'DISTINCT'
        join_foment = 'INNER JOIN foment f ON f.researcher_id = b.researcher_id'
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL += ' AND f.modality_name = ANY(%(modality)s)'

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL += ' AND r.graduation = ANY(%(graduation)s)'

    if filters.page and filters.lenght:
        FILTER_PAGINATION = tools.pagination(filters.page, filters.lenght)

    if filters.lattes_id:
        PARAMS['lattes_id'] = filters.lattes_id
        FILTERS_SQL += ' AND r.lattes_id = %(lattes_id)s'

    if filters.distinct == '1':
        DISTINCT_SQL = 'DISTINCT ON (b.title)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
            b.id, b.title, b.year, b.has_image, b.relevance,
            b.goal, b.nature, b.stars,
            r.id AS researcher_id, r.lattes_id, r.name
        FROM public.brand b
            INNER JOIN researcher r ON r.id = b.researcher_id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
            {join_group}
        WHERE 1 = 1
            {FILTERS_SQL}
        ORDER BY {'b.title, b.year DESC' if DISTINCT_SQL else 'b.year DESC'}
        {FILTER_PAGINATION}
        """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def list_book(conn, filters):
    PARAMS = {}
    DISTINCT_SQL = ''
    FILTERS_SQL = ''
    FILTER_PAGINATION = ''
    FILTER_TERMS = ''
    FILTER_YEAR = ''

    join_researcher_production = ''
    join_foment = ''
    join_program = ''
    join_institution = ''
    join_departament = ''
    join_group = ''

    if filters.star:
        PARAMS['star'] = filters.star
        FILTERS_SQL += ' AND bp.id = ANY(%(star)s)'

    if filters.collection_id:
        PARAMS['collection_id'] = filters.collection_id
        FILTERS_SQL += ' AND bp.id = ANY(%(collection_id)s)'

    if filters.term:
        FILTER_TERMS, term_params = tools.websearch_filter(
            'bp.title', filters.term
        )
        PARAMS.update(term_params)

    if filters.year:
        PARAMS['year'] = filters.year
        FILTER_YEAR = 'AND bp.year::INT >= %(year)s'

    if filters.dep_id or filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = bp.researcher_id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
    if filters.dep_id:
        PARAMS['dep_id'] = filters.dep_id
        FILTERS_SQL += ' AND dp.dep_id = %(dep_id)s'

    if filters.departament:
        PARAMS['departament'] = filters.departament.split(';')
        FILTERS_SQL += ' AND dp.dep_nom = ANY(%(departament)s)'

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL += ' AND bp.researcher_id = %(researcher_id)s'

    if filters.lattes_id:
        PARAMS['lattes_id'] = filters.lattes_id
        FILTERS_SQL += ' AND r.lattes_id = %(lattes_id)s'

    if filters.group_id or filters.group:
        join_group = """
            INNER JOIN research_group_researcher rgr ON rgr.researcher_id = r.id
            INNER JOIN research_group rg ON rg.id = rgr.research_group_id
        """
        if filters.group_id:
            PARAMS['group_id'] = filters.group_id
            FILTERS_SQL += ' AND rgr.research_group_id = %(group_id)s'
        if filters.group:
            PARAMS['group'] = filters.group.split(';')
            FILTERS_SQL += ' AND rg.name = ANY(%(group)s)'
    if filters.institution or filters.institution_id:
        join_institution = 'INNER JOIN institution i ON r.institution_id = i.id'
        if filters.institution:
            PARAMS['institution'] = filters.institution.split(';')
            FILTERS_SQL += ' AND i.name = ANY(%(institution)s)'
        if filters.institution_id:
            PARAMS['institution_id'] = filters.institution_id
            FILTERS_SQL += ' AND i.id = %(institution_id)s'
    if filters.graduate_program_id:
        DISTINCT_SQL = 'DISTINCT'
        PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        FILTERS_SQL += ' AND gpr.graduate_program_id = %(graduate_program_id)s'

    if filters.graduate_program:
        DISTINCT_SQL = 'DISTINCT'
        PARAMS['graduate_program'] = filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        FILTERS_SQL += ' AND gp.name = ANY(%(graduate_program)s)'

    if filters.city:
        PARAMS['city'] = filters.city.split(';')
        join_researcher_production = 'LEFT JOIN researcher_production rp ON rp.researcher_id = bp.researcher_id'
        FILTERS_SQL += ' AND rp.city = ANY(%(city)s)'

    if filters.area:
        PARAMS['area'] = filters.area.replace(' ', '_').split(';')
        if not join_researcher_production:
            join_researcher_production = 'LEFT JOIN researcher_production rp ON rp.researcher_id = bp.researcher_id'
        FILTERS_SQL += ' AND rp.great_area_ && %(area)s'

    if filters.modality:
        DISTINCT_SQL = 'DISTINCT'
        PARAMS['modality'] = filters.modality.split(';')
        join_foment = 'INNER JOIN foment f ON f.researcher_id = bp.researcher_id'
        FILTERS_SQL += ' AND f.modality_name = ANY(%(modality)s)'

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL += ' AND r.graduation = ANY(%(graduation)s)'

    if filters.page and filters.lenght:
        FILTER_PAGINATION = tools.pagination(filters.page, filters.lenght)

    if filters.distinct == '1':
        DISTINCT_SQL = 'DISTINCT ON (bp.title)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
            bp.title, bp.year, bpb.isbn AS isbn,
            bpb.publishing_company AS publishing_company,
            bp.researcher_id AS researcher,
            r.lattes_id AS lattes_id, bp.relevance,
            bp.has_image, bp.id, r.name, bpb.stars
        FROM public.bibliographic_production bp
            INNER JOIN public.bibliographic_production_book bpb ON bp.id = bpb.bibliographic_production_id
            INNER JOIN public.researcher r ON r.id = bp.researcher_id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
            {join_group}
        WHERE 1 = 1
            {FILTERS_SQL}
            {FILTER_TERMS}
            {FILTER_YEAR}
        ORDER BY {'bp.title, bp.year DESC' if DISTINCT_SQL else 'bp.year DESC, bp.title ASC'}
        {FILTER_PAGINATION};
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def list_bibliographic_production(conn, filters, qualis: str | None):
    PARAMS = {}
    DISTINCT_SQL = ''
    FILTERS_SQL = ''
    FILTER_PAGINATION = ''
    FILTER_TERMS = ''
    FILTER_YEAR = ''

    join_researcher_production = ''
    join_foment = ''
    join_program = ''
    join_departament = ''
    join_institution = ''
    join_group = ''

    if filters.collection_id:
        PARAMS['collection_id'] = filters.collection_id
        FILTERS_SQL += ' AND b.id = ANY(%(collection_id)s)'

    if filters.star:
        PARAMS['star'] = filters.star
        FILTERS_SQL += ' AND b.id = ANY(%(star)s)'

    if filters.term:
        FILTER_TERMS, term_params = tools.websearch_filter(
            'b.title', filters.term
        )
        PARAMS.update(term_params)

    if filters.year:
        PARAMS['year'] = filters.year
        FILTER_YEAR = 'AND b.year::INT >= %(year)s'

    if filters.type:
        PARAMS['type'] = filters.type.split(';')
        FILTERS_SQL += ' AND b.type = ANY(%(type)s)'

    if qualis:
        PARAMS['qualis'] = qualis.split(';')
        FILTERS_SQL += ' AND bpa.qualis = ANY(%(qualis)s)'

    if filters.dep_id or filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = b.researcher_id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id
            FILTERS_SQL += ' AND dp.dep_id = %(dep_id)s'
        if filters.departament:
            PARAMS['departament'] = filters.departament.split(';')
            FILTERS_SQL += ' AND dp.dep_nom = ANY(%(departament)s)'

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL += ' AND b.researcher_id = %(researcher_id)s'

    if filters.lattes_id:
        PARAMS['lattes_id'] = filters.lattes_id
        FILTERS_SQL += ' AND r.lattes_id = %(lattes_id)s'

    if filters.group_id or filters.group:
        join_group = """
            INNER JOIN research_group_researcher rgr ON rgr.researcher_id = r.id
            INNER JOIN research_group rg ON rg.id = rgr.research_group_id
        """
        if filters.group_id:
            PARAMS['group_id'] = filters.group_id
            FILTERS_SQL += ' AND rgr.research_group_id = %(group_id)s'
        if filters.group:
            PARAMS['group'] = filters.group.split(';')
            FILTERS_SQL += ' AND rg.name = ANY(%(group)s)'
    if filters.institution or filters.institution_id:
        join_institution = 'INNER JOIN institution i ON r.institution_id = i.id'
        if filters.institution:
            PARAMS['institution'] = filters.institution.split(';')
            FILTERS_SQL += ' AND i.name = ANY(%(institution)s)'
        if filters.institution_id:
            PARAMS['institution_id'] = filters.institution_id
            FILTERS_SQL += ' AND i.id = %(institution_id)s'
    if filters.graduate_program_id or filters.graduate_program:
        DISTINCT_SQL = 'DISTINCT'
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = b.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        if filters.graduate_program_id:
            PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL += (
                ' AND gpr.graduate_program_id = %(graduate_program_id)s'
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL += ' AND gp.name = ANY(%(graduate_program)s)'

    if filters.city or filters.area:
        join_researcher_production = 'LEFT JOIN researcher_production rp ON rp.researcher_id = b.researcher_id'
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL += ' AND rp.city = ANY(%(city)s)'
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL += ' AND rp.great_area_ && %(area)s'

    if filters.modality:
        DISTINCT_SQL = 'DISTINCT'
        join_foment = 'INNER JOIN foment f ON f.researcher_id = b.researcher_id'
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL += ' AND f.modality_name = ANY(%(modality)s)'

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL += ' AND r.graduation = ANY(%(graduation)s)'

    if filters.page and filters.lenght:
        FILTER_PAGINATION = tools.pagination(filters.page, filters.lenght)

    if filters.distinct == '1':
        DISTINCT_SQL = 'DISTINCT ON (b.title)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
            b.id AS id, title, b.year, b.type, doi, bpa.qualis,
            periodical_magazine_name AS magazine, r.name AS researcher,
            r.lattes_10_id, r.lattes_id, jcr AS jif,
            jcr_link, r.id AS researcher_id, opa.abstract,
            opa.article_institution, opa.authors, opa.authors_institution,
            COALESCE (opa.citations_count, 0) AS citations_count, bpa.issn,
            opa.keywords, opa.landing_page_url, opa.language, opa.pdf,
            b.has_image, b.relevance, bpa.created_at AS created_at, bpa.stars
        FROM bibliographic_production b
            LEFT JOIN bibliographic_production_article bpa ON b.id = bpa.bibliographic_production_id
            LEFT JOIN researcher r ON r.id = b.researcher_id
            LEFT JOIN openalex_article opa ON opa.article_id = b.id
            {join_institution}
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_group}
        WHERE 1 = 1
            {FILTERS_SQL}
            {FILTER_TERMS}
            {FILTER_YEAR}
        ORDER BY {'b.title, b.year DESC' if DISTINCT_SQL else 'b.year DESC'}
        {FILTER_PAGINATION};
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def list_book_chapter(conn, filters):
    PARAMS = {}
    DISTINCT_SQL = ''
    FILTERS_SQL = ''
    FILTER_PAGINATION = ''
    FILTER_TERMS = ''
    FILTER_YEAR = ''

    join_researcher_production = ''
    join_foment = ''
    join_program = ''
    join_institution = ''
    join_departament = ''
    join_group = ''

    if filters.star:
        PARAMS['star'] = filters.star
        FILTERS_SQL += ' AND bp.id = ANY(%(star)s)'

    if filters.collection_id:
        PARAMS['collection_id'] = filters.collection_id
        FILTERS_SQL += ' AND bp.id = ANY(%(collection_id)s)'

    if filters.term:
        FILTER_TERMS, term_params = tools.websearch_filter(
            'bp.title', filters.term
        )
        PARAMS.update(term_params)

    if filters.year:
        PARAMS['year'] = filters.year
        FILTER_YEAR = 'AND bp.year::INT >= %(year)s'

    if filters.dep_id or filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = bp.researcher_id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id
            FILTERS_SQL += ' AND dp.dep_id = %(dep_id)s'
        if filters.departament:
            PARAMS['departament'] = filters.departament.split(';')
            FILTERS_SQL += ' AND dp.dep_nom = ANY(%(departament)s)'

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL += ' AND bp.researcher_id = %(researcher_id)s'

    if filters.lattes_id:
        PARAMS['lattes_id'] = filters.lattes_id
        FILTERS_SQL += ' AND r.lattes_id = %(lattes_id)s'

    if filters.group_id or filters.group:
        join_group = """
            INNER JOIN research_group_researcher rgr ON rgr.researcher_id = r.id
            INNER JOIN research_group rg ON rg.id = rgr.research_group_id
        """
        if filters.group_id:
            PARAMS['group_id'] = filters.group_id
            FILTERS_SQL += ' AND rgr.research_group_id = %(group_id)s'
        if filters.group:
            PARAMS['group'] = filters.group.split(';')
            FILTERS_SQL += ' AND rg.name = ANY(%(group)s)'
    if filters.institution or filters.institution_id:
        join_institution = 'INNER JOIN institution i ON r.institution_id = i.id'
        if filters.institution:
            PARAMS['institution'] = filters.institution.split(';')
            FILTERS_SQL += ' AND i.name = ANY(%(institution)s)'
        if filters.institution_id:
            PARAMS['institution_id'] = filters.institution_id
            FILTERS_SQL += ' AND i.id = %(institution_id)s'
    if filters.graduate_program_id or filters.graduate_program:
        DISTINCT_SQL = 'DISTINCT'
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        if filters.graduate_program_id:
            PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL += (
                ' AND gpr.graduate_program_id = %(graduate_program_id)s'
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL += ' AND gp.name = ANY(%(graduate_program)s)'

    if filters.city or filters.area:
        join_researcher_production = 'LEFT JOIN researcher_production rp ON rp.researcher_id = bp.researcher_id'
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL += ' AND rp.city = ANY(%(city)s)'
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL += ' AND rp.great_area_ && %(area)s'

    if filters.modality:
        DISTINCT_SQL = 'DISTINCT'
        join_foment = 'INNER JOIN foment f ON f.researcher_id = bp.researcher_id'
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL += ' AND f.modality_name = ANY(%(modality)s)'

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL += ' AND r.graduation = ANY(%(graduation)s)'

    if filters.page and filters.lenght:
        FILTER_PAGINATION = tools.pagination(filters.page, filters.lenght)

    if filters.distinct == '1':
        DISTINCT_SQL = 'DISTINCT ON (bp.title)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
            bp.title, bp.year, bpc.isbn, bpc.publishing_company,
            bp.researcher_id AS researcher, bp.id, r.lattes_id, bp.relevance,
            bp.has_image, r.name
        FROM bibliographic_production bp
            INNER JOIN bibliographic_production_book_chapter bpc ON bpc.bibliographic_production_id = bp.id
            LEFT JOIN researcher r ON r.id = bp.researcher_id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
            {join_group}
        WHERE 1 = 1
            {FILTERS_SQL}
            {FILTER_TERMS}
            {FILTER_YEAR}
        ORDER BY {'bp.title, bp.year DESC' if DISTINCT_SQL else 'bp.year DESC'}
        {FILTER_PAGINATION};
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def list_software(conn, filters):
    PARAMS = {}
    DISTINCT_SQL = ''
    FILTERS_SQL = ''
    FILTER_PAGINATION = ''
    FILTER_TERMS = ''
    FILTER_YEAR = ''

    join_researcher_production = ''
    join_foment = ''
    join_program = ''
    join_institution = ''
    join_departament = ''
    join_group = ''

    if filters.star:
        PARAMS['star'] = filters.star
        FILTERS_SQL += ' AND s.id = ANY(%(star)s)'

    if filters.collection_id:
        PARAMS['collection_id'] = filters.collection_id
        FILTERS_SQL += ' AND s.id = ANY(%(collection_id)s)'

    if filters.term:
        FILTER_TERMS, term_params = tools.websearch_filter(
            's.title', filters.term
        )
        PARAMS.update(term_params)

    if filters.group_id or filters.group:
        join_group = """
            INNER JOIN research_group_researcher rgr ON rgr.researcher_id = r.id
            INNER JOIN research_group rg ON rg.id = rgr.research_group_id
        """
        if filters.group_id:
            PARAMS['group_id'] = filters.group_id
            FILTERS_SQL += ' AND rgr.research_group_id = %(group_id)s'
        if filters.group:
            PARAMS['group'] = filters.group.split(';')
            FILTERS_SQL += ' AND rg.name = ANY(%(group)s)'

    if filters.year:
        PARAMS['year'] = filters.year
        FILTER_YEAR = 'AND s.year::INT >= %(year)s'

    if filters.dep_id or filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = s.researcher_id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id
            FILTERS_SQL += ' AND dp.dep_id = %(dep_id)s'
        if filters.departament:
            PARAMS['departament'] = filters.departament.split(';')
            FILTERS_SQL += ' AND dp.dep_nom = ANY(%(departament)s)'

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL += ' AND s.researcher_id = %(researcher_id)s'

    # Filtro adicionado
    if filters.lattes_id:
        PARAMS['lattes_id'] = filters.lattes_id
        FILTERS_SQL += ' AND r.lattes_id = %(lattes_id)s'

    if filters.institution or filters.institution_id:
        join_institution = 'INNER JOIN institution i ON r.institution_id = i.id'
        if filters.institution:
            PARAMS['institution'] = filters.institution.split(';')
            FILTERS_SQL += ' AND i.name = ANY(%(institution)s)'
        if filters.institution_id:
            PARAMS['institution_id'] = filters.institution_id
            FILTERS_SQL += ' AND i.id = %(institution_id)s'

    if filters.graduate_program_id or filters.graduate_program:
        DISTINCT_SQL = 'DISTINCT'
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = s.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        if filters.graduate_program_id:
            PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL += (
                ' AND gpr.graduate_program_id = %(graduate_program_id)s'
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL += ' AND gp.name = ANY(%(graduate_program)s)'

    if filters.city or filters.area:
        join_researcher_production = 'LEFT JOIN researcher_production rp ON rp.researcher_id = s.researcher_id'
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL += ' AND rp.city = ANY(%(city)s)'
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL += ' AND rp.great_area_ && %(area)s'

    if filters.modality:
        DISTINCT_SQL = 'DISTINCT'
        join_foment = 'INNER JOIN foment f ON f.researcher_id = s.researcher_id'
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL += ' AND f.modality_name = ANY(%(modality)s)'

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL += ' AND r.graduation = ANY(%(graduation)s)'

    # Filtro adicionado
    if filters.group_id:
        join_group = 'INNER JOIN research_group_researcher rgr ON rgr.researcher_id = s.researcher_id'
        PARAMS['group_id'] = filters.group_id
        FILTERS_SQL += ' AND rgr.research_group_id = %(group_id)s'

    if filters.page and filters.lenght:
        FILTER_PAGINATION = tools.pagination(filters.page, filters.lenght)

    if filters.distinct == '1':
        DISTINCT_SQL = 'DISTINCT ON (s.title)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
            s.id, s.title, s.year AS year, s.has_image, s.relevance,
            s.platform, s.goal, s.environment, s.availability, s.financing_institutionc,
            r.name, r.id AS researcher_id, r.lattes_id, s.stars
        FROM public.software s
            LEFT JOIN public.researcher r ON s.researcher_id = r.id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
            {join_group}
        WHERE 1 = 1
            {FILTERS_SQL}
            {FILTER_TERMS}
            {FILTER_YEAR}
        ORDER BY {'s.title, s.year DESC' if DISTINCT_SQL else 's.year DESC'}
        {FILTER_PAGINATION};
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def list_researcher_report(conn, filters):
    PARAMS = {}
    DISTINCT_SQL = ''
    FILTERS_SQL = ''
    FILTER_PAGINATION = ''
    FILTER_TERMS = ''
    FILTER_YEAR = ''

    join_researcher_production = ''
    join_foment = ''
    join_program = ''
    join_institution = ''
    join_departament = ''
    join_group = ''
    if filters.star:
        PARAMS['star'] = filters.star
        FILTERS_SQL += ' AND rr.id = ANY(%(star)s)'

    if filters.collection_id:
        PARAMS['collection_id'] = filters.collection_id
        FILTERS_SQL += ' AND rr.id = ANY(%(collection_id)s)'

    if filters.term:
        FILTER_TERMS, term_params = tools.websearch_filter(
            'rr.title', filters.term
        )
        PARAMS.update(term_params)

    if filters.year:
        PARAMS['year'] = filters.year
        FILTER_YEAR = 'AND rr.year::INT >= %(year)s'

    if filters.dep_id or filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = rr.researcher_id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id
            FILTERS_SQL += ' AND dp.dep_id = %(dep_id)s'
        if filters.departament:
            PARAMS['departament'] = filters.departament.split(';')
            FILTERS_SQL += ' AND dp.dep_nom = ANY(%(departament)s)'

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL += ' AND rr.researcher_id = %(researcher_id)s'

    if filters.lattes_id:
        PARAMS['lattes_id'] = filters.lattes_id
        FILTERS_SQL += ' AND r.lattes_id = %(lattes_id)s'

    if filters.group_id or filters.group:
        join_group = """
            INNER JOIN research_group_researcher rgr ON rgr.researcher_id = r.id
            INNER JOIN research_group rg ON rg.id = rgr.research_group_id
        """
        if filters.group_id:
            PARAMS['group_id'] = filters.group_id
            FILTERS_SQL += ' AND rgr.research_group_id = %(group_id)s'
        if filters.group:
            PARAMS['group'] = filters.group.split(';')
            FILTERS_SQL += ' AND rg.name = ANY(%(group)s)'
    if filters.institution or filters.institution_id:
        join_institution = 'INNER JOIN institution i ON r.institution_id = i.id'
        if filters.institution:
            PARAMS['institution'] = filters.institution.split(';')
            FILTERS_SQL += ' AND i.name = ANY(%(institution)s)'
        if filters.institution_id:
            PARAMS['institution_id'] = filters.institution_id
            FILTERS_SQL += ' AND i.id = %(institution_id)s'
    if filters.graduate_program_id or filters.graduate_program:
        DISTINCT_SQL = 'DISTINCT'
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = rr.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        if filters.graduate_program_id:
            PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL += (
                ' AND gpr.graduate_program_id = %(graduate_program_id)s'
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL += ' AND gp.name = ANY(%(graduate_program)s)'

    if filters.city or filters.area:
        join_researcher_production = 'LEFT JOIN researcher_production rp ON rp.researcher_id = rr.researcher_id'
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL += ' AND rp.city = ANY(%(city)s)'
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL += ' AND rp.great_area_ && %(area)s'

    if filters.modality:
        DISTINCT_SQL = 'DISTINCT'
        join_foment = 'INNER JOIN foment f ON f.researcher_id = rr.researcher_id'
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL += ' AND f.modality_name = ANY(%(modality)s)'

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL += ' AND r.graduation = ANY(%(graduation)s)'

    if filters.page and filters.lenght:
        FILTER_PAGINATION = tools.pagination(filters.page, filters.lenght)

    if filters.distinct == '1':
        DISTINCT_SQL = 'DISTINCT ON (rr.title)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
            rr.id, r.name, rr.title, rr.year, rr.project_name,
            rr.financing_institutionc AS financing, rr.stars
        FROM public.research_report rr
            INNER JOIN public.researcher r ON r.id = rr.researcher_id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
            {join_group}
        WHERE 1 = 1
            {FILTERS_SQL}
            {FILTER_TERMS}
            {FILTER_YEAR}
        ORDER BY {'rr.title, rr.year DESC' if DISTINCT_SQL else 'rr.year DESC'}
        {FILTER_PAGINATION};
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def list_guidance_production(conn, filters):
    PARAMS = {}
    DISTINCT_SQL = ''
    FILTERS_SQL = ''
    FILTER_PAGINATION = ''
    FILTER_TERMS = ''
    FILTER_YEAR = ''

    join_researcher_production = ''
    join_foment = ''
    join_program = ''
    join_institution = ''
    join_departament = ''
    join_group = ''

    if filters.star:
        PARAMS['star'] = filters.star
        FILTERS_SQL += ' AND g.id = ANY(%(star)s)'

    if filters.collection_id:
        PARAMS['collection_id'] = filters.collection_id
        FILTERS_SQL += ' AND g.id = ANY(%(collection_id)s)'

    if filters.term:
        FILTER_TERMS, term_params = tools.websearch_filter(
            'g.title', filters.term
        )
        PARAMS.update(term_params)

    if filters.year:
        PARAMS['year'] = filters.year
        FILTER_YEAR = 'AND g.year::INT >= %(year)s'

    if filters.dep_id or filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = g.researcher_id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id
            FILTERS_SQL += ' AND dp.dep_id = %(dep_id)s'
        if filters.departament:
            PARAMS['departament'] = filters.departament.split(';')
            FILTERS_SQL += ' AND dp.dep_nom = ANY(%(departament)s)'

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL += ' AND g.researcher_id = %(researcher_id)s'

    if filters.lattes_id:
        PARAMS['lattes_id'] = filters.lattes_id
        FILTERS_SQL += ' AND r.lattes_id = %(lattes_id)s'

    if filters.graduate_program_id or filters.graduate_program:
        DISTINCT_SQL = 'DISTINCT'
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = g.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        if filters.graduate_program_id:
            PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL += (
                ' AND gpr.graduate_program_id = %(graduate_program_id)s'
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL += ' AND gp.name = ANY(%(graduate_program)s)'

    if filters.group_id or filters.group:
        join_group = """
            INNER JOIN research_group_researcher rgr ON rgr.researcher_id = r.id
            INNER JOIN research_group rg ON rg.id = rgr.research_group_id
        """
        if filters.group_id:
            PARAMS['group_id'] = filters.group_id
            FILTERS_SQL += ' AND rgr.research_group_id = %(group_id)s'
        if filters.group:
            PARAMS['group'] = filters.group.split(';')
            FILTERS_SQL += ' AND rg.name = ANY(%(group)s)'
    if filters.institution or filters.institution_id:
        join_institution = 'INNER JOIN institution i ON r.institution_id = i.id'
        if filters.institution:
            PARAMS['institution'] = filters.institution.split(';')
            FILTERS_SQL += ' AND i.name = ANY(%(institution)s)'
        if filters.institution_id:
            PARAMS['institution_id'] = filters.institution_id
            FILTERS_SQL += ' AND i.id = %(institution_id)s'
    if filters.city or filters.area:
        join_researcher_production = 'LEFT JOIN researcher_production rp ON rp.researcher_id = g.researcher_id'
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL += ' AND rp.city = ANY(%(city)s)'
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL += ' AND rp.great_area_ && %(area)s'

    if filters.modality:
        DISTINCT_SQL = 'DISTINCT'
        join_foment = 'INNER JOIN foment f ON f.researcher_id = g.researcher_id'
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL += ' AND f.modality_name = ANY(%(modality)s)'

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL += ' AND r.graduation = ANY(%(graduation)s)'

    if filters.type:
        PARAMS['type'] = filters.type.split(';')
        FILTERS_SQL += ' AND g.type = ANY(%(type)s)'

    if filters.page and filters.lenght:
        FILTER_PAGINATION = tools.pagination(filters.page, filters.lenght)

    if filters.distinct == '1':
        DISTINCT_SQL = 'DISTINCT ON (g.title)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
            g.id, g.title, g.nature, g.oriented, g.type, g.status,
            g.year, r.name, r.id AS researcher_id, r.lattes_id, g.stars
        FROM public.guidance g
            INNER JOIN public.researcher r ON g.researcher_id = r.id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
            {join_group}
        WHERE 1 = 1
            {FILTERS_SQL}
            {FILTER_TERMS}
            {FILTER_YEAR}
        ORDER BY {'g.title, g.year DESC' if DISTINCT_SQL else 'g.year DESC'}
        {FILTER_PAGINATION};
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def list_researcher_production_events(conn, filters):
    PARAMS = {}
    DISTINCT_SQL = ''
    FILTERS_SQL = "AND bp.type = 'WORK_IN_EVENT'"
    FILTER_PAGINATION = ''
    FILTER_TERMS = ''
    FILTER_YEAR = ''

    join_researcher_production = ''
    join_foment = ''
    join_program = ''
    join_institution = ''
    join_departament = ''
    join_group = ''
    if filters.star:
        PARAMS['star'] = filters.star
        FILTERS_SQL += ' AND bp.id = ANY(%(star)s)'

    if filters.collection_id:
        PARAMS['collection_id'] = filters.collection_id
        FILTERS_SQL += ' AND bp.id = ANY(%(collection_id)s)'

    if filters.term:
        FILTER_TERMS, term_params = tools.websearch_filter(
            'bp.title', filters.term
        )
        PARAMS.update(term_params)

    if filters.year:
        PARAMS['year'] = filters.year
        FILTER_YEAR = 'AND bp.year_::INT >= %(year)s'

    if filters.dep_id or filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = bp.researcher_id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id
            FILTERS_SQL += ' AND dp.dep_id = %(dep_id)s'
        if filters.departament:
            PARAMS['departament'] = filters.departament.split(';')
            FILTERS_SQL += ' AND dp.dep_nom = ANY(%(departament)s)'

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL += ' AND bp.researcher_id = %(researcher_id)s'

    if filters.institution:
        join_institution = 'INNER JOIN institution i ON r.institution_id = i.id'
        PARAMS['institution'] = filters.institution.split(';')
        FILTERS_SQL += ' AND i.name = ANY(%(institution)s)'

    if filters.graduate_program_id or filters.graduate_program:
        DISTINCT_SQL = 'DISTINCT'
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        if filters.graduate_program_id:
            PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL += (
                ' AND gpr.graduate_program_id = %(graduate_program_id)s'
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL += ' AND gp.name = ANY(%(graduate_program)s)'

    if filters.city or filters.area:
        join_researcher_production = 'LEFT JOIN researcher_production rp ON rp.researcher_id = bp.researcher_id'
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL += ' AND rp.city = ANY(%(city)s)'
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL += ' AND rp.great_area_ && %(area)s'

    if filters.modality:
        DISTINCT_SQL = 'DISTINCT'
        join_foment = 'INNER JOIN foment f ON f.researcher_id = bp.researcher_id'
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL += ' AND f.modality_name = ANY(%(modality)s)'

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL += ' AND r.graduation = ANY(%(graduation)s)'

    if filters.page and filters.lenght:
        FILTER_PAGINATION = tools.pagination(filters.page, filters.lenght)

    if filters.distinct == '1':
        DISTINCT_SQL = 'DISTINCT ON (bp.title)'

    if filters.institution_id:
        PARAMS['institution_id'] = filters.institution_id
        FILTERS_SQL += ' AND r.institution_id = %(institution_id)s'

    if filters.lattes_id:
        PARAMS['lattes_id'] = filters.lattes_id
        FILTERS_SQL += ' AND r.lattes_id = %(lattes_id)s'

    if filters.group_id or filters.group:
        join_group = """
            INNER JOIN research_group_researcher rgr ON rgr.researcher_id = r.id
            INNER JOIN research_group rg ON rg.id = rgr.research_group_id
        """
        if filters.group_id:
            PARAMS['group_id'] = filters.group_id
            FILTERS_SQL += ' AND rgr.research_group_id = %(group_id)s'
        if filters.group:
            PARAMS['group'] = filters.group.split(';')
            FILTERS_SQL += ' AND rg.name = ANY(%(group)s)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
            bp.id, bp.title, bp.title_en, bp.nature, bp.language,
            bp.means_divulgation, bp.homepage, bp.relevance,
            bp.scientific_divulgation, bp.authors, bp.year_,
            r.name, r.id AS researcher_id, r.lattes_id, r.lattes_10_id,
            bpew.event_classification, bpew.event_name,
            bpew.event_city, bpew.event_year,
            bpew.proceedings_title, bpew.volume, bpew.issue, bpew.series,
            bpew.start_page, bpew.end_page, bpew.publisher_name, bpew.publisher_city,
            bpew.event_name_english, bpew.identifier_number, bpew.isbn, bpew.stars
        FROM public.bibliographic_production bp
            INNER JOIN public.bibliographic_production_work_in_event bpew ON bpew.bibliographic_production_id = bp.id
            LEFT JOIN public.researcher r ON r.id = bp.researcher_id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
            {join_group}
        WHERE 1 = 1
            {FILTERS_SQL}
            {FILTER_TERMS}
            {FILTER_YEAR}
        ORDER BY {'bp.title, bp.year_ DESC' if DISTINCT_SQL else 'bp.year_ DESC'}
        {FILTER_PAGINATION};
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def list_research_projects(conn, filters):
    PARAMS = {}
    DISTINCT_SQL = ''
    FILTERS_SQL = ''
    FILTER_PAGINATION = ''
    FILTER_TERMS = ''
    FILTER_YEAR = ''

    join_researcher_production = ''
    join_foment = ''
    join_program = ''
    join_institution = ''
    join_departament = ''
    join_group = ''

    if filters.star:
        PARAMS['star'] = filters.star
        FILTERS_SQL += ' AND rp.id = ANY(%(star)s)'

    if filters.collection_id:
        PARAMS['collection_id'] = filters.collection_id
        FILTERS_SQL += ' AND rp.id = ANY(%(collection_id)s)'

    if filters.term:
        FILTER_TERMS, term_params = tools.websearch_filter(
            'rp.project_name', filters.term
        )
        PARAMS.update(term_params)

    if filters.year:
        PARAMS['year'] = filters.year
        FILTER_YEAR = 'AND rp.start_year >= %(year)s'

    if filters.dep_id or filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = rp.researcher_id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id
            FILTERS_SQL += ' AND dp.dep_id = %(dep_id)s'
        if filters.departament:
            PARAMS['departament'] = filters.departament.split(';')
            FILTERS_SQL += ' AND dp.dep_nom = ANY(%(departament)s)'

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL += ' AND rp.researcher_id = %(researcher_id)s'

    if filters.lattes_id:
        PARAMS['lattes_id'] = filters.lattes_id
        FILTERS_SQL += ' AND r.lattes_id = %(lattes_id)s'

    if filters.institution:
        join_institution = 'INNER JOIN institution i ON r.institution_id = i.id'
        PARAMS['institution'] = filters.institution.split(';')
        FILTERS_SQL += ' AND i.name = ANY(%(institution)s)'

    if filters.graduate_program_id or filters.graduate_program:
        DISTINCT_SQL = 'DISTINCT'
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = rp.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        if filters.graduate_program_id:
            PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL += (
                ' AND gpr.graduate_program_id = %(graduate_program_id)s'
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL += ' AND gp.name = ANY(%(graduate_program)s)'

    if filters.group_id or filters.group:
        join_group = """
            INNER JOIN research_group_researcher rgr ON rgr.researcher_id = r.id
            INNER JOIN research_group rg ON rg.id = rgr.research_group_id
        """
        if filters.group_id:
            PARAMS['group_id'] = filters.group_id
            FILTERS_SQL += ' AND rgr.research_group_id = %(group_id)s'
        if filters.group:
            PARAMS['group'] = filters.group.split(';')
            FILTERS_SQL += ' AND rg.name = ANY(%(group)s)'

    if filters.city or filters.area:
        join_researcher_production = 'LEFT JOIN researcher_production rp_prod ON rp_prod.researcher_id = rp.researcher_id'
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL += ' AND rp_prod.city = ANY(%(city)s)'
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL += ' AND rp_prod.great_area_ && %(area)s'

    if filters.modality:
        DISTINCT_SQL = 'DISTINCT'
        join_foment = 'INNER JOIN foment f ON f.researcher_id = rp.researcher_id'
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL += ' AND f.modality_name = ANY(%(modality)s)'

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL += ' AND r.graduation = ANY(%(graduation)s)'

    if filters.type:  # Mapeado para o campo 'nature' do projeto
        PARAMS['type'] = filters.type.split(';')
        FILTERS_SQL += ' AND rp.nature = ANY(%(type)s)'
    if filters.institution_id:
        PARAMS['institution_id'] = filters.institution_id
        FILTERS_SQL += ' AND r.institution_id = %(institution_id)s'

    if filters.page and filters.lenght:
        FILTER_PAGINATION = tools.pagination(filters.page, filters.lenght)

    if filters.distinct == '1':
        DISTINCT_SQL = 'DISTINCT ON (rp.project_name)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
            rp.id, rp.researcher_id, r.name, rp.start_year, rp.end_year,
            rp.agency_code, rp.agency_name, rp.project_name, rp.status,
            rp.nature, rp.number_undergraduates, rp.number_specialists,
            rp.number_academic_masters, rp.number_phd, rp.description,
            rpp.production, rpf.foment, rpc.components, rp.stars
        FROM public.research_project rp
            LEFT JOIN researcher r ON r.id = rp.researcher_id
            LEFT JOIN (SELECT project_id, JSONB_AGG(JSONB_BUILD_OBJECT('title', title, 'type', type)) AS production
                FROM public.research_project_production GROUP BY project_id) rpp ON rpp.project_id = rp.id
            LEFT JOIN (SELECT project_id, JSONB_AGG(JSONB_BUILD_OBJECT('agency_name', agency_name, 'agency_code', agency_code, 'nature', nature)) AS foment
                FROM public.research_project_foment GROUP BY project_id) rpf ON rpf.project_id = rp.id
            LEFT JOIN (SELECT project_id, JSONB_AGG(JSONB_BUILD_OBJECT('name', name, 'lattes_id', lattes_id, 'citations', citations)) AS components
                FROM public.research_project_components GROUP BY project_id) rpc ON rpc.project_id = rp.id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
            {join_group}
        WHERE 1 = 1
            {FILTERS_SQL}
            {FILTER_TERMS}
            {FILTER_YEAR}
        ORDER BY {'rp.project_name, rp.start_year DESC' if DISTINCT_SQL else 'rp.start_year DESC'}
        {FILTER_PAGINATION};
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def list_papers_magazine(conn, filters):
    PARAMS = {}
    DISTINCT_SQL = ''
    FILTERS_SQL = "AND bp.type = 'TEXT_IN_NEWSPAPER_MAGAZINE'"
    FILTER_PAGINATION = ''
    FILTER_TERMS = ''
    FILTER_YEAR = ''

    join_researcher_production = ''
    join_foment = ''
    join_program = ''
    join_institution = ''
    join_departament = ''
    join_group = ''
    if filters.star:
        PARAMS['star'] = filters.star
        FILTERS_SQL += ' AND bp.id = ANY(%(star)s)'

    if filters.collection_id:
        PARAMS['collection_id'] = filters.collection_id
        FILTERS_SQL += ' AND bp.id = ANY(%(collection_id)s)'

    if filters.term:
        FILTER_TERMS, term_params = tools.websearch_filter(
            'bp.title', filters.term
        )
        PARAMS.update(term_params)

    if filters.year:
        PARAMS['year'] = filters.year
        FILTER_YEAR = 'AND bp.year_::INT >= %(year)s'

    if filters.dep_id or filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = bp.researcher_id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id
            FILTERS_SQL += ' AND dp.dep_id = %(dep_id)s'
        if filters.departament:
            PARAMS['departament'] = filters.departament.split(';')
            FILTERS_SQL += ' AND dp.dep_nom = ANY(%(departament)s)'

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL += ' AND bp.researcher_id = %(researcher_id)s'

    if filters.lattes_id:
        PARAMS['lattes_id'] = filters.lattes_id
        FILTERS_SQL += ' AND r.lattes_id = %(lattes_id)s'

    if filters.institution:
        join_institution = 'INNER JOIN institution i ON r.institution_id = i.id'
        PARAMS['institution'] = filters.institution.split(';')
        FILTERS_SQL += ' AND i.name = ANY(%(institution)s)'

    if filters.graduate_program_id or filters.graduate_program:
        DISTINCT_SQL = 'DISTINCT'
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        if filters.graduate_program_id:
            PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL += (
                ' AND gpr.graduate_program_id = %(graduate_program_id)s'
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL += ' AND gp.name = ANY(%(graduate_program)s)'

    if filters.group_id or filters.group:
        join_group = """
            INNER JOIN research_group_researcher rgr ON rgr.researcher_id = r.id
            INNER JOIN research_group rg ON rg.id = rgr.research_group_id
        """
        if filters.group_id:
            PARAMS['group_id'] = filters.group_id
            FILTERS_SQL += ' AND rgr.research_group_id = %(group_id)s'
        if filters.group:
            PARAMS['group'] = filters.group.split(';')
            FILTERS_SQL += ' AND rg.name = ANY(%(group)s)'

    if filters.city or filters.area:
        join_researcher_production = 'LEFT JOIN researcher_production rp ON rp.researcher_id = bp.researcher_id'
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL += ' AND rp.city = ANY(%(city)s)'
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL += ' AND rp.great_area_ && %(area)s'

    if filters.modality:
        DISTINCT_SQL = 'DISTINCT'
        join_foment = 'INNER JOIN foment f ON f.researcher_id = bp.researcher_id'
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL += ' AND f.modality_name = ANY(%(modality)s)'

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL += ' AND r.graduation = ANY(%(graduation)s)'

    if filters.page and filters.lenght:
        FILTER_PAGINATION = tools.pagination(filters.page, filters.lenght)

    if filters.distinct == '1':
        DISTINCT_SQL = 'DISTINCT ON (bp.title)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
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
            {join_group}
        WHERE 1 = 1
            {FILTERS_SQL}
            {FILTER_TERMS}
            {FILTER_YEAR}
        ORDER BY {'bp.title, bp.year_ DESC' if DISTINCT_SQL else 'bp.year_ DESC'}
        {FILTER_PAGINATION};
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


# ---


async def get_book_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    """
    Calcula as métricas de produção de livros, com filtros normalizados.
    """
    PARAMS = {}
    JOINS = []
    FILTERS_SQL = []

    # Inicia com LEFT JOIN para garantir que pesquisadores sem produção sejam contados se necessário
    # (embora a lógica atual de COUNT(bp.title) os exclua)
    JOINS.append(
        "LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id AND bp.type = 'BOOK'"
    )

    use_distinct = False

    if filters.dep_id or filters.departament:
        use_distinct = True
        JOINS.append("""
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """)
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id.split(';')
            FILTERS_SQL.append('dp.dep_id = ANY(%(dep_id)s)')
        if filters.departament:
            PARAMS['dep'] = filters.departament.split(';')
            FILTERS_SQL.append('dp.dep_nom = ANY(%(dep)s)')

    if filters.graduate_program_id or filters.graduate_program:
        use_distinct = True
        JOINS.append("""
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """)
        if filters.graduate_program_id:
            PARAMS['program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL.append(
                "gpr.graduate_program_id = %(program_id)s AND gpr.type_ = 'PERMANENTE'"
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL.append(
                "gp.name = ANY(%(graduate_program)s) AND gpr.type_ = 'PERMANENTE'"
            )

    if filters.institution:
        PARAMS['institution'] = filters.institution.split(';')
        # JOIN para filtrar pelo nome da instituição
        JOINS.append('INNER JOIN institution i ON r.institution_id = i.id')
        FILTERS_SQL.append('i.name = ANY(%(institution)s)')

    if filters.area or filters.city:
        # Um único JOIN para a tabela researcher_production
        JOINS.append(
            'LEFT JOIN researcher_production rp ON rp.researcher_id = r.id'
        )
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL.append(
                "STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s"
            )
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL.append('rp.city = ANY(%(city)s)')

    if filters.modality:
        JOINS.append('INNER JOIN foment f ON f.researcher_id = r.id')
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL.append('f.modality_name = ANY(%(modality)s)')

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL.append('r.graduation = ANY(%(graduation)s)')

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL.append('bp.researcher_id = %(researcher_id)s')

    if filters.year:
        PARAMS['year'] = filters.year
        FILTERS_SQL.append('bp.year::int >= %(year)s')

    # Filtro direto na tabela researcher (r), que é a forma correta.
    if filters.institution_id:
        PARAMS['institution_id'] = filters.institution_id
        FILTERS_SQL.append('r.institution_id = %(institution_id)s')

    term_filter = ''
    if filters.term:
        term_filter, term_params = tools.websearch_filter(
            'bp.title', filters.term
        )
        PARAMS.update(term_params)

    # Constrói as cláusulas finais
    join_clauses = '\n'.join(JOINS)
    where_clauses = ' AND '.join(FILTERS_SQL) if FILTERS_SQL else '1=1'

    filter_distinct = (
        'DISTINCT' if use_distinct or filters.distinct == '1' else ''
    )

    SCRIPT_SQL = f"""
        SELECT 
            bp.year, 
            COUNT({filter_distinct} bp.title) AS among
        FROM 
            researcher r
            {join_clauses}
        WHERE 
            bp.year IS NOT NULL
            {term_filter}
            AND {where_clauses}
        GROUP BY
            bp.year
        ORDER BY
            bp.year ASC;
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def get_book_chapter_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    PARAMS = {}
    JOINS = []
    FILTERS_SQL = []

    JOINS.append(
        "LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id AND bp.type = 'BOOK_CHAPTER'"
    )

    use_distinct = False

    if filters.dep_id or filters.departament:
        use_distinct = True
        JOINS.append("""
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """)
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id.split(';')
            FILTERS_SQL.append('dp.dep_id = ANY(%(dep_id)s)')
        if filters.departament:
            PARAMS['dep'] = filters.departament.split(';')
            FILTERS_SQL.append('dp.dep_nom = ANY(%(dep)s)')

    if filters.graduate_program_id or filters.graduate_program:
        use_distinct = True
        JOINS.append("""
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """)
        if filters.graduate_program_id:
            PARAMS['program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL.append(
                "gpr.graduate_program_id = %(program_id)s AND gpr.type_ = 'PERMANENTE'"
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL.append(
                "gp.name = ANY(%(graduate_program)s) AND gpr.type_ = 'PERMANENTE'"
            )

    if filters.institution:
        PARAMS['institution'] = filters.institution.split(';')
        JOINS.append('INNER JOIN institution i ON r.institution_id = i.id')
        FILTERS_SQL.append('i.name = ANY(%(institution)s)')

    if filters.area or filters.city:
        JOINS.append(
            'LEFT JOIN researcher_production rp ON rp.researcher_id = r.id'
        )
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL.append(
                "STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s"
            )
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL.append('rp.city = ANY(%(city)s)')

    if filters.modality:
        JOINS.append('INNER JOIN foment f ON f.researcher_id = r.id')
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL.append('f.modality_name = ANY(%(modality)s)')

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL.append('r.graduation = ANY(%(graduation)s)')

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL.append('bp.researcher_id = %(researcher_id)s')

    if filters.year:
        PARAMS['year'] = filters.year
        FILTERS_SQL.append('bp.year::int >= %(year)s')

    if filters.institution_id:
        PARAMS['institution_id'] = filters.institution_id
        FILTERS_SQL.append('r.institution_id = %(institution_id)s')

    term_filter = ''
    if filters.term:
        term_filter, term_params = tools.websearch_filter(
            'bp.title', filters.term
        )
        PARAMS.update(term_params)

    join_clauses = '\n'.join(JOINS)
    where_clauses = ' AND '.join(FILTERS_SQL) if FILTERS_SQL else '1=1'

    filter_distinct = (
        'DISTINCT' if use_distinct or filters.distinct == '1' else ''
    )

    SCRIPT_SQL = f"""
        SELECT 
            bp.year, 
            COUNT({filter_distinct} bp.title) AS among
        FROM 
            researcher r
            {join_clauses}
        WHERE 
            bp.year IS NOT NULL
            {term_filter}
            AND {where_clauses}
        GROUP BY 
            bp.year
        ORDER BY 
            bp.year ASC;
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def get_researcher_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    """
    [JÁ NORMALIZADA]
    Calcula métricas gerais sobre pesquisadores.
    """
    PARAMS = {}
    JOINS = ['LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id']
    FILTERS_SQL = []

    count_among = 'COUNT(DISTINCT r.id) AS among'

    if filters.type:
        match filters.type:
            case 'ABSTRACT':
                if filters.term:
                    term_filter, term_params = tools.websearch_filter(
                        'r.abstract', filters.term
                    )
                    FILTERS_SQL.append(term_filter.replace('WHERE', '').strip())
                    PARAMS.update(term_params)
            case (
                'BOOK'
                | 'BOOK_CHAPTER'
                | 'ARTICLE'
                | 'WORK_IN_EVENT'
                | 'TEXT_IN_NEWSPAPER_MAGAZINE'
            ):
                count_among = 'COUNT(DISTINCT bp.title) AS among'
                JOINS.append(
                    f"INNER JOIN bibliographic_production bp ON bp.researcher_id = r.id AND bp.type = '{filters.type}'"
                )
                if filters.term:
                    term_filter, term_params = tools.websearch_filter(
                        'bp.title', filters.term
                    )
                    FILTERS_SQL.append(term_filter.replace('WHERE', '').strip())
                    PARAMS.update(term_params)
                if filters.year:
                    PARAMS['year'] = filters.year
                    FILTERS_SQL.append('bp.year::int >= %(year)s')
            case 'PATENT':
                count_among = 'COUNT(DISTINCT p.title) AS among'
                JOINS.append('INNER JOIN patent p ON p.researcher_id = r.id')
                if filters.term:
                    term_filter, term_params = tools.websearch_filter(
                        'p.title', filters.term
                    )
                    FILTERS_SQL.append(term_filter.replace('WHERE', '').strip())
                    PARAMS.update(term_params)
                if filters.year:
                    PARAMS['year'] = filters.year
                    FILTERS_SQL.append('p.development_year::int >= %(year)s')
            case 'AREA':
                count_among = 'COUNT(DISTINCT r.id) AS among'
                # O join para filtro de área é adicionado mais abaixo de forma unificada
                if filters.term:
                    term_filter, term_params = tools.websearch_filter(
                        'rp_filter.great_area', filters.term
                    )
                    FILTERS_SQL.append(term_filter.replace('WHERE', '').strip())
                    PARAMS.update(term_params)
            case 'EVENT':
                count_among = 'COUNT(DISTINCT e.title) AS among'
                JOINS.append(
                    'INNER JOIN event_organization e ON e.researcher_id = r.id'
                )
                if filters.term:
                    term_filter, term_params = tools.websearch_filter(
                        'e.title', filters.term
                    )
                    FILTERS_SQL.append(term_filter.replace('WHERE', '').strip())
                    PARAMS.update(term_params)
                if filters.year:
                    PARAMS['year'] = filters.year
                    FILTERS_SQL.append('e.year::int >= %(year)s')
            case 'NAME':
                if filters.term:
                    name_filter, name_params = tools.names_filter(
                        'r.name', filters.term
                    )
                    FILTERS_SQL.append(name_filter.replace('WHERE', '').strip())
                    PARAMS.update(name_params)

    if filters.dep_id or filters.departament:
        JOINS.append(tools.JOIN_DEPARTAMENT)
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id.split(';')
            FILTERS_SQL.append('dp.dep_id = ANY(%(dep_id)s)')
        if filters.departament:
            PARAMS['departament'] = filters.departament.split(';')
            FILTERS_SQL.append('dp.dep_nom = ANY(%(departament)s)')

    if filters.graduate_program_id or filters.graduate_program:
        JOINS.append(tools.JOIN_PROGRAM)
        if filters.graduate_program_id:
            PARAMS['program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL.append(
                "gpr.graduate_program_id = %(program_id)s AND gpr.type_ = 'PERMANENTE'"
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL.append(
                "gp.name = ANY(%(graduate_program)s) AND gpr.type_ = 'PERMANENTE'"
            )

    if filters.institution:
        PARAMS['institution'] = filters.institution.split(';')
        JOINS.append('INNER JOIN institution i ON r.institution_id = i.id')
        FILTERS_SQL.append('i.name = ANY(%(institution)s)')

    if filters.area or filters.city or (filters.type == 'AREA' and filters.term):
        JOINS.append(
            'INNER JOIN researcher_production rp_filter ON rp_filter.researcher_id = r.id'
        )
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL.append('rp_filter.city = ANY(%(city)s)')
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL.append(
                "STRING_TO_ARRAY(REPLACE(rp_filter.great_area, ' ', '_'), ';') && %(area)s"
            )

    if filters.modality:
        JOINS.append(tools.JOIN_FOMENT)
        if filters.modality != '*':
            PARAMS['modality'] = filters.modality.split(';')
            FILTERS_SQL.append('f.modality_name = ANY(%(modality)s)')

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL.append('r.graduation = ANY(%(graduation)s)')

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL.append('r.id = %(researcher_id)s')

    if filters.institution_id:
        PARAMS['institution_id'] = filters.institution_id
        FILTERS_SQL.append('r.institution_id = %(institution_id)s')

    unique_joins = sorted(list(set(JOINS)))
    join_clauses = '\n'.join(unique_joins)
    where_clauses = ' AND '.join(FILTERS_SQL) if FILTERS_SQL else '1=1'

    SCRIPT_SQL = f"""
        SELECT 
            COUNT(DISTINCT r.id) AS researcher_count,
            COUNT(DISTINCT r.orcid) AS orcid_count,
            COUNT(DISTINCT opr.scopus) AS scopus_count,
            {count_among}
        FROM 
            researcher r
            {join_clauses}
        WHERE 
            {where_clauses};
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def list_article_metrics(
    conn: Connection,
    qualis: str,
    filters: DefaultFilters,
):
    """
    [NORMALIZADA]
    Lista métricas de artigos com filtros unificados.
    """
    PARAMS = {}
    JOINS = [
        'LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id',
        'INNER JOIN bibliographic_production_article bpa ON bpa.bibliographic_production_id = bp.id',
        'LEFT JOIN openalex_article opa ON opa.article_id = bp.id',
    ]
    FILTERS_SQL = []

    use_distinct = False

    if filters.dep_id or filters.departament:
        use_distinct = True
        JOINS.append("""
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """)
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id.split(';')
            FILTERS_SQL.append('dp.dep_id = ANY(%(dep_id)s)')
        if filters.departament:
            PARAMS['dep'] = filters.departament.split(';')
            FILTERS_SQL.append('dp.dep_nom = ANY(%(dep)s)')

    if filters.graduate_program_id or filters.graduate_program:
        use_distinct = True
        JOINS.append("""
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """)
        if filters.graduate_program_id:
            PARAMS['program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL.append(
                "gpr.graduate_program_id = %(program_id)s AND gpr.type_ = 'PERMANENTE'"
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL.append(
                "gp.name = ANY(%(graduate_program)s) AND gpr.type_ = 'PERMANENTE'"
            )

    if filters.institution:
        PARAMS['institution'] = filters.institution.split(';')
        JOINS.append('INNER JOIN institution i ON r.institution_id = i.id')
        FILTERS_SQL.append('i.name = ANY(%(institution)s)')

    if filters.city or filters.area:
        JOINS.append(
            'LEFT JOIN researcher_production rp ON rp.researcher_id = r.id'
        )
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL.append('rp.city = ANY(%(city)s)')
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            # A query original usava '=', o que parece incorreto para área. Corrigido para '&&'.
            FILTERS_SQL.append(
                "STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s"
            )

    if filters.modality:
        JOINS.append('INNER JOIN foment f ON f.researcher_id = r.id')
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL.append('f.modality_name = ANY(%(modality)s)')

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL.append('r.graduation = ANY(%(graduation)s)')

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL.append('bp.researcher_id = %(researcher_id)s')

    if filters.year:
        PARAMS['year'] = filters.year
        FILTERS_SQL.append('bp.year::int >= %(year)s')

    if filters.institution_id:
        PARAMS['institution_id'] = filters.institution_id
        FILTERS_SQL.append('r.institution_id = %(institution_id)s')

    if qualis:
        PARAMS['qualis'] = qualis.split(';')
        FILTERS_SQL.append('bpa.qualis = ANY(%(qualis)s)')

    term_filter = ''
    if filters.term:
        term_filter, term_params = tools.websearch_filter(
            'bp.title', filters.term
        )
        PARAMS.update(term_params)

    join_clauses = '\n'.join(sorted(list(set(JOINS))))
    where_clauses = ' AND '.join(FILTERS_SQL) if FILTERS_SQL else '1=1'
    filter_distinct = (
        'DISTINCT' if use_distinct or filters.distinct == '1' else ''
    )

    SCRIPT_SQL = f"""
        SELECT 
            bp.year, 
            SUM(opa.citations_count) AS citations,
            ARRAY_AGG(bpa.qualis) AS qualis, 
            ARRAY_AGG(bpa.jcr) AS jcr,
            COUNT({filter_distinct} bp.title) AS among, 
            COUNT(DISTINCT bp.doi) AS count_doi
        FROM 
            researcher r
            {join_clauses}
        WHERE
            bp.year IS NOT NULL
            {term_filter}
            AND {where_clauses}
        GROUP BY
            bp.year
        ORDER BY
            bp.year ASC;
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def list_patent_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    """
    [NORMALIZADA]
    Lista métricas de patentes com filtros unificados e corrigidos.
    A tabela researcher (r) é juntada apenas quando necessário.
    """
    PARAMS = {}
    JOINS = []
    FILTERS_SQL = []

    needs_researcher_join = any([
        filters.institution,
        filters.institution_id,
        filters.graduation,
        filters.dep_id,
        filters.departament,
        filters.graduate_program_id,
        filters.graduate_program,
        filters.modality,
    ])

    if needs_researcher_join:
        # Adiciona o JOIN com a tabela researcher se qualquer filtro dependente dela for usado
        JOINS.append('INNER JOIN researcher r ON r.id = p.researcher_id')

    if filters.dep_id or filters.departament:
        JOINS.append("""
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """)
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id.split(';')
            FILTERS_SQL.append('dp.dep_id = ANY(%(dep_id)s)')
        if filters.departament:
            PARAMS['dep'] = filters.departament.split(';')
            FILTERS_SQL.append('dp.dep_nom = ANY(%(dep)s)')

    if filters.graduate_program_id or filters.graduate_program:
        JOINS.append("""
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """)
        if filters.graduate_program_id:
            PARAMS['program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL.append(
                "gpr.graduate_program_id = %(program_id)s AND gpr.type_ = 'PERMANENTE'"
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL.append(
                "gp.name = ANY(%(graduate_program)s) AND gpr.type_ = 'PERMANENTE'"
            )

    if filters.institution:
        PARAMS['institution'] = filters.institution.split(';')
        JOINS.append('INNER JOIN institution i ON r.institution_id = i.id')
        FILTERS_SQL.append('i.name = ANY(%(institution)s)')

    if filters.city or filters.area:
        # Este join não depende da tabela researcher, pode ser feito direto com p.researcher_id
        JOINS.append(
            'LEFT JOIN researcher_production rp ON rp.researcher_id = p.researcher_id'
        )
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL.append('rp.city = ANY(%(city)s)')
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL.append(
                "STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s"
            )

    if filters.modality:
        JOINS.append('INNER JOIN foment f ON f.researcher_id = r.id')
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL.append('f.modality_name = ANY(%(modality)s)')

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL.append('r.graduation = ANY(%(graduation)s)')

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL.append('p.researcher_id = %(researcher_id)s')

    if filters.year:
        PARAMS['year'] = filters.year
        FILTERS_SQL.append('p.development_year::INT >= %(year)s')

    if filters.institution_id:
        PARAMS['institution_id'] = filters.institution_id
        FILTERS_SQL.append('r.institution_id = %(institution_id)s')

    term_filter = ''
    if filters.term:
        term_filter, term_params = tools.websearch_filter(
            'p.title', filters.term
        )
        PARAMS.update(term_params)

    join_clauses = '\n'.join(sorted(list(set(JOINS))))
    where_clauses = ' AND '.join(FILTERS_SQL) if FILTERS_SQL else '1=1'
    filter_distinct = 'DISTINCT' if filters.distinct == '1' else ''

    SCRIPT_SQL = f"""
        SELECT 
            p.development_year AS year,
            COUNT({filter_distinct} p.title) FILTER (WHERE p.grant_date IS NULL) AS NOT_GRANTED,
            COUNT({filter_distinct} p.title) FILTER (WHERE p.grant_date IS NOT NULL) AS GRANTED
        FROM 
            patent p
            {join_clauses}
        WHERE 
            p.development_year IS NOT NULL
            {term_filter}
            AND {where_clauses}
        GROUP BY 
            p.development_year
        ORDER BY 
            p.development_year ASC;
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def list_guidance_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    """
    [NORMALIZADA]
    Lista métricas de orientação com filtros unificados.
    """
    PARAMS = {}
    JOINS = []
    FILTERS_SQL = []

    # Determina se o JOIN com a tabela 'researcher' é necessário
    needs_researcher_join = any([
        filters.institution,
        filters.institution_id,
        filters.graduation,
        filters.dep_id,
        filters.departament,
        filters.graduate_program_id,
        filters.graduate_program,
        filters.modality,
    ])

    if needs_researcher_join:
        JOINS.append('INNER JOIN researcher r ON r.id = g.researcher_id')

    if filters.dep_id or filters.departament:
        JOINS.append("""
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """)
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id.split(';')
            FILTERS_SQL.append('dp.dep_id = ANY(%(dep_id)s)')
        if filters.departament:
            PARAMS['dep'] = filters.departament.split(';')
            FILTERS_SQL.append('dp.dep_nom = ANY(%(dep)s)')

    if filters.graduate_program_id or filters.graduate_program:
        JOINS.append("""
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """)
        if filters.graduate_program_id:
            PARAMS['program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL.append(
                "gpr.graduate_program_id = %(program_id)s AND gpr.type_ = 'PERMANENTE'"
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL.append(
                "gp.name = ANY(%(graduate_program)s) AND gpr.type_ = 'PERMANENTE'"
            )

    if filters.institution:
        PARAMS['institution'] = filters.institution.split(';')
        JOINS.append('INNER JOIN institution i ON r.institution_id = i.id')
        FILTERS_SQL.append('i.name = ANY(%(institution)s)')

    if filters.city or filters.area:
        JOINS.append(
            'LEFT JOIN researcher_production rp ON rp.researcher_id = g.researcher_id'
        )
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL.append('rp.city = ANY(%(city)s)')
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL.append(
                "STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s"
            )

    if filters.modality:
        JOINS.append('INNER JOIN foment f ON f.researcher_id = r.id')
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL.append('f.modality_name = ANY(%(modality)s)')

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL.append('r.graduation = ANY(%(graduation)s)')

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL.append('g.researcher_id = %(researcher_id)s')

    if filters.year:
        PARAMS['year'] = filters.year
        FILTERS_SQL.append('g.year::int >= %(year)s::int')

    if filters.institution_id:
        PARAMS['institution_id'] = filters.institution_id
        FILTERS_SQL.append('r.institution_id = %(institution_id)s')

    term_filter = ''
    if filters.term:
        term_filter, term_params = tools.websearch_filter(
            'g.title', filters.term
        )
        PARAMS.update(term_params)

    use_distinct = any([
        filters.distinct == '1',
        filters.departament,
        filters.dep_id,
        filters.graduate_program,
    ])
    filter_distinct = (
        'DISTINCT ON (g.oriented, g.nature, g.year)' if use_distinct else ''
    )

    join_clauses = '\n'.join(sorted(list(set(JOINS))))
    where_clauses = ' AND '.join(FILTERS_SQL) if FILTERS_SQL else '1=1'

    SCRIPT_SQL = f"""
        WITH Orientacoes AS (
            SELECT {filter_distinct}
                g.year AS year,
                unaccent(lower((g.nature || ' ' || g.status))) AS nature,
                g.oriented AS oriented
            FROM
                guidance g
                {join_clauses}
            WHERE
                {where_clauses}
                {term_filter}
            ORDER BY 
                g.oriented, g.nature, g.year
        )
        SELECT
            year,
            nature,
            COUNT(oriented) AS count_nature
        FROM
            Orientacoes
        GROUP BY
            year,
            nature
        ORDER BY
            year ASC;
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def list_academic_degree_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    """
    [NORMALIZADA]
    Lista métricas de formação acadêmica com filtros unificados.
    """
    PARAMS = {}
    JOINS = []
    FILTERS_SQL = []

    needs_researcher_join = any([
        filters.institution,
        filters.institution_id,
        filters.graduation,
        filters.dep_id,
        filters.departament,
        filters.graduate_program_id,
        filters.graduate_program,
        filters.modality,
    ])

    if needs_researcher_join:
        JOINS.append('INNER JOIN researcher r ON r.id = e.researcher_id')

    if filters.dep_id or filters.departament:
        JOINS.append("""
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """)
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id.split(';')
            FILTERS_SQL.append('dp.dep_id = ANY(%(dep_id)s)')
        if filters.departament:
            PARAMS['dep'] = filters.departament.split(';')
            FILTERS_SQL.append('dp.dep_nom = ANY(%(dep)s)')

    if filters.graduate_program_id or filters.graduate_program:
        JOINS.append("""
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """)
        if filters.graduate_program_id:
            PARAMS['program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL.append(
                "gpr.graduate_program_id = %(program_id)s AND gpr.type_ = 'PERMANENTE'"
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL.append(
                "gp.name = ANY(%(graduate_program)s) AND gpr.type_ = 'PERMANENTE'"
            )

    if filters.institution:
        PARAMS['institution'] = filters.institution.split(';')
        JOINS.append('INNER JOIN institution i ON r.institution_id = i.id')
        FILTERS_SQL.append('i.name = ANY(%(institution)s)')

    if filters.city or filters.area:
        JOINS.append(
            'LEFT JOIN researcher_production rp ON rp.researcher_id = e.researcher_id'
        )
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL.append('rp.city = ANY(%(city)s)')
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL.append(
                "STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s"
            )

    if filters.modality:
        JOINS.append('INNER JOIN foment f ON f.researcher_id = r.id')
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL.append('f.modality_name = ANY(%(modality)s)')

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL.append('r.graduation = ANY(%(graduation)s)')

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL.append('e.researcher_id = %(researcher_id)s')

    if filters.year:
        PARAMS['year'] = filters.year
        FILTERS_SQL.append(
            '(e.education_start::int >= %(year)s::int OR e.education_end::int >= %(year)s::int)'
        )

    if filters.institution_id:
        PARAMS['institution_id'] = filters.institution_id
        FILTERS_SQL.append('r.institution_id = %(institution_id)s')

    join_clauses = '\n'.join(sorted(list(set(JOINS))))
    where_clauses = ' AND '.join(FILTERS_SQL) if FILTERS_SQL else '1=1'

    SCRIPT_SQL = f"""
        WITH EducacaoFiltrada AS (
            SELECT
                e.education_start,
                e.education_end,
                e.degree
            FROM
                education e
                {join_clauses}
            WHERE
                {where_clauses}
        ),
        EducacaoUnificada AS (
            SELECT education_start AS year, degree, 'START' AS event_type FROM EducacaoFiltrada
            WHERE education_start IS NOT NULL
            UNION ALL
            SELECT education_end AS year, degree, 'END' AS event_type FROM EducacaoFiltrada
            WHERE education_end IS NOT NULL
        )
        SELECT
            year,
            COUNT(degree) AS among,
            REPLACE(degree || '-' || event_type, '-', '_') AS degree
        FROM EducacaoUnificada
        GROUP BY year, degree, event_type
        ORDER BY year, degree;
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def list_software_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    """
    [NORMALIZADA]
    Lista métricas de software com filtros unificados.
    """
    PARAMS = {}
    JOINS = []
    FILTERS_SQL = []

    needs_researcher_join = any([
        filters.institution,
        filters.institution_id,
        filters.graduation,
        filters.dep_id,
        filters.departament,
        filters.graduate_program_id,
        filters.graduate_program,
        filters.modality,
    ])

    if needs_researcher_join:
        JOINS.append('INNER JOIN researcher r ON r.id = s.researcher_id')

    if filters.dep_id or filters.departament:
        JOINS.append("""
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """)
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id.split(';')
            FILTERS_SQL.append('dp.dep_id = ANY(%(dep_id)s)')
        if filters.departament:
            PARAMS['dep'] = filters.departament.split(';')
            FILTERS_SQL.append('dp.dep_nom = ANY(%(dep)s)')

    if filters.graduate_program_id or filters.graduate_program:
        JOINS.append("""
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """)
        if filters.graduate_program_id:
            PARAMS['program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL.append(
                "gpr.graduate_program_id = %(program_id)s AND gpr.type_ = 'PERMANENTE'"
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL.append(
                "gp.name = ANY(%(graduate_program)s) AND gpr.type_ = 'PERMANENTE'"
            )

    if filters.institution:
        PARAMS['institution'] = filters.institution.split(';')
        JOINS.append('INNER JOIN institution i ON r.institution_id = i.id')
        FILTERS_SQL.append('i.name = ANY(%(institution)s)')

    if filters.city or filters.area:
        JOINS.append(
            'LEFT JOIN researcher_production rp ON rp.researcher_id = s.researcher_id'
        )
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL.append('rp.city = ANY(%(city)s)')
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL.append(
                "STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s"
            )

    if filters.modality:
        JOINS.append('INNER JOIN foment f ON f.researcher_id = r.id')
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL.append('f.modality_name = ANY(%(modality)s)')

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL.append('r.graduation = ANY(%(graduation)s)')

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL.append('s.researcher_id = %(researcher_id)s')

    if filters.year:
        PARAMS['year'] = filters.year
        FILTERS_SQL.append('s.year::int >= %(year)s::int')

    if filters.institution_id:
        PARAMS['institution_id'] = filters.institution_id
        FILTERS_SQL.append('r.institution_id = %(institution_id)s')

    term_filter = ''
    if filters.term:
        term_filter, term_params = tools.websearch_filter(
            's.title', filters.term
        )
        PARAMS.update(term_params)

    use_distinct = any([
        filters.distinct == '1',
        filters.departament,
        filters.dep_id,
    ])
    filter_distinct = 'DISTINCT ON (s.title)' if use_distinct else ''

    join_clauses = '\n'.join(sorted(list(set(JOINS))))
    where_clauses = ' AND '.join(FILTERS_SQL) if FILTERS_SQL else '1=1'

    SCRIPT_SQL = f"""
        WITH FilteredSoftware AS (
            SELECT {filter_distinct}
                s.year,
                s.title
            FROM
                public.software s
                {join_clauses}
            WHERE
                {where_clauses}
                {term_filter}
            ORDER BY 
                s.title, s.year
        )
        SELECT
            fs.year,
            COUNT(fs.title) AS among
        FROM
            FilteredSoftware fs
        GROUP BY
            fs.year
        ORDER BY
            fs.year ASC;
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def get_research_report_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    """
    [NORMALIZADA]
    Obtém métricas de relatórios de pesquisa com filtros unificados.
    """
    PARAMS = {}
    JOINS = []
    FILTERS_SQL = []

    # O alias da tabela principal é 'rr' (research_report)
    main_alias = 'rr'

    needs_researcher_join = any([
        filters.institution,
        filters.institution_id,
        filters.graduation,
        filters.dep_id,
        filters.departament,
        filters.graduate_program_id,
        filters.graduate_program,
        filters.modality,
    ])

    if needs_researcher_join:
        JOINS.append(
            f'INNER JOIN researcher r ON r.id = {main_alias}.researcher_id'
        )

    if filters.dep_id or filters.departament:
        JOINS.append(f"""
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = {main_alias}.researcher_id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """)
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id.split(';')
            FILTERS_SQL.append('dp.dep_id = ANY(%(dep_id)s)')
        if filters.departament:
            PARAMS['departament'] = filters.departament.split(';')
            FILTERS_SQL.append('dp.dep_nom = ANY(%(departament)s)')

    if filters.graduate_program_id or filters.graduate_program:
        JOINS.append(f"""
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = {main_alias}.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """)
        if filters.graduate_program_id:
            PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL.append(
                "gpr.graduate_program_id = %(graduate_program_id)s AND gpr.type_ = 'PERMANENTE'"
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL.append(
                "gp.name = ANY(%(graduate_program)s) AND gpr.type_ = 'PERMANENTE'"
            )

    if filters.institution:
        PARAMS['institution'] = filters.institution.split(';')
        JOINS.append('INNER JOIN institution i ON r.institution_id = i.id')
        FILTERS_SQL.append('i.name = ANY(%(institution)s)')

    if filters.city or filters.area:
        JOINS.append(
            f'LEFT JOIN researcher_production rp ON rp.researcher_id = {main_alias}.researcher_id'
        )
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL.append('rp.city = ANY(%(city)s)')
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL.append(
                "STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s"
            )

    if filters.modality:
        JOINS.append(
            f'INNER JOIN foment f ON f.researcher_id = {main_alias}.researcher_id'
        )
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL.append('f.modality_name = ANY(%(modality)s)')

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL.append('r.graduation = ANY(%(graduation)s)')

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL.append(f'{main_alias}.researcher_id = %(researcher_id)s')

    if filters.year:
        PARAMS['year'] = filters.year
        FILTERS_SQL.append(f'{main_alias}.year::int >= %(year)s::int')

    if filters.institution_id:
        PARAMS['institution_id'] = filters.institution_id
        FILTERS_SQL.append('r.institution_id = %(institution_id)s')

    term_filter = ''
    if filters.term:
        term_filter, term_params = tools.websearch_filter(
            f'{main_alias}.title', filters.term
        )
        PARAMS.update(term_params)

    use_distinct = any([
        filters.distinct == '1',
        filters.dep_id,
        filters.departament,
        filters.graduation,
    ])
    filter_distinct = f'DISTINCT ON ({main_alias}.title)' if use_distinct else ''

    join_clauses = '\n'.join(sorted(list(set(JOINS))))
    where_clauses = ' AND '.join(FILTERS_SQL) if FILTERS_SQL else '1=1'

    SCRIPT_SQL = f"""
        WITH FilteredResearchReport AS (
            SELECT {filter_distinct}
                {main_alias}.year,
                {main_alias}.title
            FROM
                public.research_report {main_alias}
                {join_clauses}
            WHERE
                {where_clauses}
                {term_filter.replace('WHERE', 'AND')}
            ORDER BY {main_alias}.title, {main_alias}.year
        )
        SELECT
            frr.year,
            COUNT(frr.title) AS among
        FROM
            FilteredResearchReport frr
        GROUP BY
            frr.year
        ORDER BY
            frr.year ASC;
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def get_papers_magazine_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    """
    [NORMALIZADA]
    Obtém métricas de artigos em jornais/revistas com filtros unificados.
    """
    PARAMS = {}
    JOINS = []
    FILTERS_SQL = []

    # O alias da tabela principal é 'bp' (bibliographic_production)
    main_alias = 'bp'

    needs_researcher_join = any([
        filters.institution,
        filters.institution_id,
        filters.graduation,
        filters.dep_id,
        filters.departament,
        filters.graduate_program_id,
        filters.graduate_program,
        filters.modality,
    ])

    if needs_researcher_join:
        JOINS.append(
            f'INNER JOIN researcher r ON r.id = {main_alias}.researcher_id'
        )

    if filters.dep_id or filters.departament:
        JOINS.append(f"""
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = {main_alias}.researcher_id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """)
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id.split(';')
            FILTERS_SQL.append('dp.dep_id = ANY(%(dep_id)s)')
        if filters.departament:
            PARAMS['departament'] = filters.departament.split(';')
            FILTERS_SQL.append('dp.dep_nom = ANY(%(departament)s)')

    if filters.graduate_program_id or filters.graduate_program:
        JOINS.append(f"""
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = {main_alias}.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """)
        if filters.graduate_program_id:
            PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL.append(
                "gpr.graduate_program_id = %(graduate_program_id)s AND gpr.type_ = 'PERMANENTE'"
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL.append(
                "gp.name = ANY(%(graduate_program)s) AND gpr.type_ = 'PERMANENTE'"
            )

    if filters.institution:
        PARAMS['institution'] = filters.institution.split(';')
        JOINS.append('INNER JOIN institution i ON r.institution_id = i.id')
        FILTERS_SQL.append('i.name = ANY(%(institution)s)')

    if filters.city or filters.area:
        JOINS.append(
            f'LEFT JOIN researcher_production rp ON rp.researcher_id = {main_alias}.researcher_id'
        )
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL.append('rp.city = ANY(%(city)s)')
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL.append(
                "STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s"
            )

    if filters.modality:
        JOINS.append(
            f'INNER JOIN foment f ON f.researcher_id = {main_alias}.researcher_id'
        )
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL.append('f.modality_name = ANY(%(modality)s)')

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL.append('r.graduation = ANY(%(graduation)s)')

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL.append(f'{main_alias}.researcher_id = %(researcher_id)s')

    if filters.year:
        PARAMS['year'] = filters.year
        FILTERS_SQL.append(f'{main_alias}.year_::int >= %(year)s::int')

    if filters.institution_id:
        PARAMS['institution_id'] = filters.institution_id
        FILTERS_SQL.append('r.institution_id = %(institution_id)s')

    term_filter = ''
    if filters.term:
        term_filter, term_params = tools.websearch_filter(
            f'{main_alias}.title', filters.term
        )
        PARAMS.update(term_params)

    use_distinct = any([
        filters.distinct == '1',
        filters.dep_id,
        filters.departament,
        filters.graduation,
    ])
    filter_distinct = f'DISTINCT ON ({main_alias}.title)' if use_distinct else ''

    join_clauses = '\n'.join(sorted(list(set(JOINS))))
    where_clauses = ' AND '.join(FILTERS_SQL) if FILTERS_SQL else '1=1'

    SCRIPT_SQL = f"""
        WITH FilteredPapers AS (
            SELECT {filter_distinct}
                {main_alias}.title,
                {main_alias}.year_ AS year
            FROM
                public.bibliographic_production {main_alias}
                {join_clauses}
            WHERE
                {main_alias}.type = 'TEXT_IN_NEWSPAPER_MAGAZINE'
                AND {where_clauses}
                {term_filter.replace('WHERE', 'AND')}
            ORDER BY {main_alias}.title, {main_alias}.year_
        )
        SELECT
            fp.year,
            COUNT(fp.title) as among
        FROM
            FilteredPapers fp
        GROUP BY
            fp.year
        ORDER BY
            fp.year ASC;
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def get_speaker_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    """
    [NORMALIZADA]
    Obtém métricas de participação em eventos (palestras) com filtros unificados.
    """
    PARAMS = {}
    JOINS = []
    FILTERS_SQL = []

    # O alias da tabela principal é 'pe' (participation_events)
    main_alias = 'pe'

    needs_researcher_join = any([
        filters.institution,
        filters.institution_id,
        filters.graduation,
        filters.dep_id,
        filters.departament,
        filters.graduate_program_id,
        filters.graduate_program,
        filters.modality,
    ])

    if needs_researcher_join:
        JOINS.append(
            f'INNER JOIN researcher r ON r.id = {main_alias}.researcher_id'
        )

    if filters.dep_id or filters.departament:
        JOINS.append(f"""
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = {main_alias}.researcher_id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """)
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id.split(';')
            FILTERS_SQL.append('dp.dep_id = ANY(%(dep_id)s)')
        if filters.departament:
            PARAMS['departament'] = filters.departament.split(';')
            FILTERS_SQL.append('dp.dep_nom = ANY(%(departament)s)')

    if filters.graduate_program_id or filters.graduate_program:
        JOINS.append(f"""
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = {main_alias}.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """)
        if filters.graduate_program_id:
            PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL.append(
                "gpr.graduate_program_id = %(graduate_program_id)s AND gpr.type_ = 'PERMANENTE'"
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL.append(
                "gp.name = ANY(%(graduate_program)s) AND gpr.type_ = 'PERMANENTE'"
            )

    if filters.institution:
        PARAMS['institution'] = filters.institution.split(';')
        JOINS.append('INNER JOIN institution i ON r.institution_id = i.id')
        FILTERS_SQL.append('i.name = ANY(%(institution)s)')

    if filters.city or filters.area:
        JOINS.append(
            f'LEFT JOIN researcher_production rp ON rp.researcher_id = {main_alias}.researcher_id'
        )
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL.append('rp.city = ANY(%(city)s)')
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL.append(
                "STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s"
            )

    if filters.modality:
        JOINS.append(
            f'INNER JOIN foment f ON f.researcher_id = {main_alias}.researcher_id'
        )
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL.append('f.modality_name = ANY(%(modality)s)')

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL.append('r.graduation = ANY(%(graduation)s)')

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL.append(f'{main_alias}.researcher_id = %(researcher_id)s')

    if filters.year:
        PARAMS['year'] = filters.year
        FILTERS_SQL.append(f'{main_alias}.year::int >= %(year)s::int')

    if filters.institution_id:
        PARAMS['institution_id'] = filters.institution_id
        FILTERS_SQL.append('r.institution_id = %(institution_id)s')

    term_filter = ''
    if filters.term:
        term_filter, term_params = tools.websearch_filter(
            f'{main_alias}.title', filters.term
        )
        PARAMS.update(term_params)

    use_distinct = any([
        filters.distinct == '1',
        filters.dep_id,
        filters.departament,
        filters.graduation,
    ])
    filter_distinct = f'DISTINCT ON ({main_alias}.title)' if use_distinct else ''

    join_clauses = '\n'.join(sorted(list(set(JOINS))))
    where_clauses = ' AND '.join(FILTERS_SQL) if FILTERS_SQL else '1=1'

    SCRIPT_SQL = f"""
        WITH FilteredEvents AS (
            SELECT {filter_distinct}
                {main_alias}.title,
                {main_alias}.year,
                {main_alias}.nature
            FROM
                public.participation_events {main_alias}
                {join_clauses}
            WHERE
                {where_clauses}
                {term_filter.replace('WHERE', 'AND')}
            ORDER BY {main_alias}.title, {main_alias}.year
        )
        SELECT fe.year,
            SUM(CASE WHEN fe.nature = 'Congresso' THEN 1 ELSE 0 END) AS congress,
            SUM(CASE WHEN fe.nature = 'Encontro' THEN 1 ELSE 0 END) AS meeting,
            SUM(CASE WHEN fe.nature = 'Oficina' THEN 1 ELSE 0 END) AS workshop,
            SUM(CASE WHEN fe.nature = 'Outra' THEN 1 ELSE 0 END) AS other,
            SUM(CASE WHEN fe.nature = 'Seminário' THEN 1 ELSE 0 END) AS seminar,
            SUM(CASE WHEN fe.nature = 'Simpósio' THEN 1 ELSE 0 END) AS symposium
        FROM
            FilteredEvents fe
        GROUP BY
            fe.year
        ORDER BY
            fe.year ASC;
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


# Supondo que 'tools' e 'DefaultFilters' estejam definidos em outro lugar.
# from ... import tools
# from ...models import DefaultFilters
# from asyncpg.connection import Connection


async def get_brand_metrics(
    conn: Connection,
    nature: str,
    filters: DefaultFilters,
):
    """
    [NORMALIZADA]
    Obtém métricas de marcas com filtros unificados.
    """
    PARAMS = {}
    JOINS = []
    FILTERS_SQL = []

    # O alias da tabela principal é 'b' (brand)
    main_alias = 'b'

    # Verifica se o JOIN com a tabela 'researcher' é necessário
    needs_researcher_join = any([
        filters.institution,
        filters.institution_id,
        filters.graduation,
        filters.dep_id,
        filters.departament,
        filters.graduate_program_id,
        filters.graduate_program,
        filters.modality,
        filters.city,
        filters.area,
    ])

    if needs_researcher_join:
        JOINS.append(
            f'INNER JOIN public.researcher r ON r.id = {main_alias}.researcher_id'
        )

    if nature:
        PARAMS['nature'] = nature.split(';')
        FILTERS_SQL.append(f'{main_alias}.nature = ANY(%(nature)s)')

    if filters.dep_id or filters.departament:
        JOINS.append("""
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """)
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id.split(';')
            FILTERS_SQL.append('dp.dep_id = ANY(%(dep_id)s)')
        if filters.departament:
            PARAMS['departament'] = filters.departament.split(';')
            FILTERS_SQL.append('dp.dep_nom = ANY(%(departament)s)')

    if filters.graduate_program_id or filters.graduate_program:
        JOINS.append("""
            INNER JOIN public.graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN public.graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """)
        if filters.graduate_program_id:
            PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL.append(
                "gpr.graduate_program_id = %(graduate_program_id)s AND gpr.type_ = 'PERMANENTE'"
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL.append(
                "gp.name = ANY(%(graduate_program)s) AND gpr.type_ = 'PERMANENTE'"
            )

    if filters.institution:
        PARAMS['institution'] = filters.institution.split(';')
        JOINS.append(
            'INNER JOIN public.institution i ON r.institution_id = i.id'
        )
        FILTERS_SQL.append('i.name = ANY(%(institution)s)')

    if filters.city or filters.area:
        JOINS.append(
            'LEFT JOIN public.researcher_production rp ON rp.researcher_id = r.id'
        )
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL.append('rp.city = ANY(%(city)s)')
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL.append(
                "STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s"
            )

    if filters.modality:
        JOINS.append('INNER JOIN public.foment f ON f.researcher_id = r.id')
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL.append('f.modality_name = ANY(%(modality)s)')

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL.append('r.graduation = ANY(%(graduation)s)')

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL.append(f'{main_alias}.researcher_id = %(researcher_id)s')

    if filters.year:
        PARAMS['year'] = filters.year
        FILTERS_SQL.append(f'{main_alias}.year::int >= %(year)s::int')

    if filters.institution_id:
        PARAMS['institution_id'] = filters.institution_id
        FILTERS_SQL.append('r.institution_id = %(institution_id)s')

    term_filter = ''
    if filters.term:
        term_filter, term_params = tools.websearch_filter(
            f'{main_alias}.title', filters.term
        )
        PARAMS.update(term_params)

    use_distinct = any([
        filters.distinct == '1',
        filters.dep_id,
        filters.departament,
        filters.institution,
        filters.graduate_program_id,
        filters.graduate_program,
        filters.modality,
        filters.graduation,
    ])
    filter_distinct = f'DISTINCT ON ({main_alias}.title)' if use_distinct else ''

    join_clauses = '\n'.join(sorted(list(set(JOINS))))
    where_clauses = ' AND '.join(FILTERS_SQL) if FILTERS_SQL else '1=1'

    SCRIPT_SQL = f"""
        WITH FilteredBrands AS (
            SELECT {filter_distinct}
                {main_alias}.title,
                {main_alias}.year
            FROM
                public.brand {main_alias}
                {join_clauses}
            WHERE
                {where_clauses}
                {term_filter.replace('WHERE', 'AND')}
            ORDER BY {main_alias}.title, {main_alias}.year
        )
        SELECT
            fb.year,
            COUNT(fb.title) AS among
        FROM
            FilteredBrands fb
        GROUP BY
            fb.year
        ORDER BY
            fb.year ASC;
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def get_research_project_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    """
    [NORMALIZADA]
    Obtém métricas de projetos de pesquisa com filtros unificados.
    """
    PARAMS = {}
    JOINS = []
    FILTERS_SQL = []

    # O alias da tabela principal é 'b' (research_project)
    main_alias = 'b'

    needs_researcher_join = any([
        filters.institution,
        filters.institution_id,
        filters.graduation,
        filters.dep_id,
        filters.departament,
        filters.graduate_program_id,
        filters.graduate_program,
        filters.modality,
        filters.city,
        filters.area,
    ])

    if needs_researcher_join:
        JOINS.append(
            f'INNER JOIN public.researcher r ON r.id = {main_alias}.researcher_id'
        )

    if filters.dep_id or filters.departament:
        JOINS.append("""
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """)
        if filters.dep_id:
            PARAMS['dep_id'] = filters.dep_id.split(';')
            FILTERS_SQL.append('dp.dep_id = ANY(%(dep_id)s)')
        if filters.departament:
            PARAMS['departament'] = filters.departament.split(';')
            FILTERS_SQL.append('dp.dep_nom = ANY(%(departament)s)')

    if filters.graduate_program_id or filters.graduate_program:
        JOINS.append("""
            INNER JOIN public.graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN public.graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """)
        if filters.graduate_program_id:
            PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL.append(
                "gpr.graduate_program_id = %(graduate_program_id)s AND gpr.type_ = 'PERMANENTE'"
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL.append(
                "gp.name = ANY(%(graduate_program)s) AND gpr.type_ = 'PERMANENTE'"
            )

    if filters.institution:
        PARAMS['institution'] = filters.institution.split(';')
        JOINS.append(
            'INNER JOIN public.institution i ON r.institution_id = i.id'
        )
        FILTERS_SQL.append('i.name = ANY(%(institution)s)')

    if filters.city or filters.area:
        JOINS.append(
            'LEFT JOIN public.researcher_production rp ON rp.researcher_id = r.id'
        )
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL.append('rp.city = ANY(%(city)s)')
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            FILTERS_SQL.append(
                "STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s"
            )

    if filters.modality:
        JOINS.append('INNER JOIN public.foment f ON f.researcher_id = r.id')
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL.append('f.modality_name = ANY(%(modality)s)')

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL.append('r.graduation = ANY(%(graduation)s)')

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL.append(f'{main_alias}.researcher_id = %(researcher_id)s')

    if filters.year:
        PARAMS['year'] = filters.year
        FILTERS_SQL.append(f'{main_alias}.start_year::int >= %(year)s::int')

    if filters.institution_id:
        PARAMS['institution_id'] = filters.institution_id
        FILTERS_SQL.append('r.institution_id = %(institution_id)s')

    term_filter = ''
    if filters.term:
        term_filter, term_params = tools.websearch_filter(
            f'{main_alias}.project_name', filters.term
        )
        PARAMS.update(term_params)

    use_distinct = any([
        filters.distinct == '1',
        filters.dep_id,
        filters.departament,
        filters.institution,
        filters.graduate_program_id,
        filters.graduate_program,
        filters.modality,
        filters.graduation,
    ])
    filter_distinct = (
        f'DISTINCT ON ({main_alias}.project_name)' if use_distinct else ''
    )

    join_clauses = '\n'.join(sorted(list(set(JOINS))))
    where_clauses = ' AND '.join(FILTERS_SQL) if FILTERS_SQL else '1=1'

    SCRIPT_SQL = f"""
        WITH rp_ AS (
            SELECT {filter_distinct}
                nature, 
                start_year AS year, 
                project_name
            FROM research_project {main_alias}
                {join_clauses}
            WHERE start_year IS NOT NULL
                AND {where_clauses}
                {term_filter.replace('WHERE', 'AND')}
            ORDER BY project_name
        )
        SELECT 
            rp.year, 
            ARRAY_AGG(rp.nature) AS nature, 
            COUNT(rp.project_name) AS among
        FROM rp_ rp
        GROUP BY year
        ORDER BY rp.year ASC;
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def get_events_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    """
    [NORMALIZADA]
    Obtém métricas de trabalhos em eventos com filtros unificados.
    """
    PARAMS = {}
    JOINS = []
    FILTERS_SQL = []

    # O alias da tabela principal é 'bp' (bibliographic_production)
    main_alias = 'bp'

    # O JOIN com researcher é sempre necessário nesta query para os filtros complexos
    JOINS.append(
        f'INNER JOIN public.researcher r ON r.id = {main_alias}.researcher_id'
    )

    if filters.dep_id or filters.departament:
        JOINS.append("""
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """)
        if filters.dep_id:
            # Corrigindo bug: dep_id deve ser tratado como lista e usar ANY
            PARAMS['dep_id'] = filters.dep_id.split(';')
            FILTERS_SQL.append('dp.dep_id = ANY(%(dep_id)s)')
        if filters.departament:
            PARAMS['departament'] = filters.departament.split(';')
            FILTERS_SQL.append('dp.dep_nom = ANY(%(departament)s)')

    if filters.graduate_program_id or filters.graduate_program:
        JOINS.append("""
            INNER JOIN public.graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN public.graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """)
        if filters.graduate_program_id:
            PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
            FILTERS_SQL.append(
                "gpr.graduate_program_id = %(graduate_program_id)s AND gpr.type_ = 'PERMANENTE'"
            )
        if filters.graduate_program:
            PARAMS['graduate_program'] = filters.graduate_program.split(';')
            FILTERS_SQL.append(
                "gp.name = ANY(%(graduate_program)s) AND gpr.type_ = 'PERMANENTE'"
            )

    if filters.institution:
        PARAMS['institution'] = filters.institution.split(';')
        JOINS.append(
            'INNER JOIN public.institution i ON r.institution_id = i.id'
        )
        FILTERS_SQL.append('i.name = ANY(%(institution)s)')

    if filters.city or filters.area:
        JOINS.append(
            'LEFT JOIN public.researcher_production rp ON rp.researcher_id = r.id'
        )
        if filters.city:
            PARAMS['city'] = filters.city.split(';')
            FILTERS_SQL.append('rp.city = ANY(%(city)s)')
        if filters.area:
            PARAMS['area'] = filters.area.replace(' ', '_').split(';')
            # Corrigindo bug: coluna se chama great_area e operador deve ser &&
            FILTERS_SQL.append(
                "STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s"
            )

    if filters.modality:
        JOINS.append('INNER JOIN public.foment f ON f.researcher_id = r.id')
        PARAMS['modality'] = filters.modality.split(';')
        FILTERS_SQL.append('f.modality_name = ANY(%(modality)s)')

    if filters.graduation:
        PARAMS['graduation'] = filters.graduation.split(';')
        FILTERS_SQL.append('r.graduation = ANY(%(graduation)s)')

    if filters.researcher_id:
        PARAMS['researcher_id'] = str(filters.researcher_id)
        FILTERS_SQL.append(f'{main_alias}.researcher_id = %(researcher_id)s')

    if filters.year:
        PARAMS['year'] = filters.year
        FILTERS_SQL.append(f'{main_alias}.year_::int >= %(year)s::int')

    if filters.institution_id:
        PARAMS['institution_id'] = filters.institution_id
        FILTERS_SQL.append('r.institution_id = %(institution_id)s')

    term_filter = ''
    if filters.term:
        term_filter, term_params = tools.websearch_filter(
            f'{main_alias}.title', filters.term
        )
        PARAMS.update(term_params)

    join_clauses = '\n'.join(sorted(list(set(JOINS))))
    where_clauses = ' AND '.join(FILTERS_SQL) if FILTERS_SQL else '1=1'

    SCRIPT_SQL = f"""
        SELECT 
            COUNT({main_alias}.title) AS among, 
            {main_alias}.year_
        FROM
            public.bibliographic_production {main_alias}
            {join_clauses}
        WHERE 
            {main_alias}.type = 'WORK_IN_EVENT'
            AND {where_clauses}
            {term_filter.replace('WHERE', 'AND')}
        GROUP BY 
            {main_alias}.year_;
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


async def get_magazine_metrics(conn, issn, initials):
    params = {}
    filters = str()
    if initials:
        params['initials'] = initials.lower() + '%'
        filters += 'AND LOWER(name) like %(initials)s'
    if issn:
        params['issn'] = issn
        filters += "AND translate(issn, '-', '')= %(issn)s"

    SCRIPT_SQL = f"""
        SELECT COUNT(*) AS among
        FROM periodical_magazine m
        WHERE 1 = 1
            {filters}
        """
    return await conn.select(SCRIPT_SQL, params, True)
