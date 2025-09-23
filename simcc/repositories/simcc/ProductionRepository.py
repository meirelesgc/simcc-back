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
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        PARAMS['graduate_program_id'] = str(filters.graduate_program_id)
        FILTERS_SQL += ' AND gpr.graduate_program_id = %(graduate_program_id)s'

    if filters.graduate_program:
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

    if filters.distinct:
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

    if filters.distinct:
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
        ORDER BY rpe.researcher_id, rpe.enterprise, rpe.start_year DESC
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

    if filters.distinct:
        DISTINCT_SQL = 'DISTINCT ON (p.title)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
            p.id, p.title, p.category, p.relevance, p.has_image,
            p.development_year AS year, p.details, p.grant_date, p.deposit_date,
            r.id AS researcher, r.lattes_id, r.name AS name, p.code
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
        ORDER BY p.title DESC, p.development_year DESC
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

    if filters.distinct:
        DISTINCT_SQL = 'DISTINCT ON (b.title)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
            b.id, b.title, b.year, b.has_image, b.relevance,
            b.goal, b.nature,
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
        ORDER BY b.title DESC, b.year DESC
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

    if filters.distinct:
        DISTINCT_SQL = 'DISTINCT ON (bp.title)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
            bp.title, bp.year, bpb.isbn AS isbn,
            bpb.publishing_company AS publishing_company,
            bp.researcher_id AS researcher,
            r.lattes_id AS lattes_id, bp.relevance,
            bp.has_image, bp.id, r.name
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
        ORDER BY bp.title DESC, bp.year DESC
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

    if filters.distinct:
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
            b.has_image, b.relevance, bpa.created_at AS created_at
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
        ORDER BY
            b.title DESC, b.year DESC
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

    if filters.distinct:
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
        ORDER BY bp.title DESC, bp.year DESC
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

    if filters.distinct:
        DISTINCT_SQL = 'DISTINCT ON (s.title)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
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
            {join_group}
        WHERE 1 = 1
            {FILTERS_SQL}
            {FILTER_TERMS}
            {FILTER_YEAR}
        ORDER BY s.title DESC, s.year DESC
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

    if filters.distinct:
        DISTINCT_SQL = 'DISTINCT ON (rr.title)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
            rr.id, r.name, rr.title, rr.year, rr.project_name,
            rr.financing_institutionc AS financing
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
        ORDER BY rr.title DESC, rr.year DESC
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

    if filters.distinct:
        DISTINCT_SQL = 'DISTINCT ON (g.title)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
            g.id, g.title, g.nature, g.oriented, g.type, g.status,
            g.year, r.name, r.id AS researcher_id, r.lattes_id
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
        ORDER BY g.title ASC, g.year DESC
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

    if filters.distinct:
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
            bpew.event_name_english, bpew.identifier_number, bpew.isbn
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
        ORDER BY bp.title ASC, bp.year_ DESC
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
        # Usando alias 'rp_prod' para evitar conflito com 'rp' de research_project
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

    if filters.distinct:
        DISTINCT_SQL = 'DISTINCT ON (rp.project_name)'

    SCRIPT_SQL = f"""
        SELECT {DISTINCT_SQL}
            rp.id, rp.researcher_id, r.name, rp.start_year, rp.end_year,
            rp.agency_code, rp.agency_name, rp.project_name, rp.status,
            rp.nature, rp.number_undergraduates, rp.number_specialists,
            rp.number_academic_masters, rp.number_phd, rp.description,
            rpp.production, rpf.foment, rpc.components
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
        ORDER BY rp.project_name ASC, rp.start_year DESC
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

    if filters.distinct:
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
        ORDER BY bp.title ASC, bp.year_ DESC
        {FILTER_PAGINATION};
    """
    return await conn.select(SCRIPT_SQL, PARAMS)


# ---


async def get_book_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}

    join_dep = str()
    query_filters = str()
    term_filter = str()
    filter_distinct = str()
    join_program = str()

    if filters.dep_id:
        filter_distinct = 'DISTINCT'
        params['dep_id'] = filters.dep_id.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        query_filters += """
            AND dp.dep_id = ANY(%(dep_id)s)
            """

    if filters.departament:
        filter_distinct = 'DISTINCT'
        params['dep'] = filters.departament.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        query_filters += """
            AND dp.dep_nom = ANY(%(dep)s)
            """

    if filters.graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = filters.graduate_program.split(';')
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

    join_institution = str()
    if filters.institution:
        params['institution'] = filters.institution.split(';')
        join_institution = """
            LEFT JOIN institution i
                ON r.institution_id = i.id
            """
        query_filters += """
            AND i.name = ANY(%(institution)s)
            """

    join_production = str()
    if filters.area:
        params['area'] = filters.area.replace(' ', '_').split(';')
        join_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = r.id
                """
        query_filters += """
            AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s
        """

    join_modality = str()
    if filters.modality:
        params['modality'] = filters.modality.split(';')
        join_modality = """
            INNER JOIN foment f
                ON f.researcher_id = r.id
            """
        query_filters += 'AND f.modality_name = ANY(%(modality)s)'

    join_production = str()
    if filters.city:
        params['city'] = filters.city.split(';')
        join_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = r.id
                """
        query_filters += 'AND rp.city = ANY(%(city)s)'

    if filters.graduation:
        params['graduation'] = filters.graduation.split(';')
        query_filters += 'AND r.graduation = ANY(%(graduation)s)'

    if filters.distinct:
        filter_distinct = 'DISTINCT'

    if filters.term:
        term_filter, term_params = tools.websearch_filter(
            'bp.title', filters.term
        )
        params.update(term_params)

    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        query_filters += 'AND bp.researcher_id = %(researcher_id)s'

    if filters.graduate_program_id:
        params['program_id'] = str(filters.graduate_program_id)
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gpr.graduate_program_id = %(program_id)s
            AND gpr.type_ = 'PERMANENTE'
            """

    if filters.year:
        params['year'] = filters.year
        query_filters += 'AND bp.year::int >= %(year)s'

    SCRIPT_SQL = f"""
        SELECT bp.year, COUNT({filter_distinct} bp.title) AS among
        FROM researcher r
            LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id
            {join_program}
            {join_modality}
            {join_institution}
            {join_production}
            {join_dep}
        WHERE 1 = 1
            AND bp.type = 'BOOK'
            {term_filter}
            {query_filters}
        GROUP BY
            bp.year
        ORDER BY
            bp.year ASC;
            """
    return await conn.select(SCRIPT_SQL, params)


async def get_book_chapter_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}
    distinct_filter = str()
    join_dep = str()
    filter_dep = str()
    join_program = str()
    program_filter = str()
    filter_institution = str()
    filter_graduate_program = str()
    filter_city = str()
    join_city = str()
    filter_area = str()
    join_area = str()
    filter_modality = str()
    join_modality = str()
    filter_graduation = str()

    if filters.dep_id:
        distinct_filter = 'DISTINCT'
        params['dep_id'] = filters.dep_id.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        filter_dep = 'AND dp.dep_id = ANY(%(dep_id)s)'

    if filters.departament:
        distinct_filter = 'DISTINCT'
        params['dep'] = filters.departament.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        filter_dep = 'AND dp.dep_nom = ANY(%(dep)s)'

    if filters.institution:
        params['institution'] = filters.institution.split(';')
        filter_institution = """
            AND r.institution_id IN (
                SELECT id FROM institution WHERE name = ANY(%(institution)s)
            )
        """

    if filters.graduate_program:
        params['graduate_program'] = filters.graduate_program.split(';')
        filter_graduate_program = """
            AND r.id IN (
                SELECT gpr.researcher_id FROM graduate_program_researcher gpr
                JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
                WHERE gp.name = ANY(%(graduate_program)s) AND gpr.type_ = 'PERMANENTE'
            )
        """

    if filters.city:
        params['city'] = filters.city.split(';')
        join_city = 'LEFT JOIN researcher_production rp_city ON rp_city.researcher_id = r.id'
        filter_city = 'AND rp_city.city = ANY(%(city)s)'

    if filters.area:
        params['area'] = filters.area.replace(' ', '_').split(';')
        join_area = 'LEFT JOIN researcher_production rp_area ON rp_area.researcher_id = r.id'
        filter_area = "AND STRING_TO_ARRAY(REPLACE(rp_area.great_area, ' ', '_'), ';') && %(area)s"

    if filters.modality:
        params['modality'] = filters.modality.split(';')
        join_modality = 'INNER JOIN foment f ON f.researcher_id = r.id'
        filter_modality = 'AND f.modality_name = ANY(%(modality)s)'

    if filters.graduation:
        params['graduation'] = filters.graduation.split(';')
        filter_graduation = 'AND r.graduation = ANY(%(graduation)s)'

    term_filter = str()
    if filters.term:
        term_filter, term_params = tools.websearch_filter(
            'bp.title', filters.term
        )
        params.update(term_params)

    filter_id = str()
    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        filter_id = 'AND bp.researcher_id = %(researcher_id)s'

    if filters.graduate_program_id:
        params['program_id'] = str(filters.graduate_program_id)
        join_program = """
            LEFT JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
        """
        program_filter = """
            AND gpr.graduate_program_id = %(program_id)s
            AND gpr.type_ = 'PERMANENTE'
        """

    year_filter = str()
    if filters.year:
        params['year'] = filters.year
        year_filter = 'AND bp.year::int >= %(year)s'

    if filters.distinct:
        distinct_filter = 'DISTINCT'

    SCRIPT_SQL = f"""
        SELECT bp.year, COUNT({distinct_filter} bp.title) AS among
        FROM researcher r
            LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id
            LEFT JOIN openalex_article opa ON opa.article_id = bp.id
            {join_program}
            {join_dep}
            {join_city}
            {join_area}
            {join_modality}
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
        GROUP BY bp.year
        ORDER BY bp.year ASC;
    """
    return await conn.select(SCRIPT_SQL, params)


async def get_researcher_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}

    join_type = str()
    join_departament = str()
    join_program = str()
    join_researcher_production = str()
    join_foment = str()

    WHERE_SQL = str()
    count_among = 'COUNT(*) as among'

    match filters.type:
        case 'ABSTRACT':
            type_filter, term_params = tools.websearch_filter(
                'r.abstract', filters.term
            )
            WHERE_SQL += type_filter
            params.update(term_params)
            count_among = 'COUNT(*) AS among'
        case (
            'BOOK'
            | 'BOOK_CHAPTER'
            | 'ARTICLE'
            | 'WORK_IN_EVENT'
            | 'TEXT_IN_NEWSPAPER_MAGAZINE'
        ):
            count_among = 'COUNT(DISTINCT bp.title) AS among'
            join_type = f"""
                INNER JOIN bibliographic_production bp
                    ON bp.researcher_id = r.id AND bp.type = '{filters.type}'
            """
            if filters.term:
                term_filter_str, term_params_bp = tools.websearch_filter(
                    'bp.title', filters.term
                )
                WHERE_SQL += term_filter_str
                params.update(term_params_bp)
            if filters.year:
                WHERE_SQL += 'AND bp.year::int >= %(year)s'
                params['year'] = filters.year
        case 'PATENT':
            count_among = 'COUNT(DISTINCT p.title) AS among'
            join_type = 'INNER JOIN patent p ON p.researcher_id = r.id'
            if filters.term:
                term_filter_str, term_params_p = tools.websearch_filter(
                    'p.title', filters.term
                )
                WHERE_SQL += term_filter_str
                params.update(term_params_p)
            if filters.year:
                WHERE_SQL += 'AND p.development_year::int >= %(year)s'
                params['year'] = filters.year
        case 'AREA':
            count_among = 'COUNT(DISTINCT rp.researcher_id) AS among'
            join_type = """
                INNER JOIN researcher_production rp
                    ON rp.researcher_id = r.id
                """
            if filters.term:
                term_filter_str, term_params_rp = tools.websearch_filter(
                    'rp.great_area', filters.term
                )
                WHERE_SQL += term_filter_str
                params.update(term_params_rp)
        case 'EVENT':
            count_among = 'COUNT(DISTINCT e.title) AS among'
            join_type = """
                INNER JOIN event_organization e
                    ON e.researcher_id = r.id
                """
            if filters.term:
                term_filter_str, term_params_e = tools.websearch_filter(
                    'e.title', filters.term
                )
                WHERE_SQL += term_filter_str
                params.update(term_params_e)
            if filters.year:
                WHERE_SQL += 'AND e.year::int >= %(year)s'
                params['year'] = filters.year
        case 'NAME':
            count_among = 'COUNT(DISTINCT r.id) AS among'
            if filters.term:
                name_filter_str, term_params_name = tools.names_filter(
                    'r.name', filters.term
                )
                WHERE_SQL += name_filter_str
                params.update(term_params_name)
        case _:
            pass

    if filters.dep_id or filters.departament:
        if not join_departament:
            join_departament = tools.JOIN_DEPARTAMENT

        if filters.dep_id:
            params['dep_id'] = filters.dep_id.split(';')
            WHERE_SQL += 'AND dp.dep_id = ANY(%(dep_id)s) '
        if filters.departament:
            params['departament'] = filters.departament.split(';')
            WHERE_SQL += """
                AND dp.dep_nom = ANY(%(departament)s)
                """

    if filters.graduate_program_id or filters.graduate_program:
        if not join_program:
            join_program = tools.JOIN_PROGRAM
        if filters.graduate_program_id:
            params['program_id'] = str(filters.graduate_program_id)
            WHERE_SQL += """
                AND gpr.graduate_program_id = %(program_id)s
                AND gpr.type_ = 'PERMANENTE'
                """
        if filters.graduate_program:
            params['graduate_program'] = filters.graduate_program.split(';')
            WHERE_SQL += """
                AND gp.name = ANY(%(graduate_program)s)
                AND gpr.type_ = 'PERMANENTE'
                """

    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        WHERE_SQL += 'AND r.id = %(researcher_id)s'

    if filters.institution:
        params['institution'] = filters.institution.split(';')
        WHERE_SQL += """
            AND r.institution_id IN (
                SELECT id FROM institution WHERE name = ANY(%(institution)s)
            )
        """

    if filters.city or filters.area:
        if not join_researcher_production:
            join_researcher_production = tools.JOIN_RESEARCH_PRODUCTION

        if filters.city:
            params['city'] = filters.city.split(';')
            WHERE_SQL += 'AND rp_prod.city = ANY(%(city)s)'
        if filters.area:
            params['area'] = filters.area.replace(' ', '_').split(';')
            WHERE_SQL += """
                AND STRING_TO_ARRAY(REPLACE(rp_prod.great_area, ' ', '_'), ';') && %(area)s
                """

    if filters.modality:
        join_foment = tools.JOIN_FOMENT
        params['modality'] = filters.modality.split(';')
        if filters.modality != '*':
            WHERE_SQL += 'AND f.modality_name = ANY(%(modality)s)'

    if filters.graduation:
        params['graduation'] = filters.graduation.split(';')
        WHERE_SQL += 'AND r.graduation = ANY(%(graduation)s)'

    SCRIPT_SQL = f"""
        SELECT COUNT(DISTINCT r.id) AS researcher_count,
               COUNT(DISTINCT r.orcid) AS orcid_count,
               COUNT(DISTINCT opr.scopus) AS scopus_count,
               {count_among}
        FROM researcher r
        LEFT JOIN openalex_researcher opr
            ON opr.researcher_id = r.id
            {join_type}
            {join_departament}
            {join_program}
            {join_researcher_production}
            {join_foment}
        WHERE 1 = 1
            {WHERE_SQL}
        """
    return await conn.select(SCRIPT_SQL, params)


async def list_article_metrics(
    conn: Connection,
    qualis,
    filters: DefaultFilters,
):
    params = {}

    query_filters = str()
    filter_distinct = str()

    join_program = str()
    join_dep = str()
    join_institution = str()
    join_area = str()
    join_production = str()
    term_filter = str()

    if filters.distinct:
        filter_distinct = 'DISTINCT'

    if filters.departament:
        filter_distinct = 'DISTINCT'
        params['dep'] = filters.departament.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        query_filters += """
            AND dp.dep_nom = ANY(%(dep)s)
            """

    if filters.dep_id:
        filter_distinct = 'DISTINCT'
        params['dep_id'] = filters.dep_id.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        query_filters += """
            AND dp.dep_id = ANY(%(dep_id)s)
            """

    if filters.term:
        term_filter, term_params = tools.websearch_filter(
            'bp.title', filters.term
        )
        params.update(term_params)

    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        query_filters += 'AND bp.researcher_id = %(researcher_id)s'

    if filters.graduate_program_id:
        params['program_id'] = str(filters.graduate_program_id)
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        query_filters += """
            AND gpr.graduate_program_id = %(program_id)s
            AND gpr.type_ = 'PERMANENTE'
            """

    if filters.institution:
        params['institution'] = filters.institution.split(';')
        join_institution = """
            LEFT JOIN institution i ON r.institution_id = i.id
            """
        query_filters += """
            AND i.name = ANY(%(institution)s)
            """
    if filters.city:
        params['city'] = filters.city.split(';')
        join_production = """
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            """
        query_filters += 'AND rp.city = ANY(%(city)s)'

    if filters.area:
        params['area'] = filters.area.replace(' ', '_').split(';')
        join_production = """
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            """
        query_filters += """
            AND rp.great_area = ANY(%(area)s)
            """

    join_modality = str()
    if filters.modality:
        params['modality'] = filters.modality.split(';')
        join_modality = """
            INNER JOIN foment f ON f.researcher_id = r.id
            """
        query_filters += 'AND f.modality_name = ANY(%(modality)s)'

    if filters.graduation:
        params['graduation'] = filters.graduation.split(';')
        query_filters += 'AND r.graduation = ANY(%(graduation)s)'

    if filters.graduate_program:
        params['graduate_program'] = filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        query_filters += """
            AND gp.name = ANY(%(graduate_program)s)
            AND gpr.type_ = 'PERMANENTE'
            """

    if filters.year:
        params['year'] = filters.year
        query_filters += """
            AND bp.year::int >= %(year)s
            """

    if qualis:
        params['qualis'] = qualis.split(';')
        query_filters += """
            AND bpa.qualis = ANY(%(qualis)s)
            """

    SCRIPT_SQL = f"""
        SELECT bp.year, SUM(opa.citations_count) AS citations,
            ARRAY_AGG(bpa.qualis) AS qualis, ARRAY_AGG(bpa.jcr) AS jcr,
            COUNT({filter_distinct} bp.title) AS among, COUNT(DISTINCT bp.doi)
            AS count_doi
        FROM researcher r
            LEFT JOIN bibliographic_production bp ON bp.researcher_id = r.id
            INNER JOIN bibliographic_production_article bpa ON bpa.bibliographic_production_id = bp.id
            LEFT JOIN openalex_article opa ON opa.article_id = bp.id
            {join_program}
            {join_modality}
            {join_institution}
            {join_production}
            {join_area}
            {join_dep}
        WHERE 1 = 1
            {query_filters}
            {term_filter}
        GROUP BY
            bp.year
        ORDER BY
            bp.year ASC;
    """
    return await conn.select(SCRIPT_SQL, params)


async def list_patent_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}

    join_dep = str()
    filter_dep = str()
    if filters.dep_id:
        params['dep_id'] = filters.dep_id.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = p.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_id = ANY(%(dep_id)s)
            """

    if filters.departament:
        params['dep'] = filters.departament.split(';')
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
    if filters.distinct:
        filter_distinct = 'DISTINCT'

    term_filter = str()
    if filters.term:
        term_filter, term_params = tools.websearch_filter(
            'p.title', filters.term
        )
        params.update(term_params)

    filter_id = str()
    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        filter_id = 'AND p.researcher_id = %(researcher_id)s'

    join_program = str()
    filter_program = str()
    if filters.graduate_program_id:
        params['program_id'] = str(filters.graduate_program_id)
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = p.researcher_id
        """
        filter_program = 'AND gpr.graduate_program_id = %(program_id)s'

    join_graduate_program = str()
    filter_graduate_program = str()
    if filters.graduate_program:
        params['graduate_program'] = filters.graduate_program.split(';')
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
    if filters.institution:
        params['institution'] = filters.institution.split(';')
        join_institution = """
            LEFT JOIN researcher r ON r.id = p.researcher_id
            LEFT JOIN institution i ON r.institution_id = i.id
        """
        filter_institution = 'AND i.name = ANY(%(institution)s)'

    join_city = str()
    filter_city = str()
    if filters.city:
        params['city'] = filters.city.split(';')
        join_city = """
            LEFT JOIN researcher_production rp ON rp.researcher_id = p.researcher_id
        """
        filter_city = 'AND rp.city = ANY(%(city)s)'

    join_area = str()
    filter_area = str()
    if filters.area:
        params['area'] = filters.area.replace(' ', '_').split(';')
        join_area = """
            LEFT JOIN researcher_production ra ON ra.researcher_id = p.researcher_id
        """
        filter_area = """
            AND STRING_TO_ARRAY(REPLACE(ra.great_area, ' ', '_'), ';') && %(area)s
        """

    join_modality = str()
    filter_modality = str()
    if filters.modality:
        params['modality'] = filters.modality.split(';')
        join_modality = """
            INNER JOIN foment f ON f.researcher_id = p.researcher_id
        """
        filter_modality = 'AND f.modality_name = ANY(%(modality)s)'

    filter_graduation = str()
    join_graduation = str()
    if filters.graduation:
        join_graduation = """
            LEFT JOIN researcher rg ON rg.id = p.researcher_id
        """
        params['graduation'] = filters.graduation.split(';')
        filter_graduation = 'AND rg.graduation = ANY(%(graduation)s)'

    filter_year = str()
    if filters.year:
        params['year'] = filters.year
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
        GROUP BY p.development_year
        ORDER BY p.development_year ASC;
    """

    return await conn.select(SCRIPT_SQL, params)


async def list_guidance_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}
    join_dep = str()
    filter_dep = str()
    filter_distinct = str()

    if filters.distinct:
        filter_distinct = 'DISTINCT ON (oriented, nature, year)'

    if filters.departament:
        filter_distinct = 'DISTINCT ON (oriented, nature, year)'
        params['dep'] = filters.departament.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = g.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_nom = ANY(%(dep)s)
            """

    if filters.dep_id:
        filter_distinct = 'DISTINCT ON (oriented, nature, year)'
        params['dep_id'] = filters.dep_id.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = g.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_id = ANY(%(dep_id)s)
            """

    term_filter = str()
    if filters.term:
        term_filter, term_params = tools.websearch_filter(
            'g.title', filters.term
        )
        params.update(term_params)

    filter_id = str()
    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        filter_id = 'AND g.researcher_id = %(researcher_id)s'

    filter_year = str()
    if filters.year:
        params['year'] = filters.year
        filter_year = 'AND g.year >= %(year)s'

    join_program = str()
    filter_program = str()
    if filters.graduate_program_id:
        params['program_id'] = str(filters.graduate_program_id)
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = g.researcher_id
        """
        filter_program = 'AND gpr.graduate_program_id = %(program_id)s'

    join_graduate_program = str()
    filter_graduate_program = str()
    if filters.graduate_program:
        params['graduate_program'] = filters.graduate_program.split(';')
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
    if filters.institution:
        params['institution'] = filters.institution.split(';')
        join_institution = """
            LEFT JOIN researcher r ON r.id = g.researcher_id
            LEFT JOIN institution i ON r.institution_id = i.id
        """
        filter_institution = 'AND i.name = ANY(%(institution)s)'

    join_city = str()
    filter_city = str()
    if filters.city:
        params['city'] = filters.city.split(';')
        join_city = """
            LEFT JOIN researcher_production rp ON rp.researcher_id = g.researcher_id
        """
        filter_city = 'AND rp.city = ANY(%(city)s)'

    join_area = str()
    filter_area = str()
    if filters.area:
        params['area'] = filters.area.replace(' ', '_').split(';')
        join_area = """
            LEFT JOIN researcher_production ra ON ra.researcher_id = g.researcher_id
        """
        filter_area = """
            AND STRING_TO_ARRAY(REPLACE(ra.great_area, ' ', '_'), ';') && %(area)s
        """

    join_modality = str()
    filter_modality = str()
    if filters.modality:
        params['modality'] = filters.modality.split(';')
        join_modality = """
            INNER JOIN foment f ON f.researcher_id = g.researcher_id
        """
        filter_modality = 'AND f.modality_name = ANY(%(modality)s)'

    join_graduation = str()
    filter_graduation = str()
    if filters.graduation:
        join_graduation = """
            LEFT JOIN researcher rg ON rg.id = g.researcher_id
        """
        params['graduation'] = filters.graduation.split(';')
        filter_graduation = 'AND rg.graduation = ANY(%(graduation)s)'

    SCRIPT_SQL = f"""
        WITH Orientacoes AS (
            SELECT {filter_distinct}
                g.year AS year,
                unaccent(lower((g.nature || ' ' || g.status))) AS nature,
                g.oriented AS oriented
            FROM
                guidance g
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
            ORDER BY oriented, nature, year
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

    return await conn.select(SCRIPT_SQL, params)


async def list_academic_degree_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}

    join_dep = str()
    filter_dep = str()

    if filters.departament:
        params['dep'] = filters.departament.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = e.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_nom = ANY(%(dep)s)
            """

    if filters.dep_id:
        params['dep_id'] = filters.dep_id.split(';')
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
    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        filter_id = 'AND e.researcher_id = %(researcher_id)s'

    filter_year = str()
    if filters.year:
        params['year'] = filters.year
        filter_year = """
            AND (e.education_start >= %(year)s OR e.education_end >= %(year)s)
        """

    filter_program = str()
    if filters.graduate_program_id:
        params['program_id'] = str(filters.graduate_program_id)
        filter_program = """
            AND e.researcher_id IN (
                SELECT researcher_id
                FROM graduate_program_researcher
                WHERE graduate_program_id = %(program_id)s
            )
        """

    join_graduate_program = str()
    filter_graduate_program = str()
    if filters.graduate_program:
        params['graduate_program'] = filters.graduate_program.split(';')
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
    if filters.institution:
        params['institution'] = filters.institution.split(';')
        join_institution = """
            LEFT JOIN researcher r ON r.id = e.researcher_id
            LEFT JOIN institution i ON r.institution_id = i.id
        """
        filter_institution = 'AND i.name = ANY(%(institution)s)'

    join_city = str()
    filter_city = str()
    if filters.city:
        params['city'] = filters.city.split(';')
        join_city = """
            LEFT JOIN researcher_production rp ON rp.researcher_id = e.researcher_id
        """
        filter_city = 'AND rp.city = ANY(%(city)s)'

    join_area = str()
    filter_area = str()
    if filters.area:
        params['area'] = filters.area.replace(' ', '_').split(';')
        join_area = """
            LEFT JOIN researcher_production ra ON ra.researcher_id = e.researcher_id
        """
        filter_area = """
            AND STRING_TO_ARRAY(REPLACE(ra.great_area, ' ', '_'), ';') && %(area)s
        """

    join_modality = str()
    filter_modality = str()
    if filters.modality:
        params['modality'] = filters.modality.split(';')
        join_modality = """
            INNER JOIN foment f ON f.researcher_id = e.researcher_id
        """
        filter_modality = 'AND f.modality_name = ANY(%(modality)s)'

    join_graduation = str()
    filter_graduation = str()
    if filters.graduation:
        join_graduation = """
            LEFT JOIN researcher rg ON rg.id = e.researcher_id
        """
        params['graduation'] = filters.graduation.split(';')
        filter_graduation = 'AND rg.graduation = ANY(%(graduation)s)'
    SCRIPT_SQL = f"""
        WITH EducacaoFiltrada AS (
            -- 1. CTE Base: Seleciona e filtra os dados brutos apenas uma vez.
            SELECT
                e.education_start,
                e.education_end,
                e.degree
            FROM
                education e
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
            -- ORDER BY em uma CTE só tem efeito se usado com cláusulas como LIMIT/FETCH.
            -- A ordenação final é aplicada no último SELECT.
        ),
        EducacaoUnificada AS (
            -- 2. CTE Unificada: Transforma os dados, tratando anos de início e fim como eventos separados.
            SELECT
                education_start AS year,
                degree,
                'START' AS event_type
            FROM EducacaoFiltrada
            WHERE education_start IS NOT NULL

            UNION ALL

            SELECT
                education_end AS year,
                degree,
                'END' AS event_type
            FROM EducacaoFiltrada
            WHERE education_end IS NOT NULL
        )
        -- 3. SELECT Final: Agrega os dados unificados e formata a saída.
        SELECT
            year,
            COUNT(degree) AS among,
            REPLACE(degree || '-' || event_type, '-', '_') AS degree
        FROM EducacaoUnificada
        GROUP BY
            year,
            degree,
            event_type
        ORDER BY
            year,
            degree;
    """
    return await conn.select(SCRIPT_SQL, params)


async def list_software_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}
    filter_distinct = str()
    join_dep = str()
    filter_dep = str()

    if filters.departament:
        filter_distinct = 'DISTINCT ON (s.title)'
        params['dep'] = filters.departament.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = s.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_nom = ANY(%(dep)s)
            """

    if filters.dep_id:
        filter_distinct = 'DISTINCT ON (s.title)'
        params['dep_id'] = filters.dep_id.split(';')
        join_dep = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = s.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
        filter_dep = """
            AND dp.dep_id = ANY(%(dep_id)s)
            """

    if filters.distinct:
        filter_distinct = 'DISTINCT ON (s.title)'

    term_filter = str()
    if filters.term:
        term_filter, term_params = tools.websearch_filter(
            's.title', filters.term
        )
        params.update(term_params)

    filter_id = str()
    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        filter_id = 'AND s.researcher_id = %(researcher_id)s'

    filter_year = str()
    if filters.year:
        params['year'] = filters.year
        filter_year = 'AND s.year >= %(year)s'

    filter_program = str()
    join_program = str()
    if filters.graduate_program_id:
        params['program_id'] = str(filters.graduate_program_id)
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = s.researcher_id
        """
        filter_program = 'AND gpr.graduate_program_id = %(program_id)s'

    join_graduate_program = str()
    filter_graduate_program = str()
    if filters.graduate_program:
        params['graduate_program'] = filters.graduate_program.split(';')
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
    if filters.institution:
        params['institution'] = filters.institution.split(';')
        join_institution = """
            LEFT JOIN researcher r ON r.id = s.researcher_id
            LEFT JOIN institution i ON r.institution_id = i.id
        """
        filter_institution = 'AND i.name = ANY(%(institution)s)'

    join_city = str()
    filter_city = str()
    if filters.city:
        params['city'] = filters.city.split(';')
        join_city = """
            LEFT JOIN researcher_production rp ON rp.researcher_id = s.researcher_id
        """
        filter_city = 'AND rp.city = ANY(%(city)s)'

    join_area = str()
    filter_area = str()
    if filters.area:
        params['area'] = filters.area.replace(' ', '_').split(';')
        join_area = """
            LEFT JOIN researcher_production ra ON ra.researcher_id = s.researcher_id
        """
        filter_area = """
            AND STRING_TO_ARRAY(REPLACE(ra.great_area, ' ', '_'), ';') && %(area)s
        """

    join_modality = str()
    filter_modality = str()
    if filters.modality:
        params['modality'] = filters.modality.split(';')
        join_modality = """
            INNER JOIN foment f ON f.researcher_id = s.researcher_id
        """
        filter_modality = 'AND f.modality_name = ANY(%(modality)s)'

    join_graduation = str()
    filter_graduation = str()
    if filters.graduation:
        join_graduation = """
            LEFT JOIN researcher rg ON rg.id = s.researcher_id
        """
        params['graduation'] = filters.graduation.split(';')
        filter_graduation = 'AND rg.graduation = ANY(%(graduation)s)'

    SCRIPT_SQL = f"""
        WITH FilteredSoftware AS (
            SELECT {filter_distinct}
                s.year,
                s.title
            FROM
                public.software s
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
                ORDER BY s.title, s.year
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
    return await conn.select(SCRIPT_SQL, params)


async def get_research_report_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}
    filter_distinct = str()

    join_researcher = str()
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filters_sql = str()

    if filters.term:
        filter_terms_str, term_params = tools.websearch_filter(
            'rr.title', filters.term
        )
        filters_sql += filter_terms_str
        params.update(term_params)

    if filters.year:
        params['year'] = filters.year
        filters_sql += """
            AND rr.year >= %(year)s
            """

    # Aplica DISTINCT ao filtrar por departamento
    if filters.dep_id or filters.departament:
        filter_distinct = 'DISTINCT ON (rr.title)'
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = rr.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if filters.dep_id:
        params['dep_id'] = filters.dep_id.split(';')
        filters_sql += """
            AND dp.dep_id = ANY(%(dep_id)s)
            """

    if filters.departament:
        params['departament'] = filters.departament.split(';')
        filters_sql += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        filters_sql += """
            AND rr.researcher_id = %(researcher_id)s
            """

    if filters.institution:
        params['institution'] = filters.institution.split(';')
        join_researcher = """
            INNER JOIN researcher r
                ON rr.researcher_id = r.id
            """
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        filters_sql += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id or filters.graduate_program:
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = rr.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
    if filters.graduate_program_id:
        params['graduate_program_id'] = str(filters.graduate_program_id)
        filters_sql += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:
        params['graduate_program'] = filters.graduate_program.split(';')
        filters_sql += """
            AND gp.name = ANY(%(graduate_program)s)
            AND gpr.type_ = 'PERMANENTE'
            """

    if filters.city:
        params['city'] = filters.city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = rr.researcher_id
            """
        filters_sql += """
            AND rp.city = ANY(%(city)s)
            """

    if filters.area:
        params['area'] = filters.area.replace(' ', '_').split(';')
        if not join_researcher_production:
            join_researcher_production = """
                LEFT JOIN researcher_production rp
                    ON rp.researcher_id = rr.researcher_id
                """
        filters_sql += """
            AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s
            """

    if filters.modality:
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = rr.researcher_id
            """
        filters_sql += """
            AND f.modality_name = ANY(%(modality)s)
            """

    # Aplica DISTINCT ao filtrar por graduação
    if filters.graduation:
        filter_distinct = 'DISTINCT ON (rr.title)'
        if not join_researcher:
            join_researcher = """
                INNER JOIN researcher r
                    ON rr.researcher_id = r.id
                """
        params['graduation'] = filters.graduation.split(';')
        filters_sql += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if filters.distinct:
        filter_distinct = 'DISTINCT ON (rr.title)'

    SCRIPT_SQL = f"""
        WITH FilteredResearchReport AS (
            SELECT {filter_distinct}
                rr.year,
                rr.title
            FROM
                public.research_report rr
                {join_researcher}
                {join_researcher_production}
                {join_foment}
                {join_program}
                {join_departament}
                {join_institution}
            WHERE 1=1
                {filters_sql}
            ORDER BY rr.title, rr.year
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
    return await conn.select(SCRIPT_SQL, params)


async def get_papers_magazine_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}
    filter_distinct = str()

    join_researcher = str()
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filters_sql = str()

    if filters.term:
        filter_terms_str, term_params = tools.websearch_filter(
            'bp.title', filters.term
        )
        filters_sql += filter_terms_str
        params.update(term_params)

    if filters.year:
        params['year'] = filters.year
        filters_sql += """
            AND bp.year_ >= %(year)s
            """

    if filters.dep_id or filters.departament:
        filter_distinct = 'DISTINCT ON (bp.title)'
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = bp.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if filters.dep_id:
        params['dep_id'] = filters.dep_id.split(';')
        filters_sql += """
            AND dp.dep_id = ANY(%(dep_id)s)
            """

    if filters.departament:
        params['departament'] = filters.departament.split(';')
        filters_sql += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        filters_sql += """
            AND bp.researcher_id = %(researcher_id)s
            """

    if filters.institution:
        params['institution'] = filters.institution.split(';')
        join_researcher = """
            INNER JOIN researcher r
                ON bp.researcher_id = r.id
            """
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        filters_sql += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id or filters.graduate_program:
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
    if filters.graduate_program_id:
        params['graduate_program_id'] = str(filters.graduate_program_id)
        filters_sql += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:
        params['graduate_program'] = filters.graduate_program.split(';')
        filters_sql += """
            AND gp.name = ANY(%(graduate_program)s)
            AND gpr.type_ = 'PERMANENTE'
            """

    if filters.city:
        params['city'] = filters.city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = bp.researcher_id
            """
        filters_sql += """
            AND rp.city = ANY(%(city)s)
            """

    if filters.area:
        params['area'] = filters.area.replace(' ', '_').split(';')
        if not join_researcher_production:
            join_researcher_production = """
                LEFT JOIN researcher_production rp
                    ON rp.researcher_id = bp.researcher_id
                """
        filters_sql += """
            AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s
            """

    if filters.modality:
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = bp.researcher_id
            """
        filters_sql += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:
        filter_distinct = 'DISTINCT ON (bp.title)'
        if not join_researcher:
            join_researcher = """
                INNER JOIN researcher r
                    ON bp.researcher_id = r.id
                """
        params['graduation'] = filters.graduation.split(';')
        filters_sql += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if filters.distinct:
        filter_distinct = 'DISTINCT ON (bp.title)'

    SCRIPT_SQL = f"""
        WITH FilteredPapers AS (
            SELECT {filter_distinct}
                bp.title,
                bp.year_ AS year
            FROM
                public.bibliographic_production bp
                {join_researcher}
                {join_researcher_production}
                {join_foment}
                {join_program}
                {join_departament}
                {join_institution}
            WHERE
                bp.type = 'TEXT_IN_NEWSPAPER_MAGAZINE'
                {filters_sql}
            ORDER BY bp.title, bp.year_
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
    return await conn.select(SCRIPT_SQL, params)


async def get_speaker_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}
    filter_distinct = str()

    join_researcher = str()
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filters_sql = str()

    if filters.term:
        filter_terms_str, term_params = tools.websearch_filter(
            'pe.title', filters.term
        )
        filters_sql += filter_terms_str
        params.update(term_params)

    if filters.year:
        params['year'] = filters.year
        filters_sql += """
            AND pe.year >= %(year)s
            """

    if filters.dep_id or filters.departament:
        filter_distinct = 'DISTINCT ON (pe.title)'
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = pe.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if filters.dep_id:
        params['dep_id'] = filters.dep_id.split(';')
        filters_sql += """
            AND dp.dep_id = ANY(%(dep_id)s)
            """

    if filters.departament:
        params['departament'] = filters.departament.split(';')
        filters_sql += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        filters_sql += """
            AND pe.researcher_id = %(researcher_id)s
            """

    if filters.institution:
        params['institution'] = filters.institution.split(';')
        join_researcher = """
            INNER JOIN researcher r
                ON pe.researcher_id = r.id
            """
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        filters_sql += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id or filters.graduate_program:
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = pe.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
    if filters.graduate_program_id:
        params['graduate_program_id'] = str(filters.graduate_program_id)
        filters_sql += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:
        params['graduate_program'] = filters.graduate_program.split(';')
        filters_sql += """
            AND gp.name = ANY(%(graduate_program)s)
            AND gpr.type_ = 'PERMANENTE'
            """

    if filters.city:
        params['city'] = filters.city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = pe.researcher_id
            """
        filters_sql += """
            AND rp.city = ANY(%(city)s)
            """

    if filters.area:
        params['area'] = filters.area.replace(' ', '_').split(';')
        if not join_researcher_production:
            join_researcher_production = """
                LEFT JOIN researcher_production rp
                    ON rp.researcher_id = pe.researcher_id
                """
        filters_sql += """
            AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s
            """

    if filters.modality:
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = pe.researcher_id
            """
        filters_sql += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:
        filter_distinct = 'DISTINCT ON (pe.title)'
        if not join_researcher:
            join_researcher = """
                INNER JOIN researcher r
                    ON pe.researcher_id = r.id
                """
        params['graduation'] = filters.graduation.split(';')
        filters_sql += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if filters.distinct:
        filter_distinct = 'DISTINCT ON (pe.title)'

    SCRIPT_SQL = f"""
        WITH FilteredEvents AS (
            SELECT {filter_distinct}
                pe.title,
                pe.year,
                pe.nature
            FROM
                public.participation_events pe
                {join_researcher}
                {join_researcher_production}
                {join_foment}
                {join_program}
                {join_departament}
                {join_institution}
            WHERE 1=1
                {filters_sql}
            ORDER BY pe.title, pe.year
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
    return await conn.select(SCRIPT_SQL, params)


async def get_brand_metrics(
    conn: Connection,
    nature,
    filters: DefaultFilters,
):
    params = {}
    filter_distinct = str()

    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()
    join_researcher = str()

    filters_sql = str()

    if nature:
        params['nature'] = nature.split(';')
        filters_sql += """
            AND b.nature = ANY(%(nature)s)
            """

    if filters.term:
        filter_terms_str, term_params = tools.websearch_filter(
            'b.title', filters.term
        )
        filters_sql += filter_terms_str
        params.update(term_params)

    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        filters_sql += """
            AND b.researcher_id = %(researcher_id)s
            """

    if filters.year:
        params['year'] = filters.year
        filters_sql += """
            AND b.year >= %(year)s
            """

    if filters.dep_id or filters.departament:
        filter_distinct = 'DISTINCT ON (b.title)'
        join_researcher = """
            LEFT JOIN public.researcher r ON r.id = b.researcher_id
            """
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if filters.dep_id:
        params['dep_id'] = filters.dep_id.split(';')
        filters_sql += """
            AND dp.dep_id = ANY(%(dep_id)s)
            """

    if filters.departament:
        params['departament'] = filters.departament.split(';')
        filters_sql += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if filters.institution:
        filter_distinct = 'DISTINCT ON (b.title)'  # Adicionado para consistência
        join_researcher = """
            LEFT JOIN public.researcher r ON r.id = b.researcher_id
            """
        params['institution'] = filters.institution.split(';')
        join_institution = """
            INNER JOIN public.institution i
                ON r.institution_id = i.id
            """
        filters_sql += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id or filters.graduate_program:
        filter_distinct = 'DISTINCT ON (b.title)'  # Adicionado para consistência
        join_researcher = """
            LEFT JOIN public.researcher r ON r.id = b.researcher_id
            """
        join_program = """
            INNER JOIN public.graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            INNER JOIN public.graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
    if filters.graduate_program_id:
        params['graduate_program_id'] = str(filters.graduate_program_id)
        filters_sql += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:
        params['graduate_program'] = filters.graduate_program.split(';')
        filters_sql += """
            AND gp.name = ANY(%(graduate_program)s)
            AND gpr.type_ = 'PERMANENTE'
            """

    if filters.city:
        if not join_researcher:
            join_researcher = """
                LEFT JOIN public.researcher r ON r.id = b.researcher_id
                """
        params['city'] = filters.city.split(';')
        join_researcher_production = """
            LEFT JOIN public.researcher_production rp
                ON rp.researcher_id = r.id
            """
        filters_sql += """
            AND rp.city = ANY(%(city)s)
            """

    if filters.area:
        if not join_researcher:
            join_researcher = """
                LEFT JOIN public.researcher r ON r.id = b.researcher_id
                """
        params['area'] = filters.area.replace(' ', '_').split(';')
        if not join_researcher_production:
            join_researcher_production = """
                LEFT JOIN public.researcher_production rp
                    ON rp.researcher_id = r.id
                """
        filters_sql += """
            AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s
            """

    if filters.modality:
        filter_distinct = 'DISTINCT ON (b.title)'  # Adicionado para consistência
        if not join_researcher:
            join_researcher = """
                LEFT JOIN public.researcher r ON r.id = b.researcher_id
                """
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN public.foment f
                ON f.researcher_id = r.id
            """
        filters_sql += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:
        filter_distinct = 'DISTINCT ON (b.title)'
        if not join_researcher:
            join_researcher = """
                LEFT JOIN public.researcher r ON r.id = b.researcher_id
                """
        params['graduation'] = filters.graduation.split(';')
        filters_sql += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if filters.distinct:
        filter_distinct = 'DISTINCT ON (b.title)'

    SCRIPT_SQL = f"""
        WITH FilteredBrands AS (
            SELECT {filter_distinct}
                b.title,
                b.year
            FROM
                public.brand b
                {join_researcher}
                {join_researcher_production}
                {join_foment}
                {join_program}
                {join_departament}
                {join_institution}
            WHERE 1 = 1
                {filters_sql}
            ORDER BY b.title, b.year
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
    return await conn.select(SCRIPT_SQL, params)


async def get_research_project_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}
    filter_distinct = str()

    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()
    join_researcher = str()

    filters_sql = str()

    if filters.term:
        filter_terms_str, term_params = tools.websearch_filter(
            'b.project_name', filters.term
        )
        filters_sql += filter_terms_str
        params.update(term_params)

    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        filters_sql += """
            AND b.researcher_id = %(researcher_id)s
            """

    if filters.year:
        params['year'] = filters.year
        filters_sql += """
            AND b.start_year >= %(year)s
            """

    if filters.dep_id or filters.departament:
        filter_distinct = 'DISTINCT ON (b.project_name)'
        join_researcher = """
            LEFT JOIN public.researcher r ON r.id = b.researcher_id
            """
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if filters.dep_id:
        params['dep_id'] = filters.dep_id.split(';')
        filters_sql += """
            AND dp.dep_id = ANY(%(dep_id)s)
            """

    if filters.departament:
        params['departament'] = filters.departament.split(';')
        filters_sql += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if filters.institution:
        filter_distinct = 'DISTINCT ON (b.project_name)'
        if not join_researcher:
            join_researcher = """
                LEFT JOIN public.researcher r ON r.id = b.researcher_id
                """
        params['institution'] = filters.institution.split(';')
        join_institution = """
            INNER JOIN public.institution i
                ON r.institution_id = i.id
            """
        filters_sql += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id or filters.graduate_program:
        filter_distinct = 'DISTINCT ON (b.project_name)'
        if not join_researcher:
            join_researcher = """
                LEFT JOIN public.researcher r ON r.id = b.researcher_id
                """
        join_program = """
            INNER JOIN public.graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            INNER JOIN public.graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
    if filters.graduate_program_id:
        params['graduate_program_id'] = str(filters.graduate_program_id)
        filters_sql += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:
        params['graduate_program'] = filters.graduate_program.split(';')
        filters_sql += """
            AND gp.name = ANY(%(graduate_program)s)
            AND gpr.type_ = 'PERMANENTE'
            """

    if filters.city:
        if not join_researcher:
            join_researcher = """
                LEFT JOIN public.researcher r ON r.id = b.researcher_id
                """
        params['city'] = filters.city.split(';')
        join_researcher_production = """
            LEFT JOIN public.researcher_production rp
                ON rp.researcher_id = r.id
            """
        filters_sql += """
            AND rp.city = ANY(%(city)s)
            """

    if filters.area:
        if not join_researcher:
            join_researcher = """
                LEFT JOIN public.researcher r ON r.id = b.researcher_id
                """
        params['area'] = filters.area.replace(' ', '_').split(';')
        if not join_researcher_production:
            join_researcher_production = """
                LEFT JOIN public.researcher_production rp
                    ON rp.researcher_id = r.id
                """
        filters_sql += """
            AND STRING_TO_ARRAY(REPLACE(rp.great_area, ' ', '_'), ';') && %(area)s
            """

    if filters.modality:
        filter_distinct = 'DISTINCT ON (b.project_name)'
        if not join_researcher:
            join_researcher = """
                LEFT JOIN public.researcher r ON r.id = b.researcher_id
                """
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN public.foment f
                ON f.researcher_id = r.id
            """
        filters_sql += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:
        filter_distinct = 'DISTINCT ON (b.project_name)'
        if not join_researcher:
            join_researcher = """
                LEFT JOIN public.researcher r ON r.id = b.researcher_id
                """
        params['graduation'] = filters.graduation.split(';')
        filters_sql += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if filters.distinct:
        filter_distinct = 'DISTINCT ON (b.project_name)'

    SCRIPT_SQL = f"""
        WITH rp_ AS (
            SELECT {filter_distinct}
                nature AS nature, start_year AS year, project_name
            FROM research_project b
                {join_researcher}
                {join_researcher_production}
                {join_foment}
                {join_program}
                {join_departament}
                {join_institution}
            WHERE start_year IS NOT NULL
                {filters_sql}
            ORDER BY project_name
        )
        SELECT rp.year, ARRAY_AGG(rp.nature) AS nature, COUNT(rp.project_name) AS among
        FROM rp_ rp
        GROUP BY year
        ORDER BY rp.year ASC;
        """
    return await conn.select(SCRIPT_SQL, params)


async def get_researcher_research_project(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}
    SCRIPT_SQL = """

    """
    return await conn.select(SCRIPT_SQL, params)


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


async def get_events_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()
    join_researcher = """
            INNER JOIN public.researcher r ON r.id = bp.researcher_id
            """

    query_filters = str()

    if filters.term:
        filter_terms_str, term_params = tools.websearch_filter(
            'bp.title', filters.term
        )
        query_filters += filter_terms_str
        params.update(term_params)

    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        query_filters += """
            AND bp.researcher_id = %(researcher_id)s
            """

    if filters.year:
        params['year'] = filters.year
        query_filters += """
            AND bp.year_ >= %(year)s
            """

    if filters.dep_id or filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = r.id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if filters.dep_id:
        params['dep_id'] = filters.dep_id
        query_filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if filters.departament:
        params['departament'] = filters.departament.split(';')
        query_filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if filters.institution:
        params['institution'] = filters.institution.split(';')
        join_institution = """
            INNER JOIN public.institution i
                ON r.institution_id = i.id
            """
        query_filters += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id:
        params['graduate_program_id'] = str(filters.graduate_program_id)
        join_program = """
            INNER JOIN public.graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            INNER JOIN public.graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:
        params['graduate_program'] = filters.graduate_program.split(';')
        if not join_program:
            join_program = """
                INNER JOIN public.graduate_program_researcher gpr
                    ON gpr.researcher_id = r.id
                INNER JOIN public.graduate_program gp
                    ON gpr.graduate_program_id = gp.graduate_program_id
                """
        query_filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if filters.city:
        params['city'] = filters.city.split(';')
        join_researcher_production = """
            LEFT JOIN public.researcher_production rp
                ON rp.researcher_id = r.id
            """
        query_filters += """
            AND rp.city = ANY(%(city)s)
            """

    if filters.area:
        params['area'] = filters.area.replace(' ', '_').split(';')
        if not join_researcher_production:
            join_researcher_production = """
                LEFT JOIN public.researcher_production rp
                    ON rp.researcher_id = r.id
                """
        query_filters += """
            AND rp.great_area_ && %(area)s
            """

    if filters.modality:
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN public.foment f
                ON f.researcher_id = r.id
            """
        query_filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:
        params['graduation'] = filters.graduation.split(';')
        query_filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    SCRIPT_SQL = f"""
        SELECT COUNT(bp.title) AS among, bp.year_
        FROM
            public.bibliographic_production bp
            {join_researcher}
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
        WHERE bp.type = 'WORK_IN_EVENT'
        {query_filters}
        GROUP BY bp.year_;
        """
    result = await conn.select(SCRIPT_SQL, params)
    return result
