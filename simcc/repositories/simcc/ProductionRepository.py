from simcc.core.connection import Connection
from simcc.repositories.util import names_filter, pagination, webseatch_filter
from simcc.schemas import DefaultFilters
from simcc.schemas.Production.Article import ArticleProduction
from simcc.schemas.Production.Patent import PatentProduction


async def get_pevent_researcher(
    conn: Connection,
    filters: DefaultFilters,
    nature: str | None,
    page: int | None,
    lenght: int | None,
):
    params = {}
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    query_filters = str()

    if filters.term:
        filter_terms_str, term_params = webseatch_filter('p.title', filters.term)
        query_filters += filter_terms_str
        params.update(term_params)

    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        query_filters += """
            AND p.researcher_id = %(researcher_id)s
            """

    if filters.year:
        params['year'] = filters.year
        query_filters += """
            AND p.year >= %(year)s
            """

    if nature:
        params['nature'] = nature.split(';')
        query_filters += """
            AND p.nature = ANY(%(nature)s)
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

    filter_pagination = str()
    if page and lenght:
        filter_pagination = pagination(page, lenght)

    filter_distinct = str()
    if filters.distinct:
        filter_distinct = 'DISTINCT ON (p.title)'

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            r.name,
            p.id AS id,
            p.title,
            p.event_name,
            p.nature,
            p.form_participation,
            p.year
        FROM
            public.participation_events p
            LEFT JOIN public.researcher r ON r.id = p.researcher_id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
        WHERE 1 = 1
            {query_filters}
        ORDER BY
            p.title
        {filter_pagination};
        """
    result = await conn.select(SCRIPT_SQL, params)
    return result


async def professional_experience(
    conn: Connection,
    filters: DefaultFilters,
    page: int | None,
    lenght: int | None,
):
    params = {}

    join_researcher = str()
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    query_filters = str()
    filter_pagination = str()

    if filters.year:
        params['year'] = filters.year
        query_filters += """
            AND rpe.start_year >= %(year)s OR rpe.end_year >= %(year)s
            """

    if filters.dep_id or filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = rpe.researcher_id
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

    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        join_researcher = """
            LEFT JOIN researcher r
                ON r.id = rpe.researcher_id
            """
        query_filters += """
            AND rpe.researcher_id = %(researcher_id)s
            """

    if filters.institution:
        params['institution'] = filters.institution.split(';')
        join_researcher = """
            LEFT JOIN researcher r
                ON r.id = rpe.researcher_id
            """
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        query_filters += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id:
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = str(filters.graduate_program_id)
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = rpe.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = rpe.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if filters.city:
        params['city'] = filters.city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = rpe.researcher_id
            """
        query_filters += """
            AND rp.city = ANY(%(city)s)
            """
    if filters.area:
        params['area'] = filters.area.replace(' ', '_').split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = rpe.researcher_id
            """
        query_filters += """
            AND great_area_ && %(area)s
            """

    if filters.modality:
        filter_distinct = 'DISTINCT'
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = rpe.researcher_id
            """
        query_filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:
        params['graduation'] = filters.graduation.split(';')
        join_researcher = """
            LEFT JOIN researcher r
                ON r.id = rpe.researcher_id
            """
        query_filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    if filters.distinct:
        filter_distinct = 'DISTINCT ON (rpe.researcher_id, rpe.enterprise)'

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
            {query_filters}
        ORDER BY rpe.researcher_id, rpe.enterprise
        {filter_pagination}
        """
    result = await conn.select(SCRIPT_SQL, params)
    return result


async def list_patent(
    conn: Connection,
    filters: DefaultFilters,  # Recebe a instância DefaultFilters
    page: int | None,
    lenght: int | None,
) -> list[PatentProduction]:
    params = {}
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    query_filters = str()
    filter_pagination = str()

    if filters.term:  # Acessando via filters.term
        filter_terms_str, term_params = webseatch_filter('p.title', filters.term)
        query_filters += filter_terms_str
        params.update(term_params)

    if filters.year:  # Acessando via filters.year
        params['year'] = filters.year
        query_filters += """
            AND p.development_year::INT >= %(year)s
            """

    if filters.dep_id or filters.departament:  # Acessando via filters
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = p.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if filters.dep_id:  # Acessando via filters.dep_id
        params['dep_id'] = filters.dep_id
        query_filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if filters.departament:  # Acessando via filters.departament
        params['departament'] = filters.departament.split(';')
        query_filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if filters.researcher_id:  # Acessando via filters.researcher_id
        params['researcher_id'] = str(
            filters.researcher_id
        )  # Convertido para string
        query_filters += """
            AND p.researcher_id = %(researcher_id)s
            """

    if filters.institution:  # Acessando via filters.institution
        params['institution'] = filters.institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        query_filters += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id:  # Acessando via filters.graduate_program_id
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = str(
            filters.graduate_program_id
        )  # Convertido para string
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = p.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:  # Acessando via filters.graduate_program
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = p.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if filters.city:  # Acessando via filters.city
        params['city'] = filters.city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = p.researcher_id
            """
        query_filters += """
            AND rp.city = ANY(%(city)s)
            """
    if filters.area:  # Acessando via filters.area
        params['area'] = filters.area.replace(' ', '_').split(';')
        if (
            not join_researcher_production
        ):  # Adicionado para evitar duplicidade do join
            join_researcher_production = """
                LEFT JOIN researcher_production rp
                    ON rp.researcher_id = p.researcher_id
                """
        query_filters += """
            AND rp.great_area_ && %(area)s
            """

    if filters.modality:  # Acessando via filters.modality
        filter_distinct = 'DISTINCT'
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = p.researcher_id
            """
        query_filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:  # Acessando via filters.graduation
        params['graduation'] = filters.graduation.split(';')
        query_filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    if filters.distinct:
        filter_distinct = 'DISTINCT ON (p.title)'

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
            {query_filters}
        ORDER BY p.title DESC
        {filter_pagination};
        """

    result = await conn.select(SCRIPT_SQL, params)  # Adicionado await
    return result


async def list_brand(
    conn: Connection,
    filters: DefaultFilters,  # Recebe a instância DefaultFilters
    page: int | None,
    lenght: int | None,
):
    params = {}
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    query_filters = str()
    filter_pagination = str()

    if filters.term:  # Acessando via filters.term
        filter_terms_str, term_params = webseatch_filter('b.title', filters.term)
        query_filters += filter_terms_str
        params.update(term_params)

    if filters.year:  # Acessando via filters.year
        params['year'] = filters.year
        query_filters += """
            AND b.year::INT >= %(year)s
            """

    if filters.dep_id or filters.departament:  # Acessando via filters
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = b.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if filters.dep_id:  # Acessando via filters.dep_id
        params['dep_id'] = filters.dep_id
        query_filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if filters.departament:  # Acessando via filters.departament
        params['departament'] = filters.departament.split(';')
        query_filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if filters.researcher_id:  # Acessando via filters.researcher_id
        params['researcher_id'] = str(
            filters.researcher_id
        )  # Convertido para string
        query_filters += """
            AND b.researcher_id = %(researcher_id)s
            """

    if filters.institution:  # Acessando via filters.institution
        params['institution'] = filters.institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        query_filters += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id:  # Acessando via filters.graduate_program_id
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = str(
            filters.graduate_program_id
        )  # Convertido para string
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = b.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:  # Acessando via filters.graduate_program
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = b.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if filters.city:  # Acessando via filters.city
        params['city'] = filters.city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = b.researcher_id
            """
        query_filters += """
            AND rp.city = ANY(%(city)s)
            """
    if filters.area:  # Acessando via filters.area
        params['area'] = filters.area.replace(' ', '_').split(';')
        if (
            not join_researcher_production
        ):  # Adicionado para evitar duplicidade do join
            join_researcher_production = """
                LEFT JOIN researcher_production rp
                    ON rp.researcher_id = b.researcher_id
                """
        query_filters += """
            AND rp.great_area_ && %(area)s
            """

    if filters.modality:  # Acessando via filters.modality
        filter_distinct = 'DISTINCT'
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = b.researcher_id
            """
        query_filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:  # Acessando via filters.graduation
        params['graduation'] = filters.graduation.split(';')
        query_filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    if filters.distinct:
        filter_distinct = 'DISTINCT ON (b.title)'

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
            {query_filters}
        ORDER BY b.title DESC
        {filter_pagination};
        """
    result = await conn.select(SCRIPT_SQL, params)  # Adicionado await
    return result


async def list_book(
    conn: Connection,
    filters: DefaultFilters,
    page: int | None,
    lenght: int | None,
):
    params = {}
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    query_filters = str()
    filter_pagination = str()

    if filters.term:
        filter_terms_str, term_params = webseatch_filter(
            'bp.title', filters.term
        )
        query_filters += filter_terms_str
        params.update(term_params)

    if filters.year:
        params['year'] = filters.year
        query_filters += """
            AND bp.year::INT >= %(year)s
            """

    if filters.dep_id or filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = bp.researcher_id
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

    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        query_filters += """
            AND bp.researcher_id = %(researcher_id)s
            """

    if filters.institution:
        params['institution'] = filters.institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        query_filters += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id:
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = str(
            filters.graduate_program_id
        )  # Convertido para string
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if filters.city:
        params['city'] = filters.city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = bp.researcher_id
            """
        query_filters += """
            AND rp.city = ANY(%(city)s)
            """
    if filters.area:
        params['area'] = filters.area.replace(' ', '_').split(';')
        if not join_researcher_production:
            join_researcher_production = """
                LEFT JOIN researcher_production rp
                    ON rp.researcher_id = bp.researcher_id
                """
        query_filters += """
            AND rp.great_area_ && %(area)s
            """

    if filters.modality:
        filter_distinct = 'DISTINCT'
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = bp.researcher_id
            """
        query_filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:
        params['graduation'] = filters.graduation.split(';')
        query_filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    if filters.distinct:
        filter_distinct = 'DISTINCT ON (bp.title)'

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
            {query_filters}
        ORDER BY bp.title desc
        {filter_pagination};
        """
    result = await conn.select(SCRIPT_SQL, params)
    return result


async def list_bibliographic_production(
    conn: Connection,
    filters: DefaultFilters,
    qualis: str | None,
    page: int | None,
    lenght: int | None,
) -> list[ArticleProduction]:
    params = {}
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_departament = str()
    join_institution = """
        LEFT JOIN institution i
            ON r.institution_id = i.id
        """

    filter_distinct = str()
    query_filters = str()
    filter_pagination = str()

    if filters.term:  # Acessando via filters.term
        filter_terms_str, term_params = webseatch_filter('b.title', filters.term)
        query_filters += filter_terms_str
        params.update(term_params)

    if filters.type:
        params['type'] = filters.type.split(';')
        query_filters += """
            AND b.type = ANY(%(type)s)
            """

    if qualis:  # Parâmetro específico
        params['qualis'] = qualis.split(';')
        query_filters += """
            AND bpa.qualis = ANY(%(qualis)s)
            """

    if filters.year:  # Acessando via filters.year
        params['year'] = filters.year
        query_filters += """
            AND b.year::INT >= %(year)s
            """

    if filters.dep_id or filters.departament:  # Acessando via filters
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = b.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if filters.dep_id:  # Acessando via filters.dep_id
        params['dep_id'] = filters.dep_id
        query_filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if filters.departament:  # Acessando via filters.departament
        params['departament'] = filters.departament.split(';')
        query_filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if filters.researcher_id:  # Acessando via filters.researcher_id
        params['researcher_id'] = str(
            filters.researcher_id
        )  # Convertido para string
        query_filters += """
            AND b.researcher_id = %(researcher_id)s
            """

    if filters.institution:  # Acessando via filters.institution
        params['institution'] = filters.institution.split(';')
        # join_institution já está definido no início da função
        query_filters += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id:  # Acessando via filters.graduate_program_id
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = str(
            filters.graduate_program_id
        )  # Convertido para string
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = b.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:  # Acessando via filters.graduate_program
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = b.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if filters.city:  # Acessando via filters.city
        params['city'] = filters.city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = b.researcher_id
            """
        query_filters += """
            AND rp.city = ANY(%(city)s)
            """
    if filters.area:  # Acessando via filters.area
        params['area'] = filters.area.replace(' ', '_').split(';')
        if (
            not join_researcher_production
        ):  # Adicionado para evitar duplicidade do join
            join_researcher_production = """
                LEFT JOIN researcher_production rp
                    ON rp.researcher_id = b.researcher_id
                """
        query_filters += """
            AND rp.great_area_ && %(area)s
            """

    if filters.modality:  # Acessando via filters.modality
        filter_distinct = 'DISTINCT'
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = b.researcher_id
            """
        query_filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:  # Acessando via filters.graduation
        params['graduation'] = filters.graduation.split(';')
        query_filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    if filters.distinct:
        filter_distinct = 'DISTINCT ON (b.title)'

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
            {join_institution}
            LEFT JOIN openalex_article opa
                ON opa.article_id = b.id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
        WHERE 1 = 1
            {query_filters}
        ORDER BY
            b.title DESC
        {filter_pagination};
        """
    result = await conn.select(SCRIPT_SQL, params)
    return result


async def list_article_production(
    conn: Connection,
    filters: DefaultFilters,
    qualis: str | None,
    page: int | None,
    lenght: int | None,
):
    params = {}
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_departament = str()
    join_institution = """
        LEFT JOIN institution i
            ON r.institution_id = i.id
        """

    filter_distinct = str()
    query_filters = str()
    filter_pagination = str()

    if qualis:
        params['qualis'] = qualis.split(';')
        query_filters += 'AND bpa.qualis = ANY(%(qualis)s)'

    if filters.term:
        filter_terms_str, term_params = webseatch_filter('b.title', filters.term)
        query_filters += filter_terms_str
        params.update(term_params)

    if filters.year:  # Acessando via filters.year
        params['year'] = filters.year
        query_filters += """
            AND b.year::INT >= %(year)s
            """

    if filters.dep_id or filters.departament:  # Acessando via filters
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = b.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if filters.dep_id:  # Acessando via filters.dep_id
        params['dep_id'] = filters.dep_id
        query_filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if filters.departament:  # Acessando via filters.departament
        params['departament'] = filters.departament.split(';')
        query_filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if filters.researcher_id:  # Acessando via filters.researcher_id
        params['researcher_id'] = str(
            filters.researcher_id
        )  # Convertido para string
        query_filters += """
            AND b.researcher_id = %(researcher_id)s
            """

    if filters.institution:  # Acessando via filters.institution
        params['institution'] = filters.institution.split(';')
        # join_institution já está definido no início da função
        query_filters += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id:  # Acessando via filters.graduate_program_id
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = str(
            filters.graduate_program_id
        )  # Convertido para string
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = b.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:  # Acessando via filters.graduate_program
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = b.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if filters.city:  # Acessando via filters.city
        params['city'] = filters.city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = b.researcher_id
            """
        query_filters += """
            AND rp.city = ANY(%(city)s)
            """
    if filters.area:  # Acessando via filters.area
        params['area'] = filters.area.replace(' ', '_').split(';')
        if not join_researcher_production:
            join_researcher_production = """
                LEFT JOIN researcher_production rp
                    ON rp.researcher_id = b.researcher_id
                """
        query_filters += """
            AND rp.great_area_ && %(area)s
            """

    if filters.modality:  # Acessando via filters.modality
        filter_distinct = 'DISTINCT'
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = b.researcher_id
            """
        query_filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:  # Acessando via filters.graduation
        params['graduation'] = filters.graduation.split(';')
        query_filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    if filters.distinct:  # Acessando via filters.distinct
        filter_distinct = 'DISTINCT ON (b.title)'

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            b.id AS id, title, b.year, b.type, doi, bpa.qualis,
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
            {join_institution}
            LEFT JOIN openalex_article opa
                ON opa.article_id = b.id
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
        WHERE 1 = 1
            {query_filters}
        ORDER BY
           b.title DESC
        {filter_pagination};
        """
    result = await conn.select(SCRIPT_SQL, params)
    return result


async def list_book_chapter(
    conn: Connection,
    filters: DefaultFilters,
    page: int | None,
    lenght: int | None,
):
    params = {}
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    query_filters = str()
    filter_pagination = str()

    if filters.term:  # Acessando via filters.term
        filter_terms_str, term_params = webseatch_filter(
            'bp.title', filters.term
        )
        query_filters += filter_terms_str
        params.update(term_params)

    if filters.year:  # Acessando via filters.year
        params['year'] = filters.year
        query_filters += """
            AND bp.year::INT >= %(year)s
            """

    if filters.dep_id or filters.departament:  # Acessando via filters
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = bp.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if filters.dep_id:  # Acessando via filters.dep_id
        params['dep_id'] = filters.dep_id
        query_filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if filters.departament:  # Acessando via filters.departament
        params['departament'] = filters.departament.split(';')
        query_filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if filters.researcher_id:  # Acessando via filters.researcher_id
        params['researcher_id'] = str(
            filters.researcher_id
        )  # Convertido para string
        query_filters += """
            AND bp.researcher_id = %(researcher_id)s
            """

    if filters.institution:  # Acessando via filters.institution
        params['institution'] = filters.institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        query_filters += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id:  # Acessando via filters.graduate_program_id
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = str(
            filters.graduate_program_id
        )  # Convertido para string
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:  # Acessando via filters.graduate_program
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if filters.city:  # Acessando via filters.city
        params['city'] = filters.city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = bp.researcher_id
            """
        query_filters += """
            AND rp.city = ANY(%(city)s)
            """
    if filters.area:  # Acessando via filters.area
        params['area'] = filters.area.replace(' ', '_').split(';')
        if (
            not join_researcher_production
        ):  # Adicionado para evitar duplicidade do join
            join_researcher_production = """
                LEFT JOIN researcher_production rp
                    ON rp.researcher_id = bp.researcher_id
                """
        query_filters += """
            AND rp.great_area_ && %(area)s
            """

    if filters.modality:  # Acessando via filters.modality
        filter_distinct = 'DISTINCT'
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = bp.researcher_id
            """
        query_filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:  # Acessando via filters.graduation
        params['graduation'] = filters.graduation.split(';')
        query_filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    if filters.distinct:
        filter_distinct = 'DISTINCT ON (bp.title)'

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
            {query_filters}
        ORDER BY bp.title DESC
        {filter_pagination};
        """
    result = await conn.select(SCRIPT_SQL, params)  # Adicionado await
    return result


async def list_software(
    conn: Connection,
    filters: DefaultFilters,  # Recebe a instância DefaultFilters
    page: int | None,
    lenght: int | None,
):
    params = {}

    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    query_filters = str()
    filter_pagination = str()

    if filters.term:  # Acessando via filters.term
        filter_terms_str, term_params = webseatch_filter('s.title', filters.term)
        query_filters += filter_terms_str
        params.update(term_params)

    if filters.year:  # Acessando via filters.year
        params['year'] = filters.year
        query_filters += """
            AND s.year::INT >= %(year)s
            """

    if filters.dep_id or filters.departament:  # Acessando via filters
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = s.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if filters.dep_id:  # Acessando via filters.dep_id
        params['dep_id'] = filters.dep_id
        query_filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if filters.departament:  # Acessando via filters.departament
        params['departament'] = filters.departament.split(';')
        query_filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if filters.researcher_id:  # Acessando via filters.researcher_id
        params['researcher_id'] = str(
            filters.researcher_id
        )  # Convertido para string
        query_filters += """
            AND s.researcher_id = %(researcher_id)s
            """

    if filters.institution:  # Acessando via filters.institution
        params['institution'] = filters.institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        query_filters += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id:  # Acessando via filters.graduate_program_id
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = str(
            filters.graduate_program_id
        )  # Convertido para string
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = s.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:  # Acessando via filters.graduate_program
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = s.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if filters.city:  # Acessando via filters.city
        params['city'] = filters.city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = s.researcher_id
            """
        query_filters += """
            AND rp.city = ANY(%(city)s)
            """
    if filters.area:  # Acessando via filters.area
        params['area'] = filters.area.replace(' ', '_').split(';')
        if (
            not join_researcher_production
        ):  # Adicionado para evitar duplicidade do join
            join_researcher_production = """
                LEFT JOIN researcher_production rp
                    ON rp.researcher_id = s.researcher_id
                """
        query_filters += """
            AND rp.great_area_ && %(area)s
            """

    if filters.modality:  # Acessando via filters.modality
        filter_distinct = 'DISTINCT'
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = s.researcher_id
            """
        query_filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:  # Acessando via filters.graduation
        params['graduation'] = filters.graduation.split(';')
        query_filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    if filters.distinct:  # Acessando via filters.distinct
        filter_distinct = 'DISTINCT ON (s.title)'

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
            {query_filters}
        ORDER BY s.title DESC
        {filter_pagination};
        """
    result = await conn.select(SCRIPT_SQL, params)  # Adicionado await
    return result


async def list_researcher_report(
    conn: Connection,
    filters: DefaultFilters,  # Recebe a instância DefaultFilters
    page: int | None,
    lenght: int | None,
):
    params = {}
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    query_filters = str()
    filter_pagination = str()

    if filters.term:  # Acessando via filters.term
        filter_terms_str, term_params = webseatch_filter(
            'rr.title', filters.term
        )
        query_filters += filter_terms_str
        params.update(term_params)

    if filters.year:  # Acessando via filters.year
        params['year'] = filters.year
        query_filters += """
            AND rr.year::INT >= %(year)s
            """

    if filters.dep_id or filters.departament:  # Acessando via filters
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = rr.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if filters.dep_id:  # Acessando via filters.dep_id
        params['dep_id'] = filters.dep_id
        query_filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if filters.departament:  # Acessando via filters.departament
        params['departament'] = filters.departament.split(';')
        query_filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if filters.researcher_id:  # Acessando via filters.researcher_id
        params['researcher_id'] = str(
            filters.researcher_id
        )  # Convertido para string
        query_filters += """
            AND rr.researcher_id = %(researcher_id)s
            """

    if filters.institution:  # Acessando via filters.institution
        params['institution'] = filters.institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        query_filters += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id:  # Acessando via filters.graduate_program_id
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = str(
            filters.graduate_program_id
        )  # Convertido para string
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = rr.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:  # Acessando via filters.graduate_program
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = rr.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if filters.city:  # Acessando via filters.city
        params['city'] = filters.city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = rr.researcher_id
            """
        query_filters += """
            AND rp.city = ANY(%(city)s)
            """
    if filters.area:  # Acessando via filters.area
        params['area'] = filters.area.replace(' ', '_').split(';')
        if (
            not join_researcher_production
        ):  # Adicionado para evitar duplicidade do join
            join_researcher_production = """
                LEFT JOIN researcher_production rp
                    ON rp.researcher_id = rr.researcher_id
                """
        query_filters += """
            AND rp.great_area_ && %(area)s
            """

    if filters.modality:  # Acessando via filters.modality
        filter_distinct = 'DISTINCT'
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = rr.researcher_id
            """
        query_filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:  # Acessando via filters.graduation
        params['graduation'] = filters.graduation.split(';')
        query_filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    if filters.distinct:
        filter_distinct = 'DISTINCT ON (rr.title)'

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
            {query_filters}
        ORDER BY rr.title DESC
        {filter_pagination};
        """
    result = await conn.select(SCRIPT_SQL, params)  # Adicionado await
    return result


async def list_guidance_production(
    conn: Connection,
    filters: DefaultFilters,  # Recebe a instância DefaultFilters
    page: int | None,
    lenght: int | None,
):
    params = {}

    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    query_filters = str()
    filter_pagination = str()

    if filters.term:  # Acessando via filters.term
        filter_terms_str, term_params = webseatch_filter('g.title', filters.term)
        query_filters += filter_terms_str
        params.update(term_params)

    if filters.year:  # Acessando via filters.year
        params['year'] = filters.year
        query_filters += """
            AND g.year::INT >= %(year)s
            """

    if filters.dep_id or filters.departament:  # Acessando via filters
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = g.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if filters.dep_id:  # Acessando via filters.dep_id
        params['dep_id'] = filters.dep_id
        query_filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if filters.departament:  # Acessando via filters.departament
        params['departament'] = filters.departament.split(';')
        query_filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if filters.researcher_id:  # Acessando via filters.researcher_id
        params['researcher_id'] = str(
            filters.researcher_id
        )  # Convertido para string
        query_filters += """
            AND g.researcher_id = %(researcher_id)s
            """

    if filters.institution:  # Acessando via filters.institution
        params['institution'] = filters.institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        query_filters += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id:  # Acessando via filters.graduate_program_id
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = str(
            filters.graduate_program_id
        )  # Convertido para string
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = g.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:  # Acessando via filters.graduate_program
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = g.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if filters.city:  # Acessando via filters.city
        params['city'] = filters.city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = g.researcher_id
            """
        query_filters += """
            AND rp.city = ANY(%(city)s)
            """
    if filters.area:  # Acessando via filters.area
        params['area'] = filters.area.replace(' ', '_').split(';')
        if (
            not join_researcher_production
        ):  # Adicionado para evitar duplicidade do join
            join_researcher_production = """
                LEFT JOIN researcher_production rp
                    ON rp.researcher_id = g.researcher_id
                """
        query_filters += """
            AND rp.great_area_ && %(area)s
            """

    if filters.modality:  # Acessando via filters.modality
        filter_distinct = 'DISTINCT'
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = g.researcher_id
            """
        query_filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:  # Acessando via filters.graduation
        params['graduation'] = filters.graduation.split(';')
        query_filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)
    if filters.distinct:  # Acessando via filters.distinct
        filter_distinct = 'DISTINCT ON (g.title)'

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
            {query_filters}
        ORDER BY g.title DESC
        {filter_pagination};
        """
    result = await conn.select(SCRIPT_SQL, params)  # Adicionado await
    return result


async def list_researcher_production_events(
    conn: Connection,
    filters: DefaultFilters,  # Recebe a instância DefaultFilters
    page: int | None,
    lenght: int | None,
):
    params = {}

    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    query_filters = "AND bp.type = 'WORK_IN_EVENT'"  # Mantido filtro fixo
    filter_pagination = str()

    if filters.term:  # Acessando via filters.term
        filter_terms_str, term_params = webseatch_filter(
            'bp.title', filters.term
        )
        query_filters += filter_terms_str
        params.update(term_params)

    if filters.year:  # Acessando via filters.year
        params['year'] = filters.year
        query_filters += """
            AND bp.year_::INT >= %(year)s
            """

    if filters.dep_id or filters.departament:  # Acessando via filters
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = bp.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if filters.dep_id:  # Acessando via filters.dep_id
        params['dep_id'] = filters.dep_id
        query_filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if filters.departament:  # Acessando via filters.departament
        params['departament'] = filters.departament.split(';')
        query_filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if filters.researcher_id:  # Acessando via filters.researcher_id
        params['researcher_id'] = str(
            filters.researcher_id
        )  # Convertido para string
        query_filters += """
            AND bp.researcher_id = %(researcher_id)s
            """

    if filters.institution:  # Acessando via filters.institution
        params['institution'] = filters.institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        query_filters += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id:  # Acessando via filters.graduate_program_id
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = str(
            filters.graduate_program_id
        )  # Convertido para string
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:  # Acessando via filters.graduate_program
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if filters.city:  # Acessando via filters.city
        params['city'] = filters.city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = bp.researcher_id
            """
        query_filters += """
            AND rp.city = ANY(%(city)s)
            """
    if filters.area:  # Acessando via filters.area
        params['area'] = filters.area.replace(' ', '_').split(';')
        if (
            not join_researcher_production
        ):  # Adicionado para evitar duplicidade do join
            join_researcher_production = """
                LEFT JOIN researcher_production rp
                    ON rp.researcher_id = bp.researcher_id
                """
        query_filters += """
            AND rp.great_area_ && %(area)s
            """

    if filters.modality:  # Acessando via filters.modality
        filter_distinct = 'DISTINCT'
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = bp.researcher_id
            """
        query_filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:  # Acessando via filters.graduation
        params['graduation'] = filters.graduation.split(';')
        query_filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)
    if filters.distinct:  # Acessando via filters.distinct
        filter_distinct = 'DISTINCT ON (bp.title)'

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
            {query_filters}
        ORDER BY bp.title DESC
        {filter_pagination};
        """
    result = await conn.select(SCRIPT_SQL, params)  # Adicionado await
    return result


async def list_research_projects(
    conn: Connection,
    filters: DefaultFilters,  # Recebe a instância DefaultFilters
    page: int | None,
    lenght: int | None,
):
    params = {}

    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    query_filters = str()
    filter_pagination = str()

    if filters.term:  # Acessando via filters.term
        filter_terms_str, term_params = webseatch_filter(
            'rp.project_name', filters.term
        )
        query_filters += filter_terms_str
        params.update(term_params)

    if filters.year:  # Acessando via filters.year
        params['year'] = filters.year
        query_filters += """
            AND rp.start_year >= %(year)s
            """

    if filters.dep_id or filters.departament:  # Acessando via filters
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = rp.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if filters.dep_id:  # Acessando via filters.dep_id
        params['dep_id'] = filters.dep_id
        query_filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if filters.departament:  # Acessando via filters.departament
        params['departament'] = filters.departament.split(';')
        query_filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if filters.researcher_id:  # Acessando via filters.researcher_id
        params['researcher_id'] = str(
            filters.researcher_id
        )  # Convertido para string
        query_filters += """
            AND rp.researcher_id = %(researcher_id)s
            """

    if filters.institution:  # Acessando via filters.institution
        params['institution'] = filters.institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        query_filters += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id:  # Acessando via filters.graduate_program_id
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = str(
            filters.graduate_program_id
        )  # Convertido para string
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = rp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:  # Acessando via filters.graduate_program
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = rp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if filters.city:  # Acessando via filters.city
        params['city'] = filters.city.split(';')
        # Renomeei a tabela de join para evitar conflito com 'rp' (research_project)
        join_researcher_production = """
            LEFT JOIN researcher_production rp_prod
                ON rp_prod.researcher_id = rp.researcher_id
            """
        query_filters += """
            AND rp_prod.city = ANY(%(city)s)
            """
    if filters.area:  # Acessando via filters.area
        params['area'] = filters.area.replace(' ', '_').split(';')
        if (
            not join_researcher_production
        ):  # Adicionado para evitar duplicidade do join
            join_researcher_production = """
                LEFT JOIN researcher_production rp_prod
                    ON rp_prod.researcher_id = rp.researcher_id
                """
        query_filters += """
            AND rp_prod.great_area_ && %(area)s
            """

    if filters.modality:  # Acessando via filters.modality
        filter_distinct = 'DISTINCT'
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = rp.researcher_id
            """
        query_filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:  # Acessando via filters.graduation
        params['graduation'] = filters.graduation.split(';')
        query_filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    if filters.distinct:  # Acessando via filters.distinct
        filter_distinct = 'DISTINCT ON (rp.project_name)'

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
            {query_filters}
        ORDER BY rp.project_name DESC
        {filter_pagination};
        """

    result = await conn.select(SCRIPT_SQL, params)  # Adicionado await
    return result


async def list_papers_magazine(
    conn: Connection,
    filters: DefaultFilters,  # Recebe a instância DefaultFilters
    page: int | None,
    lenght: int | None,
):
    params = {}

    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filter_distinct = str()
    query_filters = str()
    filter_pagination = str()

    if filters.term:  # Acessando via filters.term
        filter_terms_str, term_params = webseatch_filter(
            'bp.title', filters.term
        )
        query_filters += filter_terms_str
        params.update(term_params)

    if filters.year:  # Acessando via filters.year
        params['year'] = filters.year
        query_filters += """
            AND bp.year_::INT >= %(year)s
            """

    if filters.dep_id or filters.departament:  # Acessando via filters
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr
                ON dpr.researcher_id = bp.researcher_id
            INNER JOIN ufmg.departament dp
                ON dp.dep_id = dpr.dep_id
            """
    if filters.dep_id:  # Acessando via filters.dep_id
        params['dep_id'] = filters.dep_id
        query_filters += """
            AND dp.dep_id = %(dep_id)s
            """

    if filters.departament:  # Acessando via filters.departament
        params['departament'] = filters.departament.split(';')
        query_filters += """
            AND dp.dep_nom = ANY(%(departament)s)
            """

    if filters.researcher_id:  # Acessando via filters.researcher_id
        params['researcher_id'] = str(
            filters.researcher_id
        )  # Convertido para string
        query_filters += """
            AND bp.researcher_id = %(researcher_id)s
            """

    if filters.institution:  # Acessando via filters.institution
        params['institution'] = filters.institution.split(';')
        join_institution = """
            INNER JOIN institution i
                ON r.institution_id = i.id
            """
        query_filters += """
            AND i.name = ANY(%(institution)s)
            """

    if filters.graduate_program_id:  # Acessando via filters.graduate_program_id
        filter_distinct = 'DISTINCT'
        params['graduate_program_id'] = str(
            filters.graduate_program_id
        )  # Convertido para string
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:  # Acessando via filters.graduate_program
        filter_distinct = 'DISTINCT'
        params['graduate_program'] = filters.graduate_program.split(';')
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = bp.researcher_id
            INNER JOIN graduate_program gp
                ON gpr.graduate_program_id = gp.graduate_program_id
            """
        query_filters += """
            AND gp.name = ANY(%(graduate_program)s)
            """

    if filters.city:  # Acessando via filters.city
        params['city'] = filters.city.split(';')
        join_researcher_production = """
            LEFT JOIN researcher_production rp
                ON rp.researcher_id = bp.researcher_id
            """
        query_filters += """
            AND rp.city = ANY(%(city)s)
            """
    if filters.area:  # Acessando via filters.area
        params['area'] = filters.area.replace(' ', '_').split(';')
        if (
            not join_researcher_production
        ):  # Adicionado para evitar duplicidade do join
            join_researcher_production = """
                LEFT JOIN researcher_production rp
                    ON rp.researcher_id = bp.researcher_id
                """
        query_filters += """
            AND rp.great_area_ && %(area)s
            """

    if filters.modality:  # Acessando via filters.modality
        filter_distinct = 'DISTINCT'
        params['modality'] = filters.modality.split(';')
        join_foment = """
            INNER JOIN foment f
                ON f.researcher_id = bp.researcher_id
            """
        query_filters += """
            AND f.modality_name = ANY(%(modality)s)
            """

    if filters.graduation:  # Acessando via filters.graduation
        params['graduation'] = filters.graduation.split(';')
        query_filters += """
            AND r.graduation = ANY(%(graduation)s)
            """

    if page and lenght:
        filter_pagination = pagination(page, lenght)

    if filters.distinct:  # Acessando via filters.distinct
        filter_distinct = 'DISTINCT ON (bp.title)'

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
            {query_filters}
        ORDER BY bp.title DESC
        {filter_pagination};
        """

    result = await conn.select(SCRIPT_SQL, params)  # Adicionado await
    return result


# ---


async def get_book_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}

    join_dep = str()
    query_filters = str()
    filter_distinct = str()

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

    join_program = str()
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
        term_filter, term_params = webseatch_filter('bp.title', filters.term)
        params.update(term_params)

    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        query_filters += 'AND bp.researcher_id = %(researcher_id)s'

    if filters.graduate_program_id:
        params['program_id'] = str(filters.graduate_program_id)
        join_program = """
            LEFT JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
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
        GROUP BY
            bp.year;
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
        term_filter, term_params = webseatch_filter('bp.title', filters.term)
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
        GROUP BY bp.year;
    """
    return await conn.select(SCRIPT_SQL, params)


async def get_researcher_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}
    join_filter = str()  # Specific join for the 'type' filter (bibliographic production, patent, area, event)
    type_filter = str()  # Specific where clause for the 'type' filter
    year_filter = str()  # Specific year filter for certain 'type' cases
    filter_name = str()  # Specific filter for researcher name

    # Initialize all potential shared joins as empty strings
    join_departament = str()
    join_program = str()
    join_institution = str()
    join_researcher_production = str()  # Used for city and area
    join_foment = str()

    where_extra = str()  # Accumulates general 'AND' conditions
    count_among = 'COUNT(*) as among'  # Default count

    match filters.type:
        case 'ABSTRACT':
            type_filter, term_params = webseatch_filter(
                'r.abstract', filters.term
            )
            params.update(term_params)
            count_among = 'COUNT(*) AS among'
        case ('BOOK' | 'BOOK_CHAPTER' | 'ARTICLE' | 'WORK_IN_EVENT' | 'TEXT_IN_NEWSPAPER_MAGAZINE'):  # fmt: skip
            count_among = 'COUNT(DISTINCT bp.title) AS among'  # Use DISTINCT here to count unique titles
            join_filter = f"""
                INNER JOIN bibliographic_production bp
                    ON bp.researcher_id = r.id AND bp.type = '{filters.type}'
            """
            if filters.term:  # Only apply if filters.term exists
                term_filter_str, term_params_bp = webseatch_filter(
                    'bp.title', filters.term
                )
                type_filter += term_filter_str
                params.update(term_params_bp)
            if filters.year:  # Only apply if filters.year exists
                year_filter = 'AND bp.year::int >= %(year)s'
                params['year'] = filters.year
        case 'PATENT':
            count_among = 'COUNT(DISTINCT p.title) AS among'  # Use DISTINCT here
            join_filter = 'INNER JOIN patent p ON p.researcher_id = r.id'
            if filters.term:  # Only apply if filters.term exists
                term_filter_str, term_params_p = webseatch_filter(
                    'p.title', filters.term
                )
                type_filter += term_filter_str
                params.update(term_params_p)
            if filters.year:  # Only apply if filters.year exists
                year_filter = 'AND p.development_year::int >= %(year)s'
                params['year'] = filters.year
        case 'AREA':
            count_among = (
                'COUNT(DISTINCT rp.researcher_id) AS among'  # Use DISTINCT here
            )
            join_filter = """
                INNER JOIN researcher_production rp
                    ON rp.researcher_id = r.id
                """
            if filters.term:  # Only apply if filters.term exists
                term_filter_str, term_params_rp = webseatch_filter(
                    'rp.great_area', filters.term
                )
                type_filter += term_filter_str
                params.update(term_params_rp)
        case 'EVENT':
            count_among = 'COUNT(DISTINCT e.title) AS among'  # Use DISTINCT here
            join_filter = """
                INNER JOIN event_organization e
                    ON e.researcher_id = r.id
                """
            if filters.term:  # Only apply if filters.term exists
                term_filter_str, term_params_e = webseatch_filter(
                    'e.title', filters.term
                )
                type_filter += term_filter_str
                params.update(term_params_e)
            if filters.year:  # Only apply if filters.year exists
                year_filter = 'AND e.year::int >= %(year)s'
                params['year'] = filters.year
        case 'NAME':
            count_among = 'COUNT(DISTINCT r.id) AS among'  # Already counting researchers, so use DISTINCT
            if filters.term:  # Only apply if filters.term exists
                name_filter_str, term_params_name = names_filter(
                    'r.name', filters.term
                )
                filter_name += name_filter_str
                params.update(term_params_name)
        case _:
            # Default case for when filters.type is not specified or doesn't match
            # count_among remains 'COUNT(*) as among' (for total researchers)
            pass

    # Consolidated logic for departament joins and filters
    if filters.dep_id or filters.departament:
        if not join_departament:  # Ensure join is added only once
            join_departament = """
                INNER JOIN ufmg.departament_researcher dpr
                    ON dpr.researcher_id = r.id
                INNER JOIN ufmg.departament dp
                    ON dp.dep_id = dpr.dep_id
                """
        if filters.dep_id:
            params['dep_id'] = filters.dep_id.split(';')
            where_extra += """
                AND dp.dep_id = ANY(%(dep_id)s)
                """
        if filters.departament:
            params['departament'] = filters.departament.split(';')
            where_extra += """
                AND dp.dep_nom = ANY(%(departament)s)
                """

    # Consolidated logic for graduate program joins and filters
    if filters.graduate_program_id or filters.graduate_program:
        if not join_program:  # Ensure join is added only once
            join_program = """
                INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
                INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
                """
        if filters.graduate_program_id:
            params['program_id'] = str(filters.graduate_program_id)
            where_extra += """
                AND gpr.graduate_program_id = %(program_id)s
                AND gpr.type_ = 'PERMANENTE'
                """
        if filters.graduate_program:
            params['graduate_program'] = filters.graduate_program.split(';')
            where_extra += """
                AND gp.name = ANY(%(graduate_program)s)
                AND gpr.type_ = 'PERMANENTE'
                """

    # Filter by researcher ID (does not require a new join, `r` is always available)
    filter_id = str()
    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        filter_id = 'AND r.id = %(researcher_id)s'

    # Filter by institution (uses subquery to avoid adding `join_institution` if not already needed, or if it's already there)
    filter_institution = ''
    if filters.institution:
        params['institution'] = filters.institution.split(';')
        filter_institution = """
            AND r.institution_id IN (
                SELECT id FROM institution WHERE name = ANY(%(institution)s)
            )
        """

    # Consolidated logic for researcher_production joins (city and area)
    if filters.city or filters.area:
        if not join_researcher_production:  # Ensure join is added only once
            join_researcher_production = """
                LEFT JOIN researcher_production rp_prod ON rp_prod.researcher_id = r.id
            """
        if filters.city:
            params['city'] = filters.city.split(';')
            where_extra += 'AND rp_prod.city = ANY(%(city)s)'
        if filters.area:
            params['area'] = filters.area.replace(' ', '_').split(';')
            # The original code had `rp_area.great_area` but the join used `rp_city`. Corrected to rp_prod.
            where_extra += """
                AND STRING_TO_ARRAY(REPLACE(rp_prod.great_area, ' ', '_'), ';') && %(area)s
            """

    # Consolidated logic for foment joins
    if filters.modality:
        if not join_foment:  # Ensure join is added only once
            join_foment = """
                INNER JOIN foment f ON f.researcher_id = r.id
            """
        params['modality'] = filters.modality.split(';')
        if filters.modality != '*':  # Only add filter if not wildcard
            where_extra += 'AND f.modality_name = ANY(%(modality)s)'

    # Filter by graduation (does not require a new join, `r` is always available)
    if filters.graduation:
        params['graduation'] = filters.graduation.split(';')
        where_extra += 'AND r.graduation = ANY(%(graduation)s)'

    SCRIPT_SQL = f"""
        SELECT COUNT(DISTINCT r.id) AS researcher_count,
                COUNT(DISTINCT r.orcid) AS orcid_count,
                COUNT(DISTINCT opr.scopus) AS scopus_count,
                {count_among}
        FROM researcher r
        LEFT JOIN openalex_researcher opr
            ON opr.researcher_id = r.id
            {join_filter}
            {join_departament}
            {join_program}
            {join_researcher_production}
            {join_foment}
        WHERE 1 = 1
            {type_filter}
            {filter_id}
            {filter_name}
            {filter_institution}
            {year_filter}
            {where_extra}
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
        term_filter, term_params = webseatch_filter('bp.title', filters.term)
        params.update(term_params)

    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        query_filters += 'AND bp.researcher_id = %(researcher_id)s'

    if filters.graduate_program_id:
        params['program_id'] = str(filters.graduate_program_id)
        join_program = """
            LEFT JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
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

    join_graduate_program = str()
    if filters.graduate_program:
        params['graduate_program'] = filters.graduate_program.split(';')
        join_graduate_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = r.id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        query_filters += """
            AND gp.name = ANY(%(graduate_program)s)
            AND gpr.type_ = 'PERMANENTE'
            """

    if filters.year:
        params['year'] = filters.year
        query_filters += 'AND bp.year::int >= %(year)s'

    if qualis:
        params['qualis'] = qualis.split(';')
        query_filters += 'AND bpa.qualis = ANY(%(qualis)s)'

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
            {join_production}
            {join_area}
            {join_dep}
        WHERE 1 = 1
            {query_filters}
            {term_filter}
        GROUP BY
            bp.year;
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
        term_filter, term_params = webseatch_filter('p.title', filters.term)
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
        GROUP BY p.development_year;
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
        filter_distinct = 'DISTINCT'

    if filters.departament:
        filter_distinct = 'DISTINCT'
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
        filter_distinct = 'DISTINCT'
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
        term_filter, term_params = webseatch_filter('g.title', filters.term)
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
        filter_distinct = 'DISTINCT'
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
        filter_distinct = 'DISTINCT'
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
        filter_distinct = 'DISTINCT'

    term_filter = str()
    if filters.term:
        term_filter, term_params = webseatch_filter('s.title', filters.term)
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
    return await conn.select(SCRIPT_SQL, params)


async def get_research_report_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}

    join_researcher = str()
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filters_sql = str()

    if filters.term:
        filter_terms_str, term_params = webseatch_filter(
            'rr.title', filters.term
        )
        filters_sql += filter_terms_str
        params.update(term_params)

    if filters.year:
        params['year'] = filters.year
        filters_sql += """
            AND rr.year >= %(year)s
            """

    if filters.dep_id or filters.departament:
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

    if filters.graduate_program_id:
        params['graduate_program_id'] = str(filters.graduate_program_id)
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = rr.researcher_id
            """
        filters_sql += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:
        params['graduate_program'] = filters.graduate_program.split(';')
        if not join_program:
            join_program = """
                INNER JOIN graduate_program_researcher gpr
                    ON gpr.researcher_id = rr.researcher_id
                INNER JOIN graduate_program gp
                    ON gpr.graduate_program_id = gp.graduate_program_id
                """
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

    if filters.graduation:
        if not join_researcher:
            join_researcher = """
                INNER JOIN researcher r
                    ON rr.researcher_id = r.id
                """
        params['graduation'] = filters.graduation.split(';')
        filters_sql += """
            AND r.graduation = ANY(%(graduation)s)
            """

    SCRIPT_SQL = f"""
        SELECT
            COUNT(*) as among,
            year
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
        GROUP BY year
        ORDER BY
            year DESC
        """
    return await conn.select(SCRIPT_SQL, params)


async def get_papers_magazine_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}

    join_researcher = str()
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filters_sql = str()

    if filters.term:
        filter_terms_str, term_params = webseatch_filter(
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

    if filters.graduate_program_id:
        params['graduate_program_id'] = str(filters.graduate_program_id)
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = bp.researcher_id
            """
        filters_sql += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:
        params['graduate_program'] = filters.graduate_program.split(';')
        if not join_program:
            join_program = """
                INNER JOIN graduate_program_researcher gpr
                    ON gpr.researcher_id = bp.researcher_id
                INNER JOIN graduate_program gp
                    ON gpr.graduate_program_id = gp.graduate_program_id
                """
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
        if not join_researcher:
            join_researcher = """
                INNER JOIN researcher r
                    ON bp.researcher_id = r.id
                """
        params['graduation'] = filters.graduation.split(';')
        filters_sql += """
            AND r.graduation = ANY(%(graduation)s)
            """

    SCRIPT_SQL = f"""
        SELECT COUNT(*) as among, year AS year
        FROM public.bibliographic_production bp
            {join_researcher}
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
        WHERE type = 'TEXT_IN_NEWSPAPER_MAGAZINE'
            {filters_sql}
        GROUP BY year
        ORDER BY
            year DESC
        """
    return await conn.select(SCRIPT_SQL, params)


async def get_speaker_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}

    join_researcher = str()
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()

    filters_sql = str()

    if filters.term:
        filter_terms_str, term_params = webseatch_filter(
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

    if filters.graduate_program_id:
        params['graduate_program_id'] = str(filters.graduate_program_id)
        join_program = """
            INNER JOIN graduate_program_researcher gpr
                ON gpr.researcher_id = pe.researcher_id
            """
        filters_sql += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:
        params['graduate_program'] = filters.graduate_program.split(';')
        if not join_program:
            join_program = """
                INNER JOIN graduate_program_researcher gpr
                    ON gpr.researcher_id = pe.researcher_id
                INNER JOIN graduate_program gp
                    ON gpr.graduate_program_id = gp.graduate_program_id
                """
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
        if not join_researcher:
            join_researcher = """
                INNER JOIN researcher r
                    ON pe.researcher_id = r.id
                """
        params['graduation'] = filters.graduation.split(';')
        filters_sql += """
            AND r.graduation = ANY(%(graduation)s)
            """

    SCRIPT_SQL = f"""
        SELECT pe.year,
            SUM(CASE WHEN nature = 'Congresso' THEN 1 ELSE 0 END) AS congress,
            SUM(CASE WHEN nature = 'Encontro' THEN 1 ELSE 0 END) AS meeting,
            SUM(CASE WHEN nature = 'Oficina' THEN 1 ELSE 0 END) AS workshop,
            SUM(CASE WHEN nature = 'Outra' THEN 1 ELSE 0 END) AS other,
            SUM(CASE WHEN nature = 'Seminário' THEN 1 ELSE 0 END) AS seminar,
            SUM(CASE WHEN nature = 'Simpósio' THEN 1 ELSE 0 END) AS symposium
        FROM public.participation_events pe
            {join_researcher}
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
        WHERE 1=1
            {filters_sql}
        GROUP BY
            pe.year
        """
    return await conn.select(SCRIPT_SQL, params)


async def get_brand_metrics(
    conn: Connection,
    nature,
    filters: DefaultFilters,
):
    params = {}
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()
    join_researcher = str()

    filter_distinct = str()
    if filters.distinct:
        filter_distinct = 'DISTINCT'

    filters_sql = str()

    if nature:
        params['nature'] = nature.split(';')
        filters_sql += """
            AND b.nature = ANY(%(nature)s)
            """

    if filters.term:
        filter_terms_str, term_params = webseatch_filter('b.title', filters.term)
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

    if filters.graduate_program_id:
        if not join_researcher:
            join_researcher = """
                LEFT JOIN public.researcher r ON r.id = b.researcher_id
                """
        params['graduate_program_id'] = str(filters.graduate_program_id)
        join_program = """
            INNER JOIN public.graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            """
        filters_sql += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:
        params['graduate_program'] = filters.graduate_program.split(';')
        if not join_program:
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
        if not join_researcher:
            join_researcher = """
                LEFT JOIN public.researcher r ON r.id = b.researcher_id
                """
        params['graduation'] = filters.graduation.split(';')
        filters_sql += """
            AND r.graduation = ANY(%(graduation)s)
            """

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            COUNT(*) AS among,
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
        GROUP BY b.year
        ORDER BY b.year;
        """
    return await conn.select(SCRIPT_SQL, params)


async def get_research_project_metrics(
    conn: Connection,
    filters: DefaultFilters,
):
    params = {}
    join_researcher_production = str()
    join_foment = str()
    join_program = str()
    join_institution = str()
    join_departament = str()
    join_researcher = str()

    filter_distinct = str()
    if filters.distinct:
        filter_distinct = 'DISTINCT'

    filters_sql = str()

    if filters.term:
        filter_terms_str, term_params = webseatch_filter('b.title', filters.term)
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

    if filters.graduate_program_id:
        if not join_researcher:
            join_researcher = """
                LEFT JOIN public.researcher r ON r.id = b.researcher_id
                """
        params['graduate_program_id'] = str(filters.graduate_program_id)
        join_program = """
            INNER JOIN public.graduate_program_researcher gpr
                ON gpr.researcher_id = r.id
            """
        filters_sql += """
            AND gpr.graduate_program_id = %(graduate_program_id)s
            """

    if filters.graduate_program:
        params['graduate_program'] = filters.graduate_program.split(';')
        if not join_program:
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
        if not join_researcher:
            join_researcher = """
                LEFT JOIN public.researcher r ON r.id = b.researcher_id
                """
        params['graduation'] = filters.graduation.split(';')
        filters_sql += """
            AND r.graduation = ANY(%(graduation)s)
            """

    SCRIPT_SQL = f"""
        SELECT {filter_distinct}
            COUNT(*) AS among,
            b.start_year AS year
        FROM
            public.research_project b
            {join_researcher}
            {join_researcher_production}
            {join_foment}
            {join_program}
            {join_departament}
            {join_institution}
        WHERE 1 = 1
            {filters_sql}
        GROUP BY b.start_year
        ORDER BY b.start_year;
        """
    return await conn.select(SCRIPT_SQL, params)
