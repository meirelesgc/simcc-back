from sqlalchemy import text


def _format_websearch(sql_string):
    return sql_string.replace('%(', ':').replace(')s', '')


def build_common_filters(filters, table_alias='bp', year_col='year'):
    params = {}
    filters_sql = []
    joins = {
        'departament': '',
        'group': '',
        'institution': '',
        'program': '',
        'researcher_production': '',
        'foment': '',
    }
    distinct_sql = ''

    if getattr(filters, 'star', None):
        params['star'] = filters.star
        filters_sql.append(f' AND {table_alias}.id = ANY(:star)')

    if getattr(filters, 'collection_id', None):
        params['collection_id'] = filters.collection_id
        filters_sql.append(f' AND {table_alias}.id = ANY(:collection_id)')

    if getattr(filters, 'term', None):
        filter_terms, term_params = tools.websearch_filter(
            f'{table_alias}.title', filters.term
        )
        params.update(term_params)
        filters_sql.append(_format_websearch(filter_terms))

    if getattr(filters, 'year', None):
        params['year'] = int(filters.year)
        filters_sql.append(f' AND {table_alias}.{year_col}::INT >= :year')

    if getattr(filters, 'type', None):
        params['type'] = filters.type.split(';')
        filters_sql.append(f' AND {table_alias}.type = ANY(:type)')

    if getattr(filters, 'dep_id', None) or getattr(
        filters, 'departament', None
    ):
        joins['departament'] = f"""
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = {table_alias}.researcher_id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        if getattr(filters, 'dep_id', None):
            params['dep_id'] = filters.dep_id
            filters_sql.append(' AND dp.dep_id = :dep_id')
        if getattr(filters, 'departament', None):
            params['departament'] = filters.departament.split(';')
            filters_sql.append(' AND dp.dep_nom = ANY(:departament)')

    if getattr(filters, 'researcher_id', None):
        params['researcher_id'] = str(filters.researcher_id)
        filters_sql.append(
            f' AND {table_alias}.researcher_id = :researcher_id'
        )

    if getattr(filters, 'lattes_id', None):
        params['lattes_id'] = filters.lattes_id
        filters_sql.append(' AND r.lattes_id = :lattes_id')

    if getattr(filters, 'group_id', None) or getattr(filters, 'group', None):
        joins['group'] = """
            INNER JOIN research_group_researcher rgr ON rgr.researcher_id = r.id
            INNER JOIN research_group rg ON rg.id = rgr.research_group_id
        """
        if getattr(filters, 'group_id', None):
            params['group_id'] = filters.group_id
            filters_sql.append(' AND rgr.research_group_id = :group_id')
        if getattr(filters, 'group', None):
            params['group'] = filters.group.split(';')
            filters_sql.append(' AND rg.name = ANY(:group)')

    if getattr(filters, 'institution', None) or getattr(
        filters, 'institution_id', None
    ):
        joins['institution'] = (
            'INNER JOIN institution i ON r.institution_id = i.id'
        )
        if getattr(filters, 'institution', None):
            params['institution'] = filters.institution.split(';')
            filters_sql.append(' AND i.name = ANY(:institution)')
        if getattr(filters, 'institution_id', None):
            params['institution_id'] = filters.institution_id
            filters_sql.append(' AND i.id = :institution_id')

    if getattr(filters, 'graduate_program_id', None) or getattr(
        filters, 'graduate_program', None
    ):
        distinct_sql = 'DISTINCT'
        joins['program'] = f"""
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = {table_alias}.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        if getattr(filters, 'graduate_program_id', None):
            params['graduate_program_id'] = str(filters.graduate_program_id)
            filters_sql.append(
                ' AND gpr.graduate_program_id = :graduate_program_id'
            )
        if getattr(filters, 'graduate_program', None):
            params['graduate_program'] = filters.graduate_program.split(';')
            filters_sql.append(' AND gp.name = ANY(:graduate_program)')

    if getattr(filters, 'city', None) or getattr(filters, 'area', None):
        joins['researcher_production'] = (
            f'LEFT JOIN researcher_production rp ON rp.researcher_id = {table_alias}.researcher_id'
        )
        if getattr(filters, 'city', None):
            params['city'] = filters.city.split(';')
            filters_sql.append(' AND rp.city = ANY(:city)')
        if getattr(filters, 'area', None):
            params['area'] = filters.area.replace(' ', '_').split(';')
            filters_sql.append(' AND rp.great_area_ && :area')

    if getattr(filters, 'modality', None):
        distinct_sql = 'DISTINCT'
        joins['foment'] = (
            f'INNER JOIN foment f ON f.researcher_id = {table_alias}.researcher_id'
        )
        params['modality'] = filters.modality.split(';')
        filters_sql.append(' AND f.modality_name = ANY(:modality)')

    if getattr(filters, 'graduation', None):
        params['graduation'] = filters.graduation.split(';')
        filters_sql.append(' AND r.graduation = ANY(:graduation)')

    filter_pagination = ''
    if getattr(filters, 'page', None) and getattr(filters, 'lenght', None):
        filter_pagination = _format_websearch(
            tools.pagination(filters.page, filters.lenght)
        )

    if getattr(filters, 'distinct', None) == '1':
        distinct_sql = f'DISTINCT ON ({table_alias}.title)'

    return {
        'params': params,
        'filters_sql': ''.join(filters_sql),
        'joins': joins,
        'distinct_sql': distinct_sql,
        'filter_pagination': filter_pagination,
    }


async def list_papers_magazine(session, filters):
    cf = build_common_filters(filters, table_alias='bp', year_col='year_')

    FILTERS_SQL = (
        "AND bp.type = 'TEXT_IN_NEWSPAPER_MAGAZINE'" + cf['filters_sql']
    )

    SCRIPT_SQL = f"""
        SELECT {cf['distinct_sql']}
            title, title_en, nature, language, means_divulgation, homepage,
            relevance, scientific_divulgation, authors, year_, r.name,
            bp.id, bp.researcher_id, r.lattes_id
        FROM public.bibliographic_production bp
            INNER JOIN researcher r ON bp.researcher_id = r.id
            {cf['joins']['researcher_production']}
            {cf['joins']['foment']}
            {cf['joins']['program']}
            {cf['joins']['departament']}
            {cf['joins']['institution']}
            {cf['joins']['group']}
        WHERE 1 = 1
            {FILTERS_SQL}
        ORDER BY {'bp.title, bp.year_ DESC' if cf['distinct_sql'] else 'bp.year_ DESC'}
        {cf['filter_pagination']};
    """
    result = await session.execute(text(SCRIPT_SQL), cf['params'])
    return result.mappings().all()


async def list_bibliographic_production(session, filters, qualis: str | None):
    cf = build_common_filters(filters, table_alias='b', year_col='year')

    params = cf['params']
    FILTERS_SQL = cf['filters_sql']

    if qualis:
        params['qualis'] = qualis.split(';')
        FILTERS_SQL += ' AND bpa.qualis = ANY(:qualis)'

    SCRIPT_SQL = f"""
        SELECT {cf['distinct_sql']}
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
            {cf['joins']['institution']}
            {cf['joins']['researcher_production']}
            {cf['joins']['foment']}
            {cf['joins']['program']}
            {cf['joins']['departament']}
            {cf['joins']['group']}
        WHERE 1 = 1
            {FILTERS_SQL}
        ORDER BY {'b.title, b.year DESC' if cf['distinct_sql'] else 'b.year DESC'}
        {cf['filter_pagination']};
    """
    result = await session.execute(text(SCRIPT_SQL), params)
    return result.mappings().all()


async def list_book_chapter(session, filters):
    cf = build_common_filters(filters, table_alias='bp', year_col='year')

    SCRIPT_SQL = f"""
        SELECT {cf['distinct_sql']}
            bp.title, bp.year, bpc.isbn, bpc.publishing_company,
            bp.researcher_id AS researcher, bp.id, r.lattes_id, bp.relevance,
            bp.has_image, r.name
        FROM bibliographic_production bp
            INNER JOIN bibliographic_production_book_chapter bpc ON bpc.bibliographic_production_id = bp.id
            LEFT JOIN researcher r ON r.id = bp.researcher_id
            {cf['joins']['researcher_production']}
            {cf['joins']['foment']}
            {cf['joins']['program']}
            {cf['joins']['departament']}
            {cf['joins']['institution']}
            {cf['joins']['group']}
        WHERE 1 = 1
            {cf['filters_sql']}
        ORDER BY {'bp.title, bp.year DESC' if cf['distinct_sql'] else 'bp.year DESC'}
        {cf['filter_pagination']};
    """
    result = await session.execute(text(SCRIPT_SQL), cf['params'])
    return result.mappings().all()


async def list_book(session, filters):
    cf = build_common_filters(filters, table_alias='bp', year_col='year')

    SCRIPT_SQL = f"""
        SELECT {cf['distinct_sql']}
            bp.title, bp.year, bpb.isbn AS isbn,
            bpb.publishing_company AS publishing_company,
            bp.researcher_id AS researcher,
            r.lattes_id AS lattes_id, bp.relevance,
            bp.has_image, bp.id, r.name, bpb.stars
        FROM public.bibliographic_production bp
            INNER JOIN public.bibliographic_production_book bpb ON bp.id = bpb.bibliographic_production_id
            INNER JOIN public.researcher r ON r.id = bp.researcher_id
            {cf['joins']['researcher_production']}
            {cf['joins']['foment']}
            {cf['joins']['program']}
            {cf['joins']['departament']}
            {cf['joins']['institution']}
            {cf['joins']['group']}
        WHERE 1 = 1
            {cf['filters_sql']}
        ORDER BY {'bp.title, bp.year DESC' if cf['distinct_sql'] else 'bp.year DESC, bp.title ASC'}
        {cf['filter_pagination']};
    """
    result = await session.execute(text(SCRIPT_SQL), cf['params'])
    return result.mappings().all()
