from simcc.queries.graduate_program_query import GraduateProgramQuery
from simcc.schemas import DefaultFilters


def test_graduate_program_query_base_sql():
    query = GraduateProgramQuery(session=None)
    sql = query.build_sql()
    assert 'FROM public.graduate_program gp' in sql
    assert 'WITH permanent AS' in sql
    assert 'LEFT JOIN institution i ON i.id = gp.institution_id' in sql


def test_graduate_program_query_with_id_filter():
    query = GraduateProgramQuery(session=None)
    filters = DefaultFilters(graduate_program_id='123e4567-e89b-12d3-a456-426614174000')
    query.apply_filters(filters)
    sql = query.build_sql()

    assert 'AND gp.graduate_program_id = :graduate_program_id' in sql
    assert query.params['graduate_program_id'] == '123e4567-e89b-12d3-a456-426614174000'


def test_graduate_program_query_with_university_filter():
    query = GraduateProgramQuery(session=None)
    filters = DefaultFilters(institution='UFMG')
    query.apply_filters(filters)
    sql = query.build_sql()

    assert 'AND i.name ILIKE :institution' in sql
    assert 'LEFT JOIN institution i ON gp.institution_id = i.id' in sql
    assert query.params['institution'] == 'UFMG%'


def test_research_lines_query():
    from simcc.queries.graduate_program_query import ResearchLinesQuery

    query = ResearchLinesQuery(session=None)
    filters = DefaultFilters(
        graduate_program_id='123e4567-e89b-12d3-a456-426614174000',
        institution='UFMG',
        term='bioinformatics',
    )
    query.apply_filters(filters)
    sql = query.build_sql()

    assert 'FROM public.research_lines_programs lgp' in sql
    assert 'AND lgp.graduate_program_id = :graduate_program_id' in sql
    assert 'AND i.name ILIKE :institution' in sql
    assert 'ts_rank' in sql


def test_graduate_program_researcher_query():
    from simcc.queries.graduate_program_query import (
        GraduateProgramResearcherQuery,
    )

    query = GraduateProgramResearcherQuery(session=None)
    filters = DefaultFilters(
        graduate_program_id='123e4567-e89b-12d3-a456-426614174000'
    )
    query.apply_filters(filters)
    sql = query.build_sql()

    assert 'FROM' in sql
    assert 'graduate_program_researcher gpr' in sql
    assert 'AND gpr.graduate_program_id = :graduate_program_id' in sql


def test_graduate_program_article_production_query():
    from simcc.queries.graduate_program_query import (
        GraduateProgramArticleProductionQuery,
    )

    query = GraduateProgramArticleProductionQuery(
        session=None,
        program_id='123e4567-e89b-12d3-a456-426614174000',
        year=2021,
    )
    sql = query.build_sql()

    assert "WHERE bpa.qualis = 'A1'" in sql
    assert 'INNER JOIN graduate_program_researcher gpr' in sql
    assert 'AND gpr.graduate_program_id = :program_id' in sql
    assert 'AND bp.year_ >= :year' in sql
    assert query.params['year'] == 2021
