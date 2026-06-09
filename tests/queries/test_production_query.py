from simcc.queries.production_query import (
    PatentQuery,
    ProfessionalExperienceQuery,
    ResearchProjectQuery,
    ScholarshipQuery,
    SoftwareQuery,
)
from simcc.schemas import DefaultFilters


def test_patent_query_base_sql():
    query = PatentQuery(session=None)
    sql = query.build_sql()
    assert 'FROM public.patent p' in sql
    assert 'ORDER BY p.development_year DESC' in sql


def test_patent_query_with_common_filters():
    query = PatentQuery(session=None)
    filters = DefaultFilters(
        institution='UFMG', city='Belo Horizonte', year=2020
    )
    query.apply_filters(filters)
    sql = query.build_sql()

    assert 'AND i.name = ANY(:institution)' in sql
    assert 'AND rp.city = ANY(:city)' in sql
    assert 'AND p.development_year::INT >= :year' in sql
    assert 'INNER JOIN institution i' in sql
    assert query.params['institution'] == ['UFMG']
    assert query.params['city'] == ['Belo Horizonte']
    assert query.params['year'] == 2020


def test_software_query_term_search():
    query = SoftwareQuery(session=None)
    filters = DefaultFilters(term='simcc project')
    query.apply_filters(filters)
    sql = query.build_sql()

    assert 'ts_rank' in sql
    assert 's.title' in sql
    assert 'term1' in query.params


def test_research_project_query_type_filter():
    query = ResearchProjectQuery(session=None)
    filters = DefaultFilters(type='Extensão;Pesquisa')
    query.apply_filters(filters)
    sql = query.build_sql()

    assert 'AND rp.nature = ANY(:type)' in sql
    assert query.params['type'] == ['Extensão', 'Pesquisa']


def test_professional_experience_query_ordering():
    query = ProfessionalExperienceQuery(session=None)
    filters = DefaultFilters(distinct=1)
    query.apply_filters(filters)
    sql = query.build_sql()

    assert 'DISTINCT ON (rpe.researcher_id, rpe.enterprise)' in sql
    assert (
        'ORDER BY rpe.researcher_id, rpe.enterprise, rpe.start_year DESC'
        in sql
    )


def test_patent_query_star_filter():
    query = PatentQuery(session=None)
    filters = DefaultFilters(star=True)
    # Simulate CurrentUser UUID passing through
    filters.star = ['89f36553-652e-461d-9e6b-07e376e3d74c']
    query.apply_filters(filters)
    sql = query.build_sql()

    assert 'AND p.id = ANY(:star_ids)' in sql
    assert query.params['star_ids'] == ['89f36553-652e-461d-9e6b-07e376e3d74c']


def test_scholarship_query_distinct_ordering():
    query = ScholarshipQuery(session=None)
    filters = DefaultFilters(distinct=1)
    query.apply_filters(filters)
    sql = query.build_sql()

    assert 'DISTINCT ON (s.researcher_id, s.call_title)' in sql
    assert 'ORDER BY s.researcher_id, s.call_title, r.name ASC' in sql
