from simcc.queries.researcher_query import (
    ResearcherSearchQuery,
    ResearcherTermQuery,
)
from simcc.schemas import DefaultFilters


def test_researcher_query_base_sql():
    query = ResearcherSearchQuery(session=None)
    sql = query.build_sql()
    assert 'FROM researcher r' in sql
    assert 'WHERE 1 = 1' in sql
    assert 'AND r.status IS True' in sql


def test_researcher_query_with_filters():
    query = ResearcherSearchQuery(session=None)
    filters = DefaultFilters(city='Belo Horizonte', institution='UFMG')
    query.apply_filters(filters)
    sql = query.build_sql()

    assert 'AND rp.city = ANY(:city)' in sql
    assert 'AND i.name = ANY(:institution)' in sql
    assert 'INNER JOIN institution i' in sql
    assert 'LEFT JOIN researcher_production rp' in sql
    assert query.params['city'] == ['Belo Horizonte']
    assert query.params['institution'] == ['UFMG']


def test_researcher_query_distinct_ordering():
    query = ResearcherSearchQuery(session=None)
    filters = DefaultFilters(distinct=1)
    query.apply_filters(filters)
    sql = query.build_sql()

    assert 'DISTINCT ON (r.id)' in sql
    assert 'ORDER BY r.id, r.name' in sql


def test_researcher_query_term_abstract():
    query = ResearcherSearchQuery(session=None, search_type='ABSTRACT')
    filters = DefaultFilters(term='machine learning')
    query.apply_filters(filters)
    sql = query.build_sql()

    assert 'ts_rank' in sql
    assert 'r.abstract' in sql
    assert 'term1' in query.params


def test_researcher_query_with_name_parentheses():
    query = ResearcherSearchQuery(
        session=None, name='(henrique resende martins)'
    )
    sql = query.build_sql()

    assert 'ILIKE :name_filter' in sql
    assert query.params['name_filter'] == '%henrique resende martins%'


def test_researcher_term_query_base_sql():
    query = ResearcherTermQuery(session=None)
    sql = query.build_sql()
    assert 'unnest(' in sql
    assert 'bibliographic_production b' in sql
    assert 'lexeme <> ALL(:stopwords)' in sql
