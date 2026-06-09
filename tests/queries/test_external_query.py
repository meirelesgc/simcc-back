from simcc.queries.external_query import (
    DepartmentSearchQuery,
    DocenteSearchQuery,
    ResearcherArticleProductionQuery,
)
from simcc.schemas import DefaultFilters


def test_docente_search_query_sql():
    query = DocenteSearchQuery(session=None)
    filters = DefaultFilters(dep_id='DEP123', type='ARTICLE', term='AI')
    query.apply_filters(filters)
    sql = query.build_sql()

    assert 'ufmg.departament_researcher' in sql
    assert 'dp.dep_id = ANY(:dep_id)' in sql
    assert 'bibliographic_production bp' in sql
    assert "bp.type = 'ARTICLE'" in sql
    assert 'bp.title' in sql  # from term filter


def test_department_search_query_sql():
    query = DepartmentSearchQuery(session=None, dep_id='D001')
    sql = query.build_sql()
    assert 'd.dep_id = :dep_id' in sql
    assert query.params['dep_id'] == 'D001'


def test_researcher_article_production_query_sql():
    query = ResearcherArticleProductionQuery(
        session=None, dep_id='DEP1', year=2021
    )
    sql = query.build_sql()
    assert 'dpr.dep_id = :dep_id' in sql
    assert 'bp.year_::int >= :year' in sql
    assert query.params['year'] == 2021


def test_word_frequency_query_sql():
    from simcc.queries.external_query import WordFrequencyQuery

    query = WordFrequencyQuery(session=None, term='test', stopwords=['a', 'b'])
    sql = query.build_sql()

    assert 'regexp_split_to_table' in sql
    assert 'TRIM(word) <> ALL(:stopwords)' in sql
    assert 'unaccent(word) ILIKE :term' in sql
    assert query.params['term'] == 'test%'
    assert query.params['stopwords'] == ['a', 'b']
