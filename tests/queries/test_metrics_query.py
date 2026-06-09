from simcc.queries.metrics_query import (
    EducationMetricsQuery,
    PatentMetricsQuery,
    YearlyProductionMetricsQuery,
)
from simcc.schemas import DefaultFilters


def test_yearly_production_query_article():
    query = YearlyProductionMetricsQuery(
        session=None, production_type='ARTICLE'
    )
    sql = query.build_sql()
    assert 'bp.year' in sql
    assert 'SUM(COALESCE(opa.citations_count, 0))' in sql
    assert 'bibliographic_production_article bpa' in sql


def test_yearly_production_query_book():
    query = YearlyProductionMetricsQuery(session=None, production_type='BOOK')
    sql = query.build_sql()
    assert 'bp.year' in sql
    assert 'COUNT( bp.title) AS among' in sql
    assert "bp.type = 'BOOK'" in sql


def test_patent_metrics_query():
    query = PatentMetricsQuery(session=None)
    sql = query.build_sql()
    assert 'FILTER (WHERE p.grant_date IS NULL) AS NOT_GRANTED' in sql
    assert 'FILTER (WHERE p.grant_date IS NOT NULL) AS GRANTED' in sql


def test_education_metrics_query():
    query = EducationMetricsQuery(session=None)
    sql = query.build_sql()
    assert 'WITH EducacaoFiltrada AS' in sql
    assert 'UNION ALL' in sql
    assert (
        "REPLACE(degree || '_' || event_type, '-', '_')"
        or "REPLACE(degree || '-' || event_type, '-', '_')" in sql
    )


def test_metrics_query_with_year_filter():
    query = YearlyProductionMetricsQuery(session=None, production_type='BOOK')
    filters = DefaultFilters(year=2020)
    query.apply_filters(filters)
    sql = query.build_sql()
    assert 'AND bp.year::INT >= :year_metrics' in sql
    assert query.params['year_metrics'] == 2020
