from simcc.queries.research_group_query import (
    ResearchGroupCountQuery,
    ResearchGroupQuery,
    ResearchLinesQuery,
)
from simcc.schemas import DefaultFilters


def test_research_group_query_base_sql():
    query = ResearchGroupQuery(session=None)
    sql = query.build_sql()
    assert 'FROM research_group rg' in sql
    assert 'INNER JOIN institution i ON i.acronym = rg.institution' in sql


def test_research_group_query_with_filter():
    query = ResearchGroupQuery(session=None)
    filters = DefaultFilters(group_id='123e4567-e89b-12d3-a456-426614174000')
    query.apply_filters(filters)
    sql = query.build_sql()
    assert 'AND rg.id = :group_id' in sql
    assert query.params['group_id'] == '123e4567-e89b-12d3-a456-426614174000'


def test_research_lines_query():
    query = ResearchLinesQuery(session=None)
    filters = DefaultFilters(group_id='123e4567-e89b-12d3-a456-426614174000')
    query.apply_filters(filters)
    sql = query.build_sql()
    assert 'FROM \n            research_lines rl' in sql
    assert 'AND rl.research_group_id = :group_id' in sql


def test_research_group_count_query():
    query = ResearchGroupCountQuery(session=None)
    sql = query.build_sql()
    assert 'GROUP BY area' in sql
    assert 'COUNT(*)' in sql
