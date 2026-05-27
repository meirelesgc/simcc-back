from simcc.queries.metrics_query import GreatAreaMetricsQuery
from simcc.schemas import DefaultFilters


def test_great_area_metrics_query_duplicate_alias_fix():
    query = GreatAreaMetricsQuery(session=None)
    # Applying a filter that triggers 'researcher_production' join
    filters = DefaultFilters(city='Belo Horizonte')
    query.apply_filters(filters)
    
    sql = query.build_sql()
    
    # Check that 'INNER JOIN researcher_production rp' appears only once in the final SQL
    # In GreatAreaMetricsQuery, it's in the FROM clause, so it should NOT be in the joins list.
    assert sql.count('researcher_production rp') == 1
    assert 'FROM researcher_production rp' in sql
    assert 'INNER JOIN researcher_production rp' not in sql
