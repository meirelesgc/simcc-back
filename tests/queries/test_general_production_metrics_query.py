from simcc.queries.metrics_query import GeneralProductionMetricsQuery


def test_general_production_metrics_query_sql():
    query = GeneralProductionMetricsQuery(session=None)
    query._apply_year_filter(2020)
    query.build_sql()

    # Check if all subqueries are generated
    assert 'guidance' in query._queries
    assert 'books' in query._queries
    assert 'patents' in query._queries
    assert 'articles' in query._queries

    # Check for basic SQL structure in one of them
    guidance_sql = query._queries['guidance']
    assert 'FROM guidance g' in guidance_sql
    assert 'g.year::int >= :year' in guidance_sql


def test_general_production_metrics_query_filters():
    query = GeneralProductionMetricsQuery(session=None)
    query._apply_year_filter(2015)
    query._apply_dep_id_filter('DEP_123')
    query.build_sql()

    for key, sql in query._queries.items():
        assert 'dep_id = :dep_id' in sql
        assert 'year' in query.params
