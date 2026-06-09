from simcc.queries.metrics_query import ScholarshipMetricsQuery
from simcc.schemas import DefaultFilters

def test_scholarship_metrics_query_basic():
    query = ScholarshipMetricsQuery(session=None)
    sql = query.build_sql()
    
    assert 'f.modality_code' in sql
    assert 'f.category_level_code' in sql
    assert 'COUNT(DISTINCT f.researcher_id) AS count' in sql
    assert 'GROUP BY f.modality_code, f.category_level_code' in sql
    assert 'FROM foment f' in sql

def test_scholarship_metrics_query_with_institution_filter():
    query = ScholarshipMetricsQuery(session=None)
    filters = DefaultFilters(institution='UFMG')
    query.apply_filters(filters)
    sql = query.build_sql()
    
    assert 'INNER JOIN institution i ON r.institution_id = i.id' in sql
    assert 'AND i.name = ANY(:institution)' in sql
    assert query.params['institution'] == ['UFMG']

def test_scholarship_metrics_query_with_researcher_id():
    query = ScholarshipMetricsQuery(session=None)
    filters = DefaultFilters(researcher_id='7f938d6c-6f8d-4f1d-8f8d-6f8d4f1d8f8d')
    query.apply_filters(filters)
    sql = query.build_sql()
    
    assert 'AND r.id = :researcher_id' in sql
    assert query.params['researcher_id'] == '7f938d6c-6f8d-4f1d-8f8d-6f8d4f1d8f8d'
