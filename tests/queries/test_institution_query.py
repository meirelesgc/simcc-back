from uuid import uuid4

from simcc.queries.institution_query import InstitutionQuery


def test_institution_query_list_sql():
    query = InstitutionQuery(session=None)
    sql = query.build_sql()
    assert 'WITH researcher_count AS (' in sql
    assert 'FROM institution i' in sql
    assert 'AND i.id = :institution_id' not in sql


def test_institution_query_get_one_sql():
    inst_id = uuid4()
    query = InstitutionQuery(session=None, institution_id=inst_id)
    sql = query.build_sql()
    assert 'AND i.id = :institution_id' in sql
    assert query.params['institution_id'] == inst_id
