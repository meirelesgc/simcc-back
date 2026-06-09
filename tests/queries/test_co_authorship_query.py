import pytest
from uuid import uuid4
from simcc.queries.researcher_query import CoAuthorshipQuery

def test_co_authorship_query_build_sql():
    researcher_id = uuid4()
    query = CoAuthorshipQuery(session=None, researcher_id=researcher_id)
    sql = query.build_sql()
    
    assert ":researcher_id" in sql
    assert "WITH co_authorship AS" in sql
    assert "UNION" in sql
    assert "similarity(ca.name, r.name) < 0.2" in sql
    assert query.params['researcher_id'] == str(researcher_id)
