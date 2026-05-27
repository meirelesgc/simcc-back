from uuid import uuid4

from simcc.queries.metrics_query import GraduateProgramProductionQuery
from simcc.schemas import DefaultFilters


def test_graduate_program_production_query_program_sql():
    query = GraduateProgramProductionQuery(session=None)
    gp_id = uuid4()
    filters = DefaultFilters(graduate_program_id=gp_id, year=2020)
    query.apply_filters(filters)
    sql = query.build_sql()

    assert 'JOIN graduate_program_researcher gpr' in sql
    assert 'AND gpr.graduate_program_id = :graduate_program_id' in sql
    assert 'AND b.year_ >= :year' in sql
    assert query.params['graduate_program_id'] == str(gp_id)
    assert query.params['year'] == 2020


def test_graduate_program_production_query_dep_sql():
    query = GraduateProgramProductionQuery(session=None)
    filters = DefaultFilters(dep_id='DEP_TEST', year=2018)
    query.apply_filters(filters)
    sql = query.build_sql()

    assert 'COUNT(DISTINCT b.title) AS qtd' in sql
    assert 'ufmg.departament_researcher' in sql
    assert 'dep_id = :dep_id' in sql
    assert query.params['dep_id'] == 'DEP_TEST'
    assert query.params['year'] == 2018
