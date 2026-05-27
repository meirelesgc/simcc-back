from simcc.queries.institution_query import InstitutionFrequencyQuery


def test_institution_frequency_query_abstract():
    query = InstitutionFrequencyQuery(
        session=None, terms='machine learning', institution='UFMG', type_='abstract'
    )
    sql = query.build_sql()
    assert 'COUNT(DISTINCT r.id)' in sql
    assert 'r.abstract' in sql
    assert 'i.name = ANY(:institution)' in sql
    assert query.params['institution'] == ['UFMG']
    assert 'term1' in query.params


def test_institution_frequency_query_article():
    query = InstitutionFrequencyQuery(
        session=None, terms='deep learning', institution='USP;UNICAMP', type_='article'
    )
    sql = query.build_sql()
    assert 'COUNT(r.id)' in sql
    assert 'bibliographic_production' in sql
    assert "b.type = 'ARTICLE'" in sql
    assert query.params['institution'] == ['USP', 'UNICAMP']


def test_institution_frequency_query_patent():
    query = InstitutionFrequencyQuery(
        session=None, terms='blockchain', institution=None, type_='patent'
    )
    sql = query.build_sql()
    assert 'COUNT(DISTINCT b.title)' in sql
    assert 'patent' in sql
    assert 'blockchain' in query.params['term1']


def test_institution_frequency_query_area():
    query = InstitutionFrequencyQuery(
        session=None, terms='Physics', institution='UFMG', type_='area'
    )
    sql = query.build_sql()
    assert 'COUNT(rp.researcher_id)' in sql
    assert 'researcher_production' in sql
    assert 'rp.area_specialty' in sql
