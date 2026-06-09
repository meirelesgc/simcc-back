from simcc.queries.term_query import OriginalWordsQuery


def test_original_words_query_name():
    query = OriginalWordsQuery(session=None, initials='abc', type_='name')
    sql = query.build_sql()
    assert 'FROM' in sql
    assert 'researcher' in sql
    assert 'unaccent(LOWER(name)) LIKE unaccent(:initials) ||' in sql
    assert query.params['initials'] == 'abc'


def test_original_words_query_area():
    query = OriginalWordsQuery(session=None, initials='bio', type_='area')
    sql = query.build_sql()
    assert 'UNION' in sql
    assert 'area_specialty' in sql
    assert 'area_expertise' in sql
    assert 'sub_area_expertise' in sql
    assert query.params['initials'] == 'bio'


def test_original_words_query_dictionary():
    query = OriginalWordsQuery(session=None, initials='deep', type_='article')
    sql = query.build_sql()
    assert 'FROM research_dictionary r' in sql
    assert 'type_ = :type' in sql
    assert query.params['initials'] == 'deep'
    assert query.params['type'] == 'ARTICLE'


def test_original_words_query_book():
    query = OriginalWordsQuery(session=None, initials='art', type_='book')
    sql = query.build_sql()
    assert "(type_ = 'BOOK' OR type_ = 'BOOK_CHAPTER')" in sql
    assert query.params['initials'] == 'art'
    assert query.params['type'] == 'BOOK'
