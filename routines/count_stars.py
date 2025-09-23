from simcc.repositories import conn, conn_admin

types = {
    'ARTICLE': 'bibliographic_production_article',
    'BOOK': 'bibliographic_production_book',
    'BOOK_CHAPTER': 'bibliographic_production_book_chapter',
    'WORK_IN_EVENT': 'bibliographic_production_work_in_event',
    'BRAND': 'brand',
    'SOFTWARE': 'software',
    'PATENT': 'patent',
    'REPORT': 'research_report',
    'GUIDANCE': 'guidance',
    'PARTICIPATION_EVENTS': 'participation_events',
    'RESEARCH_PROJECT': 'research_project',
    'RESEARCHER': 'researcher',
}

id_columns = {
    'bibliographic_production_article': 'bibliographic_production_id',
    'bibliographic_production_book': 'bibliographic_production_id',
    'bibliographic_production_book_chapter': 'bibliographic_production_id',
    'bibliographic_production_work_in_event': 'bibliographic_production_id',
    'brand': 'id',
    'software': 'id',
    'patent': 'id',
    'research_report': 'id',
    'guidance': 'id',
    'participation_events': 'id',
    'research_project': 'id',
    'researcher': 'id',
}

SCRIPT_SQL = """
    SELECT entry_id, type, COUNT(*) AS COUNT
    FROM feature.stars
    GROUP BY entry_id, type;
    """
stars = conn_admin.select(SCRIPT_SQL)

for row in stars:
    entry_id = row['entry_id']
    entry_type = row['type']
    count = row['count']

    if entry_type in types:
        table = types[entry_type]
        id_column = id_columns.get(table, 'id')
        SCRIPT_SQL = f"""
            UPDATE {table}
            SET stars = {count}
            WHERE {id_column} = '{entry_id}';
        """
        conn.exec(SCRIPT_SQL)
