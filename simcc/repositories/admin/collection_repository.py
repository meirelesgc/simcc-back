from simcc.core.connection import Connection


async def get_collection_entrys(conn: Connection, type, collection_id):
    SCRIPT_SQL = """
        SELECT ARRAY_AGG(entry_id) AS ids
        FROM feature.collection_entries
        WHERE collection_id = %(collection_id)s
            AND type = %(type)s;
        """
    return await conn.select(
        SCRIPT_SQL,
        params={'collection_id': collection_id, 'type': type},
        one=True,
    )


async def filter_star_entrys(conn: Connection, type, user_id):
    SCRIPT_SQL = """
        SELECT ARRAY_AGG(entry_id) AS ids
        FROM feature.stars
        WHERE user_id = %(user_id)s
            AND type = %(type)s;
        """
    return await conn.select(
        SCRIPT_SQL,
        params={'user_id': user_id, 'type': type},
        one=True,
    )
