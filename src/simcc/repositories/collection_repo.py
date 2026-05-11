from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def get_collection_entries(
    session: AsyncSession,
    type,
    collection_id,
    one: bool = False,
):
    script_sql = text("""
        SELECT ARRAY_AGG(entry_id) AS ids
        FROM feature.collection_entries
        WHERE collection_id = :collection_id
          AND type = :type;
    """)

    result = await session.execute(
        script_sql,
        {
            'collection_id': collection_id,
            'type': type,
        },
    )

    mappings = result.mappings()

    if one:
        return mappings.one_or_none()

    return mappings.all()


async def filter_star_entries(
    session: AsyncSession,
    type,
    user_id,
    one: bool = False,
):
    script_sql = text("""
        SELECT ARRAY_AGG(entry_id) AS ids
        FROM feature.stars
        WHERE user_id = :user_id
          AND type = :type;
    """)

    result = await session.execute(
        script_sql,
        {
            'user_id': user_id,
            'type': type,
        },
    )

    mappings = result.mappings()

    if one:
        return mappings.one_or_none()

    return mappings.all()
