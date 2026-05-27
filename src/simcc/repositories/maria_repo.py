import uuid
from typing import List

from sqlalchemy import text


async def search_by_embeddings(
    session, embedding: List[float], search_type: str
) -> List[uuid.UUID]:
    # search_type can be 'abstract', 'article', 'article_abstract', 'book', 'event', 'patent'

    embedding_str = str(embedding)

    script_sql = f"""
        SELECT reference_id
        FROM embeddings.{search_type}
        ORDER BY embeddings <=> :embedding
        LIMIT 10;
    """

    result = await session.execute(
        text(script_sql), {'embedding': embedding_str}
    )
    reference_ids = [row[0] for row in result.all()]

    if not reference_ids:
        return []

    # 2. Map reference_ids to researcher_ids
    if search_type == 'abstract':
        return reference_ids

    if search_type in ['article', 'book', 'event', 'article_abstract']:
        map_sql = """
            SELECT DISTINCT researcher_id
            FROM bibliographic_production
            WHERE id = ANY(:ids)
        """
        map_result = await session.execute(
            text(map_sql), {'ids': reference_ids}
        )
        return [row[0] for row in map_result.all()]

    if search_type == 'patent':
        map_sql = """
            SELECT DISTINCT researcher_id
            FROM patent
            WHERE id = ANY(:ids)
        """
        map_result = await session.execute(
            text(map_sql), {'ids': reference_ids}
        )
        return [row[0] for row in map_result.all()]

    return []
