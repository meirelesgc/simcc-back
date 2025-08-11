from uuid import UUID

import pandas as pd

from simcc.core.connection import Connection
from simcc.repositories.simcc import (
    GraduateProgramRepository,
    researcher_repository,
)
from simcc.schemas.Researcher import ResearcherArticleProduction


async def list_graduate_program_researcher(conn, graduate_program_id):
    return await GraduateProgramRepository.list_graduate_program_researcher(  # fmt:skip
        conn, graduate_program_id
    )


async def get_research_lines(
    conn: Connection,
    program_id: UUID = None,
    university: str = None,
    term: str = None,
):
    return await GraduateProgramRepository.get_research_lines(
        conn, program_id, university, term
    )


async def list_graduate_programs(
    conn: Connection, program_id: UUID = None, university: str = None
):
    return await GraduateProgramRepository.list_graduate_programs(
        conn, program_id, university
    )


def list_article_production(
    program_id: UUID, dep_id: str, year: int
) -> ResearcherArticleProduction:
    article_production = researcher_repository.list_article_production(
        program_id, dep_id, year
    )
    if not article_production:
        return []

    article_production = pd.DataFrame(article_production)

    article_production_pivot = article_production.pivot_table(
        index=['name', 'year'], columns='qualis', aggfunc='size', fill_value=0
    )

    citations = (
        article_production.groupby(['name', 'year'])['citations']
        .sum()
        .reset_index()
    )

    article_production_pivot = article_production_pivot.merge(
        citations, on=['name', 'year'], how='left'
    )

    columns = ResearcherArticleProduction.model_fields.keys()
    article_production = article_production_pivot.reindex(
        columns, axis='columns', fill_value=0
    )

    return article_production.to_dict(orient='records')
