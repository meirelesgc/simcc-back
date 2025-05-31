from collections import Counter

import pandas as pd

from simcc.core.connection import Connection  # Adicionado
from simcc.repositories.simcc import ProductionRepository
from simcc.schemas import DefaultFilters  # Adicionado
from simcc.schemas.Production.Article import ArticleMetric, ArticleProduction
from simcc.schemas.Production.Brand import BrandProduction
from simcc.schemas.Production.Guidance import GuidanceMetrics
from simcc.schemas.Production.Patent import PatentMetric, PatentProduction
from simcc.schemas.Researcher import AcademicMetric


async def get_pevent_researcher(
    conn: Connection,
    default_filters: DefaultFilters,
    nature: str | None,
    page: int | None,
    lenght: int | None,
):
    return await ProductionRepository.get_pevent_researcher(
        conn, default_filters, nature, page, lenght
    )


async def professional_experience(
    conn: Connection,
    default_filters: DefaultFilters,
    page: int | None,
    lenght: int | None,
):
    return await ProductionRepository.professional_experience(
        conn, default_filters, page, lenght
    )


async def list_patent(
    conn: Connection,
    default_filters: DefaultFilters,
    page: int | None,
    lenght: int | None,
) -> list[PatentProduction]:
    patents = await ProductionRepository.list_patent(
        conn, default_filters, page, lenght
    )
    if not patents:
        return []
    return patents


async def list_brand(
    conn: Connection,
    default_filters: DefaultFilters,
    page: int | None,
    lenght: int | None,
) -> list[BrandProduction]:
    brands = await ProductionRepository.list_brand(
        conn, default_filters, page, lenght
    )
    if not brands:
        return []
    return brands


async def list_book(
    conn: Connection,
    default_filters: DefaultFilters,
    page: int | None,
    lenght: int | None,
):
    books = await ProductionRepository.list_book(
        conn, default_filters, page, lenght
    )
    if not books:
        return []
    return books


async def list_bibliographic_production(
    conn: Connection,
    default_filters: DefaultFilters,
    qualis: str | None,
    page: int | None,
    lenght: int | None,
) -> list[ArticleProduction]:
    production = await ProductionRepository.list_bibliographic_production(
        conn, default_filters, qualis, page, lenght
    )

    if not production:
        return []

    production = pd.DataFrame(production)
    production = production.fillna('')

    return production.to_dict(orient='records')


async def list_article_production(
    conn: Connection,
    default_filters: DefaultFilters,
    qualis: str | None,
    page: int | None,
    lenght: int | None,
):
    production = await ProductionRepository.list_article_production(
        conn, default_filters, qualis, page, lenght
    )
    if not production:
        return []

    production = pd.DataFrame(production)
    production = production.fillna('')

    return production.to_dict(orient='records')


async def list_book_chapter(
    conn: Connection,
    default_filters: DefaultFilters,
    page: int | None,
    lenght: int | None,
):
    return await ProductionRepository.list_book_chapter(
        conn, default_filters, page, lenght
    )


async def list_software(
    conn: Connection,
    default_filters: DefaultFilters,
    page: int | None,
    lenght: int | None,
):
    return await ProductionRepository.list_software(
        conn, default_filters, page, lenght
    )


async def list_researcher_report(
    conn: Connection,
    default_filters: DefaultFilters,
    page: int | None,
    lenght: int | None,
):
    return await ProductionRepository.list_researcher_report(
        conn, default_filters, page, lenght
    )


async def list_guidance_production(
    conn: Connection,
    default_filters: DefaultFilters,
    page: int | None,
    lenght: int | None,
):
    return await ProductionRepository.list_guidance_production(
        conn, default_filters, page, lenght
    )


async def list_researcher_production_events(
    conn: Connection,
    default_filters: DefaultFilters,
    page: int | None,
    lenght: int | None,
):
    return await ProductionRepository.list_researcher_production_events(
        conn, default_filters, page, lenght
    )


async def list_research_projects(
    conn: Connection,
    default_filters: DefaultFilters,
    page: int | None,
    lenght: int | None,
):
    return await ProductionRepository.list_research_projects(
        conn, default_filters, page, lenght
    )


async def list_papers_magazine(
    conn: Connection,
    default_filters: DefaultFilters,
    page: int | None,
    lenght: int | None,
):
    return await ProductionRepository.list_papers_magazine(
        conn, default_filters, page, lenght
    )


# ---


async def get_article_metrics(conn, default_filters) -> list[ArticleMetric]:
    article_metrics = await ProductionRepository.list_article_metrics(
        conn, default_filters
    )
    if not article_metrics:
        return []

    def count_qualis(qualis):
        return dict(Counter(qualis))

    def count_jcr(jcr):
        bins = [0.0, 0.65, 2.0, 4.0, float('inf')]
        labels = ['very_low', 'low', 'medium', 'high']
        jcr_metrics = pd.cut(
            pd.to_numeric(jcr, errors='coerce'),
            bins=bins,
            labels=labels,
        )
        jcr_metrics = jcr_metrics.value_counts()
        jcr_metrics = jcr_metrics.to_dict()

        jcr_metrics['not_applicable'] = jcr.count('N/A')
        jcr_metrics['without_jcr'] = jcr.count(None)
        return jcr_metrics

    article_metrics = pd.DataFrame(article_metrics)
    article_metrics['qualis'] = article_metrics['qualis'].apply(count_qualis)
    article_metrics['jcr'] = article_metrics['jcr'].apply(count_jcr)
    article_metrics['citations'] = article_metrics['citations'].fillna(0)
    return article_metrics.to_dict(orient='records')


async def get_patent_metrics(conn, default_filters) -> list[PatentMetric]:
    patent_metrics = await ProductionRepository.list_patent_metrics(
        conn, default_filters
    )
    if not patent_metrics:
        return []
    return patent_metrics


async def get_guidance_metrics(conn, default_filters) -> list[GuidanceMetrics]:
    guidance_metrics = await ProductionRepository.list_guidance_metrics(
        conn, default_filters
    )
    if not guidance_metrics:
        return []
    guidance_metrics = pd.DataFrame(guidance_metrics)

    guidance_metrics = guidance_metrics.pivot_table(
        index='year',
        columns='nature',
        values='count_nature',
        aggfunc='sum',
        fill_value=0,
    ).reset_index()

    rename_dict = {
        'iniciacao cientifica concluida': 'ic_completed',
        'iniciacao cientifica em andamento': 'ic_in_progress',
        'dissertacao de mestrado concluida': 'm_completed',
        'dissertacao de mestrado em andamento': 'm_in_progress',
        'tese de doutorado concluida': 'd_completed',
        'tese de doutorado em andamento': 'd_in_progress',
        'trabalho de conclusao de curso graduacao concluida': 'g_completed',
        'trabalho de conclusao de curso de graduacao em andamento': 'g_in_progress',
        'monografia de conclusao de curso aperfeicoamento e especializacao concluida': 'e_completed',
        'monografia de conclusao de curso aperfeicoamento e especializacao em andamento': 'e_in_progress',
        'orientacao-de-outra-natureza concluida': 'o_completed',
        'supervisao de pos-doutorado concluida': 'sd_completed',
        'supervisao de pos-doutorado em andamento': 'sd_in_progress',
        'year': 'year',
    }

    guidance_metrics.rename(columns=rename_dict, inplace=True)
    columns = [column for column in rename_dict.values()]
    guidance_metrics = guidance_metrics.reindex(
        columns, axis='columns', fill_value=0
    )
    return guidance_metrics.to_dict(orient='records')


async def get_academic_degree_metrics(
    conn, default_filters
) -> list[AcademicMetric]:
    degree_metrics = await ProductionRepository.list_academic_degree_metrics(
        conn, default_filters
    )
    if not degree_metrics:
        return []
    degree_metrics = pd.DataFrame(degree_metrics)
    degree_metrics['degree'] = degree_metrics['degree'].str.lower()
    degree_metrics = degree_metrics.pivot_table(
        index='year',
        columns='degree',
        values='among',
        aggfunc='sum',
        fill_value=0,
    ).reset_index()

    columns = AcademicMetric.model_fields.keys()
    degree_metrics = degree_metrics.reindex(
        columns, axis='columns', fill_value=0
    )

    return degree_metrics.to_dict(orient='records')


async def get_software_metrics(conn, default_filters):
    metrics = await ProductionRepository.list_software_metrics(
        conn, default_filters
    )
    return metrics


async def get_book_metrics(conn, default_filters):
    return await ProductionRepository.get_book_metrics(conn, default_filters)


async def get_book_chapter_metrics(conn, default_filters):
    return await ProductionRepository.get_book_chapter_metrics(
        conn, default_filters
    )


async def get_researcher_metrics(conn, default_filters):
    return await ProductionRepository.get_researcher_metrics(
        conn, default_filters
    )


async def get_speaker_metrics(conn, default_filters):
    return await ProductionRepository.get_speaker_metrics(conn, default_filters)


async def get_research_report_metrics(conn, default_filters):
    return await ProductionRepository.get_research_report_metrics(
        conn, default_filters
    )


async def get_papers_magazine_metrics(conn, default_filters):
    return await ProductionRepository.get_papers_magazine_metrics(
        conn, default_filters
    )


async def get_speaker_metrics(conn, default_filters):
    return await ProductionRepository.get_speaker_metrics(conn, default_filters)


async def get_brand_metrics(conn, nature, default_filters):
    return await ProductionRepository.get_brand_metrics(
        conn, nature, default_filters
    )
