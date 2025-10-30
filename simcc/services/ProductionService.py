from collections import Counter

import pandas as pd

from simcc.repositories.admin import collection_repository
from simcc.repositories.simcc import ProductionRepository
from simcc.schemas.production.Article import ArticleMetric
from simcc.schemas.production.Guidance import GuidanceMetrics
from simcc.schemas.production.Patent import PatentMetric
from simcc.schemas.Researcher import AcademicMetric


async def filter_collection_entrys(conn, type, collection_id):
    ids_dict = await collection_repository.get_collection_entrys(
        conn, type, collection_id
    )
    return ids_dict.get('ids') if ids_dict else None


async def filter_star_entrys(conn, type, user):
    ids_dict = await collection_repository.filter_star_entrys(
        conn, type, user['user_id']
    )
    return ids_dict.get('ids') if ids_dict else None


async def list_participation_event(conn, conn_admin, filters, nature):
    if filters.collection_id:
        filters.collection_id = await filter_collection_entrys(
            conn_admin, 'EVENT_PARTICIPATION', filters.collection_id
        )
    if filters.star:
        filters.star = await filter_star_entrys(
            conn_admin, 'EVENT_PARTICIPATION', filters.star
        )
    return await ProductionRepository.list_participation_event(
        conn, filters, nature
    )


async def list_professional_experience(conn, conn_admin, filters):
    if filters.collection_id:
        filters.collection_id = await filter_collection_entrys(
            conn_admin, 'PROFESSIONAL_EXPERIENCE', filters.collection_id
        )
    if filters.star:
        filters.star = await filter_star_entrys(
            conn_admin, 'PROFESSIONAL_EXPERIENCE', filters.star
        )
    return await ProductionRepository.list_professional_experience(conn, filters)


async def list_patent(conn, conn_admin, filters):
    if filters.collection_id:
        filters.collection_id = await filter_collection_entrys(
            conn_admin, 'PATENT', filters.collection_id
        )
    if filters.star:
        filters.star = await filter_star_entrys(
            conn_admin, 'PATENT', filters.star
        )
    return await ProductionRepository.list_patent(conn, filters)


async def list_brand(conn, conn_admin, filters):
    if filters.collection_id:
        filters.collection_id = await filter_collection_entrys(
            conn_admin, 'BRAND', filters.collection_id
        )
    if filters.star:
        filters.star = await filter_star_entrys(
            conn_admin, 'BRAND', filters.star
        )
    result = await ProductionRepository.list_brand(conn, filters)
    if not result:
        return []
    result = pd.DataFrame(result).to_dict(orient='records')
    return sorted(result, key=lambda x: x['year'], reverse=True)


async def list_book(conn, conn_admin, filters):
    if filters.collection_id:
        filters.collection_id = await filter_collection_entrys(
            conn_admin, 'BOOK', filters.collection_id
        )
    if filters.star:
        filters.star = await filter_star_entrys(conn_admin, 'BOOK', filters.star)
    return await ProductionRepository.list_book(conn, filters)


async def list_bibliographic_production(conn, conn_admin, filters, qualis):
    if filters.collection_id:
        filters.collection_id = await filter_collection_entrys(
            conn_admin, 'ARTICLE', filters.collection_id
        )
    if filters.star:
        filters.star = await filter_star_entrys(
            conn_admin, 'ARTICLE', filters.star
        )
    result = await ProductionRepository.list_bibliographic_production(
        conn, filters, qualis
    )
    if not result:
        return []
    df = pd.DataFrame(result).fillna('')
    return df.sort_values(by='year', ascending=False).to_dict(orient='records')


async def list_book_chapter(conn, conn_admin, filters):
    if filters.collection_id:
        filters.collection_id = await filter_collection_entrys(
            conn_admin, 'BOOK_CHAPTER', filters.collection_id
        )
    if filters.star:
        filters.star = await filter_star_entrys(
            conn_admin, 'BOOK_CHAPTER', filters.star
        )
    return await ProductionRepository.list_book_chapter(conn, filters)


async def list_software(conn, conn_admin, filters):
    if filters.collection_id:
        filters.collection_id = await filter_collection_entrys(
            conn_admin, 'SOFTWARE', filters.collection_id
        )
    if filters.star:
        filters.star = await filter_star_entrys(
            conn_admin, 'SOFTWARE', filters.star
        )
    return await ProductionRepository.list_software(conn, filters)


async def list_researcher_report(conn, conn_admin, filters):
    if filters.collection_id:
        filters.collection_id = await filter_collection_entrys(
            conn_admin, 'REPORT', filters.collection_id
        )
    if filters.star:
        filters.star = await filter_star_entrys(
            conn_admin, 'REPORT', filters.star
        )
    return await ProductionRepository.list_researcher_report(conn, filters)


async def list_guidance_production(conn, conn_admin, filters):
    if filters.collection_id:
        filters.collection_id = await filter_collection_entrys(
            conn_admin, 'GUIDANCE', filters.collection_id
        )
    if filters.star:
        filters.star = await filter_star_entrys(
            conn_admin, 'GUIDANCE', filters.star
        )
    return await ProductionRepository.list_guidance_production(conn, filters)


async def list_researcher_production_events(conn, conn_admin, filters):
    if filters.collection_id:
        filters.collection_id = await filter_collection_entrys(
            conn_admin, 'WORK_IN_EVENT', filters.collection_id
        )
    if filters.star:
        filters.star = await filter_star_entrys(
            conn_admin, 'WORK_IN_EVENT', filters.star
        )
    result = await ProductionRepository.list_researcher_production_events(
        conn, filters
    )
    return result


async def list_papers_magazine(conn, conn_admin, filters):
    if filters.collection_id:
        filters.collection_id = await filter_collection_entrys(
            conn_admin, 'PAPER', filters.collection_id
        )
    if filters.star:
        filters.star = await filter_star_entrys(
            conn_admin, 'PAPER', filters.star
        )
    return await ProductionRepository.list_papers_magazine(conn, filters)


async def list_research_projects(conn, conn_admin, filters):
    if filters.collection_id:
        filters.collection_id = await filter_collection_entrys(
            conn_admin, 'RESEARCH_PROJECT', filters.collection_id
        )
    if filters.star:
        filters.star = await filter_star_entrys(
            conn_admin, 'RESEARCH_PROJECT', filters.star
        )
    return await ProductionRepository.list_research_projects(conn, filters)


async def get_article_metrics(
    conn, qualis, default_filters
) -> list[ArticleMetric]:
    article_metrics = await ProductionRepository.list_article_metrics(
        conn, qualis, default_filters
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


async def get_brand_metrics(conn, nature, default_filters):
    return await ProductionRepository.get_brand_metrics(
        conn, nature, default_filters
    )


async def get_research_project_metrics(conn, default_filters):
    rp_metrics = await ProductionRepository.get_research_project_metrics(
        conn, default_filters
    )
    if not rp_metrics:
        return []
    rp_metrics = pd.DataFrame(rp_metrics)
    rp_metrics['nature'] = rp_metrics['nature'].apply(Counter)

    return rp_metrics.to_dict(orient='records')


async def get_magazine_metrics(conn, issn, initials):
    return await ProductionRepository.get_magazine_metrics(conn, issn, initials)


async def get_events_metrics(conn, default_filters):
    return await ProductionRepository.get_events_metrics(conn, default_filters)
