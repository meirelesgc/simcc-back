from simcc.integrations import ai_integrations
from simcc.repositories.simcc import ProductionRepository, ResearcherRepository


def extract_fields(result, fields):
    blocks = []
    for item in result:
        lines = []
        for field in fields:
            if field in item:
                lines.append(f'{field}: {item[field]}')
        blocks.append('\n'.join(lines))
    return '\n\n---\n\n'.join(blocks)


async def ai_summary_search(conn, model, default_filters):
    match default_filters.type:
        case 'ARTICLE':
            result = await ProductionRepository.list_bibliographic_production(
                conn, default_filters, None
            )
            fields = ['title', 'year', 'qualis', 'magazine', 'abstract', 'jif']
            search = extract_fields(result, fields)
            print(search)
        case 'BOOK':
            result = await ProductionRepository.list_book(conn, default_filters)
            fields = ['year', 'title', 'publishing_company']
            search = extract_fields(result, fields)
        case 'BOOK_CHAPTER':
            result = await ProductionRepository.list_book_chapter(
                conn, default_filters
            )
            fields = ['year', 'title', 'publishing_company']
            search = extract_fields(result, fields)
        case 'ABSTRACT':
            result = await ResearcherRepository.search_in_abstracts(
                conn, default_filters
            )
            fields = [
                'name',
                'abstractuniversity',
                'area',
                'graduation',
                'h_index',
                'relevance_score',
                'works_count',
                'cited_by_count',
                'i10_index',
                'research_groups',
                'subsidy',
                'graduate_programs',
                'ufmg',
            ]
            search = extract_fields(result, fields)
        case 'NAME':
            result = await ResearcherRepository.search_in_name(
                conn, default_filters, None
            )
            fields = [
                'name',
                'university',
                'area',
                'abstract',
                'graduation',
                'h_index',
                'relevance_score',
                'works_count',
                'cited_by_count',
                'i10_index',
                'research_groups',
                'subsidy',
                'graduate_programs',
                'ufmg',
            ]
            search = extract_fields(result, fields)
        case _:
            pass
    return await ai_integrations.summary_search(model, search)
