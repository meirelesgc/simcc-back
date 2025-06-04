import os

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

from simcc.config import Settings
from simcc.repositories import conn

SCRIPT_SQL = """
        SELECT
            r.id, r.name, r.lattes_id, r.lattes_10_id, r.abstract, r.orcid,
            r.graduation, r.last_update AS lattes_update,
            REPLACE(rp.great_area, '_', ' ') AS area, rp.city,
            i.image AS image_university, i.name AS university,
            NULL AS among, rp.articles, rp.book_chapters, rp.book, rp.patent,
            rp.software, rp.brand, opr.h_index, opr.relevance_score,
            opr.works_count, opr.cited_by_count, opr.i10_index, opr.scopus,
            opr.openalex, r.classification, r.status, r.institution_id
        FROM researcher r
            LEFT JOIN institution i ON i.id = r.institution_id
            LEFT JOIN researcher_production rp ON rp.researcher_id = r.id
            LEFT JOIN openalex_researcher opr ON opr.researcher_id = r.id
        WHERE 1 = 1
        ORDER BY
            among DESC
    """


def get_researcher_data():
    try:
        data = conn.select(SCRIPT_SQL)
        return data
    except Exception as e:
        print(f'Erro ao executar a consulta SQL: {e}')
        return []


def generate_summary_with_langchain(
    data: list[dict], model: str = 'gpt-3.5-turbo', max_tokens: int = 500
) -> str:
    if not data:
        return 'Não há dados para gerar o resumo.'

    formatted_data = []
    for item in data:
        researcher_info = (
            f'Nome: {item.get("name", "N/A")}\n'
            f'Graduação: {item.get("graduation", "N/A")}\n'
            f'Área: {item.get("area", "N/A")}\n'
            f'Universidade: {item.get("university", "N/A")}\n'
            f'H-Index: {item.get("h_index", "N/A")}\n'
            f'Artigos: {item.get("articles", "N/A")}\n'
            f'Capítulos de Livro: {item.get("book_chapters", "N/A")}\n'
            f'Patentes: {item.get("patent", "N/A")}\n'
            f'Resumo Lattes: {item.get("abstract", "N/A")}\n'
            f'Citações: {item.get("cited_by_count", "N/A")}\n'
            f'Relevância (OpenAlex): {item.get("relevance_score", "N/A")}\n'
            f'Status: {item.get("status", "N/A")}\n'
        )
        formatted_data.append(researcher_info)

    full_text_data = '\n---\n'.join(formatted_data)

    llm = ChatOpenAI(model=model, temperature=0.7, max_tokens=max_tokens)

    prompt = ChatPromptTemplate.from_messages([
        (
            'system',
            'Você é um assistente útil que resume informações sobre pesquisadores.',
        ),
        (
            'user',
            'Com base nos seguintes dados de pesquisadores, forneça um resumo conciso e informativo, destacando as principais características, áreas de atuação, produção científica e métricas de impacto (como h-index e citações). O resumo deve ser objetivo e apresentar os dados de forma agregada quando possível, ou individualmente se houver um destaque importante.\n\nDados dos pesquisadores:\n{data}',
        ),
    ])

    output_parser = StrOutputParser()

    chain = prompt | llm | output_parser

    try:
        summary = chain.invoke({'data': full_text_data})
        return summary
    except Exception as e:
        return f'Erro ao gerar resumo com LangChain e OpenAI: {e}'


if __name__ == '__main__':
    researcher_data = get_researcher_data()
    os.environ['OPENAI_API_KEY'] = Settings().OPENAI_API_KEY
    if researcher_data:
        summary = generate_summary_with_langchain(
            researcher_data, model='gpt-4o', max_tokens=700
        )
        print('\n--- Resumo Gerado pelo LangChain e OpenAI ---\n')
        print(summary)
    else:
        print(
            'Não foi possível obter os dados dos pesquisadores para gerar o resumo.'
        )
