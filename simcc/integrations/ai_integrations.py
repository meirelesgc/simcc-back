from langchain_core.language_models.chat_models import BaseChatModel


async def summary_search(model: BaseChatModel, search: str):
    search = f"""
        Você receberá uma série de conteúdos acadêmicos (ex.: artigos, livros, patentes ou perfis de pesquisadores).
        Sua tarefa é extrair e apresentar os principais tópicos, tendências ou áreas de atuação descritas nos textos.

        Instruções:
        1. O resumo deve ter no máximo 5 parágrafos curtos.
        2. A linguagem deve ser formal, objetiva e técnica.
        3. Não use adjetivos, julgamentos de valor ou qualquer tipo de elogio.
        4. Não use palavras como "destaca-se", "exemplifica", "esforço", "forte ênfase", "preocupação", "relevante", etc.
        5. Apenas descreva o que é mencionado, sem interpretar intenções ou qualificar os autores.

        Lista:
        {search}
        """
    summary = await model.ainvoke(search)
    return summary.content
