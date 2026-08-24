from simcc.ai.prompts.maria_prompts import (
    MARIA_PROMPT_TEMPLATE,
    SUMMARY_SEARCH_PROMPT,
)
from simcc.ai.providers.base import EmbeddingsProvider, LLMProvider
from simcc.ai.schemas.maria import MariaResponse
from simcc.repositories import maria_repo, researcher_repo
from simcc.schemas import DefaultFilters
from simcc.services import production_service, researcher_service


class MariaService:
    def __init__(
        self,
        llm: LLMProvider,
        embeddings: EmbeddingsProvider,
    ):
        self.llm = llm
        self.embeddings = embeddings

    async def search_and_summarize(
        self, session, query: str, search_type: str
    ) -> MariaResponse:
        # 1. Get embedding for the query
        vector = await self.embeddings.get_embeddings(query)

        # 2. Search IDs by similarity
        researcher_ids = await maria_repo.search_by_embeddings(
            session, vector, search_type
        )

        if not researcher_ids:
            return MariaResponse(query='', researchers=[])

        # 3. Fetch full researcher data
        filters = DefaultFilters(researcher_ids=researcher_ids)
        researchers_data = await researcher_repo.search_researchers(
            session, filters
        )

        # 4. Generate AI summary comment
        # Limita a 5 pesquisadores e reduz campos para o prompt
        data_to_summarize = [
            self._get_compact_researcher_data(dict(r))
            for r in researchers_data[:5]
        ]

        prompt = MARIA_PROMPT_TEMPLATE.format(data_dict=str(data_to_summarize))

        comment = await self.llm.generate(prompt)

        return MariaResponse(query=comment, researchers=researchers_data)

    async def generate_search_summary(
        self, session, filters: DefaultFilters
    ) -> str:
        """
        Gera um resumo baseado nos resultados de uma busca filtrada.
        """
        search_type = filters.type.upper() if filters.type else 'ARTICLE'

        # Limite reduzido para pesquisadores para evitar prompts gigantes
        limit = 5 if search_type in {'NAME', 'AREA'} else 10
        filters.lenght = limit
        filters.page = 1

        data = []

        if search_type == 'ARTICLE':
            data = await production_service.list_bibliographic_production(
                session, filters
            )
        elif search_type == 'BOOK':
            data = await production_service.list_book(session, filters)
        elif search_type == 'BOOK_CHAPTER':
            data = await production_service.list_book_chapter(session, filters)
        elif search_type == 'ABSTRACT':
            # Utiliza o service de produção bibliográfica filtrando por tipo
            filters.type = 'ABSTRACT'
            data = await production_service.list_bibliographic_production(
                session, filters
            )
        elif search_type in {'NAME', 'AREA'}:
            data = await researcher_service.search_researchers(
                session, filters
            )
        elif search_type == 'WORK_IN_EVENT':
            data = await production_service.list_researcher_production_events(
                session, filters
            )
        elif search_type == 'PATENT':
            data = await production_service.list_patent(session, filters)
        elif search_type == 'EVENT':
            data = await production_service.list_participation_event(
                session, filters
            )
        else:
            # Fallback para produções bibliográficas gerais
            data = await production_service.list_bibliographic_production(
                session, filters
            )

        if not data:
            return 'Nenhum resultado relevante encontrado para gerar o resumo.'

        # Formata dados para o prompt
        if search_type in {'NAME', 'AREA'}:
            data_to_summarize = [
                self._get_compact_researcher_data(dict(r))
                for r in data[:limit]
            ]
        else:
            data_to_summarize = [dict(r) for r in data[:limit]]

        prompt = SUMMARY_SEARCH_PROMPT.format(data_dict=str(data_to_summarize))

        return await self.llm.generate(prompt)

    def _get_compact_researcher_data(self, researcher: dict) -> dict:
        """
        Retorna um dicionário reduzido com os campos essenciais para o prompt da IA.
        """
        return {
            'name': researcher.get('name'),
            'university': researcher.get('university'),
            'area': researcher.get('area'),
            'abstract': (researcher.get('abstract') or '')[:500] + '...'
            if researcher.get('abstract')
            else None,
            'articles': researcher.get('articles'),
            'h_index': researcher.get('h_index'),
        }

    async def chat_ask(
        self, session, query: str, planner, search_service
    ) -> dict:
        from simcc.ai.schemas.maria import ChatResponse

        # 1. Planejar a query
        plan = await planner.plan(query)

        # 2. Executar a busca baseada na intenção
        researchers = []
        productions = []

        if plan.intent in [
            'researcher_search',
            'researcher_profile',
            'researcher_comparison',
            'aggregation',
        ]:
            # Extrai filtros limpos
            filters_dict = plan.filters.model_dump(exclude_none=True)

            researchers = await search_service.search_researchers_hybrid(
                session=session,
                query=plan.semantic_query,
                limit=10,
                filters=filters_dict,
            )

        # 3. Construir um prompt contextualizado de síntese
        researchers_context = ''
        for i, r in enumerate(researchers, 1):
            inst = (
                r.get('institution_acronym')
                or r.get('institution')
                or 'Instituição não informada'
            )
            researchers_context += (
                f'\n--- [Pesquisador {i}] ---\n'
                f'Nome: {r["name"]}\n'
                f'Instituição: {inst}\n'
                f'Conteúdo Semântico:\n{r.get("semantic_content", r.get("abstract", ""))}\n'
            )

        synthesis_prompt = f"""
Você é a MarIA, assistente de inteligência artificial especializada na base de dados de pesquisadores e produções científicas da Bahia.

Pergunta do Usuário: "{query}"
Plano de Execução do Planner:
- Intenção: {plan.intent}
- Filtros Aplicados: {plan.filters.model_dump(exclude_none=True)}
- Busca Semântica: "{plan.semantic_query}"

Contexto Recuperado da Base de Dados ({len(researchers)} registros):
{researchers_context if researchers else 'Nenhum pesquisador recuperado para os critérios informados.'}

Instruções para a Resposta:
1. Responda em Português de forma clara, natural, profissional e informativa.
2. Direcione o estilo da resposta de acordo com a intenção:
   - Se for 'researcher_profile': apresente o perfil completo da pessoa (instituição, formação, titulação, áreas de atuação e resumo de trajetória).
   - Se for 'researcher_comparison': organize e agrupe a resposta por instituição (ex: 'Na UFBA...', 'Na UNEB...'), comparando as linhas de atuação e perfis de cada pesquisador recuperado.
   - Se for 'researcher_search': apresente os pesquisadores encontrados que melhor atendem ao pedido, explicando brevemente por que cada um é relevante para o tema.
   - Se a base não tiver resultados correspondentes ou tiver dados insuficientes, reconheça com transparência que não foram encontrados registros para aquele critério na base atual.
3. Use formatação Markdown (títulos, negrito para nomes de pesquisadores e instituições, tópicos) para facilitar a leitura.
4. Baseie-se ESTRITAMENTE nas informações fornecidas no contexto acima. Não invente formações ou produções.
"""

        answer = await self.llm.generate(synthesis_prompt)

        return ChatResponse(
            answer=answer,
            intent=plan.intent,
            filters_extracted=plan.filters.model_dump(exclude_none=True),
            researchers=researchers,
            productions=productions,
            sources=[
                f'{r["name"]} ({r.get("institution_acronym") or r.get("institution") or "BA"})'
                for r in researchers
            ]
            if researchers
            else [],
        )
