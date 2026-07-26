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
