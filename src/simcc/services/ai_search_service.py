from typing import Any, Dict, List, Optional

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from simcc.ai.providers.base import EmbeddingsProvider
from simcc.core.db.models.ai import SearchDocumentResearcher
from simcc.core.db.models.institution import Institution
from simcc.core.db.models.researcher import Researcher


class AISearchService:
    def __init__(self, embeddings_provider: EmbeddingsProvider):
        self.embeddings = embeddings_provider

    async def search_researchers_hybrid(
        self,
        session: AsyncSession,
        query: str,
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Realiza uma busca híbrida por pesquisadores combinando:
        - Filtros exatos/parciais de instituições (siglas e nomes)
        - Busca por nome próprio do pesquisador
        - Similaridade semântica vetorial (pgvector cosine_distance)
        """
        filters = filters or {}

        # 1. Base query
        stmt = (
            select(SearchDocumentResearcher, Researcher, Institution)
            .join(
                Researcher,
                Researcher.id == SearchDocumentResearcher.researcher_id,
            )
            .outerjoin(
                Institution, Institution.id == Researcher.institution_id
            )
        )

        # 2. Filtro por Instituições (suporta lista de siglas/nomes: UFBA, UNEB, etc)
        institutions = filters.get('institutions', [])
        if isinstance(institutions, str):
            institutions = [institutions]

        if institutions:
            inst_conditions = []
            for inst in institutions:
                inst_clean = inst.strip()
                if inst_clean:
                    inst_conditions.append(
                        Institution.acronym.ilike(f'%{inst_clean}%')
                    )
                    inst_conditions.append(
                        Institution.name.ilike(f'%{inst_clean}%')
                    )
            if inst_conditions:
                stmt = stmt.filter(or_(*inst_conditions))

        # 3. Filtro por Nome do Pesquisador (se especificado)
        researcher_name = filters.get('researcher_name')
        if researcher_name:
            tokens = [t.strip() for t in researcher_name.split() if t.strip()]
            for tok in tokens:
                stmt = stmt.filter(Researcher.name.ilike(f'%{tok}%'))

        # 4. Ordenação e Busca Semântica
        if query and query.strip():
            vector = await self.embeddings.get_embeddings(query.strip())
            stmt = stmt.order_by(
                SearchDocumentResearcher.embedding.cosine_distance(vector)
            )
        else:
            stmt = stmt.order_by(Researcher.name.asc())

        stmt = stmt.limit(limit)

        result = await session.execute(stmt)
        rows = result.all()

        # 5. Mapear e retornar
        response = []
        for doc, researcher, institution in rows:
            response.append({
                'id': str(researcher.id),
                'name': researcher.name,
                'institution': institution.name if institution else None,
                'institution_acronym': institution.acronym
                if institution
                else None,
                'lattes_id': researcher.lattes_id,
                'abstract': researcher.abstract or researcher.abstract_ai,
                'semantic_content': doc.document_content,
            })

        return response
