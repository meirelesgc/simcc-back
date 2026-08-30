import asyncio
import os
import sys

# Ajusta o path para importar os módulos internos corretamente
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from simcc.ai.providers.openai_provider import OpenAIProvider
from simcc.core.db.models.ai import SearchDocumentResearcher
from simcc.core.db.models.expertise import (
    AreaExpertise,
    GreatAreaExpertise,
    SubAreaExpertise,
)
from simcc.core.db.models.institution import Institution
from simcc.core.db.models.researcher import (
    Education,
    Researcher,
    ResearcherAreaExpertise,
    ResearcherProfessionalExperience,
)
from simcc.core.settings import Settings


async def run_ingestion():
    settings = Settings()
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )

    if not settings.OPENAI_API_KEY:
        print("OPENAI_API_KEY não configurada. Abortando ingestão.")
        return

    ai_provider = OpenAIProvider(api_key=settings.OPENAI_API_KEY)

    async with async_session() as session:
        # Busca os 10 primeiros pesquisadores
        query = (
            select(Researcher, Institution)
            .outerjoin(Institution, Institution.id == Researcher.institution_id)
            .limit(10)
        )

        result = await session.execute(query)
        rows = result.all()

        if not rows:
            print("Nenhum pesquisador encontrado.")
            return

        print(f"Indexando com dados enriquecidos {len(rows)} pesquisadores...")

        for r, inst in rows:
            print(f"Processando: {r.name}")

            # 1. Instituição
            institution_str = ""
            if inst:
                institution_str = f"{inst.name} ({inst.acronym})" if inst.acronym else inst.name
            else:
                institution_str = "Não informada"

            # 2. Áreas de Conhecimento
            areas_stmt = (
                select(AreaExpertise.name, SubAreaExpertise.name, GreatAreaExpertise.name)
                .select_from(ResearcherAreaExpertise)
                .outerjoin(AreaExpertise, AreaExpertise.id == ResearcherAreaExpertise.area_expertise_id)
                .outerjoin(SubAreaExpertise, SubAreaExpertise.id == ResearcherAreaExpertise.sub_area_expertise_id)
                .outerjoin(GreatAreaExpertise, GreatAreaExpertise.id == ResearcherAreaExpertise.great_area_expertise_id)
                .filter(ResearcherAreaExpertise.researcher_id == r.id)
            )
            areas_res = await session.execute(areas_stmt)
            areas_list = []
            for ae_name, sa_name, ga_name in areas_res.all():
                parts = [p for p in [ga_name, ae_name, sa_name] if p]
                if parts:
                    areas_list.append(" / ".join(parts))
            areas_str = "; ".join(areas_list) if areas_list else "Não informada"

            # 3. Formação Acadêmica
            edu_stmt = select(Education).filter(Education.researcher_id == r.id)
            edu_res = await session.execute(edu_stmt)
            edu_list = []
            for e in edu_res.scalars().all():
                edu_parts = [e.degree or "", e.education_name or "", e.institution or ""]
                edu_list.append(" - ".join([p for p in edu_parts if p]))
            edu_str = "\n".join(edu_list) if edu_list else "Conforme resumo Lattes"

            # 4. Experiência e Cargos
            exp_stmt = select(ResearcherProfessionalExperience).filter(
                ResearcherProfessionalExperience.researcher_id == r.id
            )
            exp_res = await session.execute(exp_stmt)
            exp_list = []
            for exp in exp_res.scalars().all():
                exp_parts = [
                    exp.enterprise,
                    exp.functional_classification or exp.other_functional_classification,
                    exp.additional_info,
                ]
                exp_list.append(" - ".join([p for p in exp_parts if p]))
            exp_str = "\n".join(exp_list) if exp_list else "Conforme resumo Lattes"

            # 5. Resumo Profissional
            abstract = r.abstract or r.abstract_ai or "Resumo não disponível"

            document = (
                f"Pesquisador: {r.name}\n"
                f"Instituição: {institution_str}\n"
                f"Áreas de Atuação e Especialidades: {areas_str}\n"
                f"Formação Acadêmica:\n{edu_str}\n"
                f"Experiência Profissional e Gestão:\n{exp_str}\n"
                f"Resumo do Currículo Lattes:\n{abstract}\n"
            )

            # Gera embedding de alta fidelidade
            embedding = await ai_provider.get_embeddings(document)

            # Deleta index anterior para este pesquisador se existir (upsert simples)
            await session.execute(
                delete(SearchDocumentResearcher).filter(
                    SearchDocumentResearcher.researcher_id == r.id
                )
            )

            # Salva o novo documento
            doc = SearchDocumentResearcher(
                researcher_id=r.id,
                document_content=document,
                embedding=embedding
            )
            session.add(doc)

        await session.commit()
        print("Ingestão enriquecida concluída com sucesso.")


if __name__ == "__main__":
    asyncio.run(run_ingestion())
