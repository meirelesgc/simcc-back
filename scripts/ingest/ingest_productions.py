import argparse
import asyncio
import os
import sys
from typing import List, Optional

# Ajusta o path para importar os módulos internos corretamente
sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src'))
)

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from simcc.ai.providers.openai_provider import OpenAIProvider
from simcc.core.db.models.ai import SearchDocumentProduction
from simcc.core.db.models.institution import Institution
from simcc.core.db.models.production import (
    BibliographicProduction,
    BibliographicProductionArticle,
    BibliographicProductionBook,
    BibliographicProductionBookChapter,
    Patent,
    ResearchReport,
    Software,
)
from simcc.core.db.models.researcher import Researcher
from simcc.core.settings import Settings


async def process_articles(
    session: AsyncSession, ai_provider: OpenAIProvider, limit: Optional[int]
):
    print('==> Processando Artigos...')
    stmt = (
        select(
            BibliographicProduction,
            BibliographicProductionArticle,
            Researcher,
            Institution,
        )
        .join(
            Researcher,
            Researcher.id == BibliographicProduction.researcher_id,
        )
        .outerjoin(Institution, Institution.id == Researcher.institution_id)
        .outerjoin(
            BibliographicProductionArticle,
            BibliographicProductionArticle.bibliographic_production_id
            == BibliographicProduction.id,
        )
        .filter(BibliographicProduction.type == 'ARTICLE')
    )
    if limit:
        stmt = stmt.limit(limit)

    result = await session.execute(stmt)
    rows = result.all()

    for bp, art, r, inst in rows:
        inst_name = (
            f'{inst.name} ({inst.acronym})'
            if (inst and inst.acronym)
            else (inst.name if inst else 'Não informada')
        )
        periodical = art.periodical_magazine_name if art else ''
        qualis = art.qualis if art else ''
        doi = bp.doi or ''
        year = bp.year or (str(bp.year_) if bp.year_ else '')

        document = (
            f'Tipo: Artigo Publicado\n'
            f'Título: {bp.title}\n'
            f'Autores: {bp.authors or r.name}\n'
            f'Pesquisador Responsável: {r.name}\n'
            f'Instituição: {inst_name}\n'
            f'Ano: {year}\n'
            f'Periódico/Revista: {periodical}\n'
            f'Qualis: {qualis}\n'
            f'DOI: {doi}\n'
        )

        embedding = await ai_provider.get_embeddings(document)

        await session.execute(
            delete(SearchDocumentProduction).filter(
                SearchDocumentProduction.production_id == bp.id
            )
        )

        doc = SearchDocumentProduction(
            production_id=bp.id,
            type='ARTICLE',
            document_content=document,
            embedding=embedding,
        )
        session.add(doc)

    print(f'Artigos processados: {len(rows)}')


async def process_books(
    session: AsyncSession, ai_provider: OpenAIProvider, limit: Optional[int]
):
    print('==> Processando Livros...')
    stmt = (
        select(
            BibliographicProduction,
            BibliographicProductionBook,
            Researcher,
            Institution,
        )
        .join(
            Researcher,
            Researcher.id == BibliographicProduction.researcher_id,
        )
        .outerjoin(Institution, Institution.id == Researcher.institution_id)
        .outerjoin(
            BibliographicProductionBook,
            BibliographicProductionBook.bibliographic_production_id
            == BibliographicProduction.id,
        )
        .filter(BibliographicProduction.type == 'BOOK')
    )
    if limit:
        stmt = stmt.limit(limit)

    result = await session.execute(stmt)
    rows = result.all()

    for bp, bk, r, inst in rows:
        inst_name = (
            f'{inst.name} ({inst.acronym})'
            if (inst and inst.acronym)
            else (inst.name if inst else 'Não informada')
        )
        publisher = bk.publishing_company if bk else ''
        isbn = bk.isbn if bk else ''
        year = bp.year or (str(bp.year_) if bp.year_ else '')

        document = (
            f'Tipo: Livro Publicado\n'
            f'Título: {bp.title}\n'
            f'Autores: {bp.authors or r.name}\n'
            f'Pesquisador Responsável: {r.name}\n'
            f'Instituição: {inst_name}\n'
            f'Ano: {year}\n'
            f'Editora: {publisher}\n'
            f'ISBN: {isbn}\n'
        )

        embedding = await ai_provider.get_embeddings(document)

        await session.execute(
            delete(SearchDocumentProduction).filter(
                SearchDocumentProduction.production_id == bp.id
            )
        )

        doc = SearchDocumentProduction(
            production_id=bp.id,
            type='BOOK',
            document_content=document,
            embedding=embedding,
        )
        session.add(doc)

    print(f'Livros processados: {len(rows)}')


async def process_book_chapters(
    session: AsyncSession, ai_provider: OpenAIProvider, limit: Optional[int]
):
    print('==> Processando Capítulos de Livros...')
    stmt = (
        select(
            BibliographicProduction,
            BibliographicProductionBookChapter,
            Researcher,
            Institution,
        )
        .join(
            Researcher,
            Researcher.id == BibliographicProduction.researcher_id,
        )
        .outerjoin(Institution, Institution.id == Researcher.institution_id)
        .outerjoin(
            BibliographicProductionBookChapter,
            BibliographicProductionBookChapter.bibliographic_production_id
            == BibliographicProduction.id,
        )
        .filter(BibliographicProduction.type == 'BOOK_CHAPTER')
    )
    if limit:
        stmt = stmt.limit(limit)

    result = await session.execute(stmt)
    rows = result.all()

    for bp, chp, r, inst in rows:
        inst_name = (
            f'{inst.name} ({inst.acronym})'
            if (inst and inst.acronym)
            else (inst.name if inst else 'Não informada')
        )
        book_title = chp.book_title if chp else ''
        publisher = chp.publishing_company if chp else ''
        organizers = chp.organizers if chp else ''
        isbn = chp.isbn if chp else ''
        year = bp.year or (str(bp.year_) if bp.year_ else '')

        document = (
            f'Tipo: Capítulo de Livro\n'
            f'Título do Capítulo: {bp.title}\n'
            f'Livro: {book_title}\n'
            f'Organizadores: {organizers}\n'
            f'Autores: {bp.authors or r.name}\n'
            f'Pesquisador Responsável: {r.name}\n'
            f'Instituição: {inst_name}\n'
            f'Ano: {year}\n'
            f'Editora: {publisher}\n'
            f'ISBN: {isbn}\n'
        )

        embedding = await ai_provider.get_embeddings(document)

        await session.execute(
            delete(SearchDocumentProduction).filter(
                SearchDocumentProduction.production_id == bp.id
            )
        )

        doc = SearchDocumentProduction(
            production_id=bp.id,
            type='BOOK_CHAPTER',
            document_content=document,
            embedding=embedding,
        )
        session.add(doc)

    print(f'Capítulos de livros processados: {len(rows)}')


async def process_patents(
    session: AsyncSession, ai_provider: OpenAIProvider, limit: Optional[int]
):
    print('==> Processando Patentes...')
    stmt = (
        select(Patent, Researcher, Institution)
        .join(Researcher, Researcher.id == Patent.researcher_id)
        .outerjoin(Institution, Institution.id == Researcher.institution_id)
    )
    if limit:
        stmt = stmt.limit(limit)

    result = await session.execute(stmt)
    rows = result.all()

    for pat, r, inst in rows:
        inst_name = (
            f'{inst.name} ({inst.acronym})'
            if (inst and inst.acronym)
            else (inst.name if inst else 'Não informada')
        )
        year = pat.development_year or (
            pat.deposit_date[:4] if pat.deposit_date else ''
        )

        document = (
            f'Tipo: Patente\n'
            f'Título: {pat.title or "Patente"}\n'
            f'Categoria: {pat.category or ""}\n'
            f'Código: {pat.code or ""}\n'
            f'Detalhes/Resumo: {pat.details or ""}\n'
            f'Pesquisador/Inventor: {r.name}\n'
            f'Instituição: {inst_name}\n'
            f'Ano: {year}\n'
        )

        embedding = await ai_provider.get_embeddings(document)

        await session.execute(
            delete(SearchDocumentProduction).filter(
                SearchDocumentProduction.production_id == pat.id
            )
        )

        doc = SearchDocumentProduction(
            production_id=pat.id,
            type='PATENT',
            document_content=document,
            embedding=embedding,
        )
        session.add(doc)

    print(f'Patentes processadas: {len(rows)}')


async def process_softwares(
    session: AsyncSession, ai_provider: OpenAIProvider, limit: Optional[int]
):
    print('==> Processando Softwares...')
    stmt = (
        select(Software, Researcher, Institution)
        .join(Researcher, Researcher.id == Software.researcher_id)
        .outerjoin(Institution, Institution.id == Researcher.institution_id)
    )
    if limit:
        stmt = stmt.limit(limit)

    result = await session.execute(stmt)
    rows = result.all()

    for sft, r, inst in rows:
        inst_name = (
            f'{inst.name} ({inst.acronym})'
            if (inst and inst.acronym)
            else (inst.name if inst else 'Não informada')
        )
        year = str(sft.year) if sft.year else ''

        document = (
            f'Tipo: Software / Programa de Computador\n'
            f'Título: {sft.title or "Software"}\n'
            f'Plataforma/Ambiente: {sft.platform or ""} / {sft.environment or ""}\n'
            f'Objetivo/Finalidade: {sft.goal or ""}\n'
            f'Financiador: {sft.financing_institutionc or ""}\n'
            f'Pesquisador Responsável: {r.name}\n'
            f'Instituição: {inst_name}\n'
            f'Ano: {year}\n'
        )

        embedding = await ai_provider.get_embeddings(document)

        await session.execute(
            delete(SearchDocumentProduction).filter(
                SearchDocumentProduction.production_id == sft.id
            )
        )

        doc = SearchDocumentProduction(
            production_id=sft.id,
            type='SOFTWARE',
            document_content=document,
            embedding=embedding,
        )
        session.add(doc)

    print(f'Softwares processados: {len(rows)}')


async def process_reports(
    session: AsyncSession, ai_provider: OpenAIProvider, limit: Optional[int]
):
    print('==> Processando Relatórios Técnicos...')
    stmt = (
        select(ResearchReport, Researcher, Institution)
        .join(Researcher, Researcher.id == ResearchReport.researcher_id)
        .outerjoin(Institution, Institution.id == Researcher.institution_id)
    )
    if limit:
        stmt = stmt.limit(limit)

    result = await session.execute(stmt)
    rows = result.all()

    for rep, r, inst in rows:
        inst_name = (
            f'{inst.name} ({inst.acronym})'
            if (inst and inst.acronym)
            else (inst.name if inst else 'Não informada')
        )
        year = str(rep.year) if rep.year else ''

        document = (
            f'Tipo: Relatório Técnico ou Científico\n'
            f'Título: {rep.title or "Relatório Técnico"}\n'
            f'Projeto: {rep.project_name or ""}\n'
            f'Financiador: {rep.financing_institutionc or ""}\n'
            f'Pesquisador Responsável: {r.name}\n'
            f'Instituição: {inst_name}\n'
            f'Ano: {year}\n'
        )

        embedding = await ai_provider.get_embeddings(document)

        await session.execute(
            delete(SearchDocumentProduction).filter(
                SearchDocumentProduction.production_id == rep.id
            )
        )

        doc = SearchDocumentProduction(
            production_id=rep.id,
            type='REPORT',
            document_content=document,
            embedding=embedding,
        )
        session.add(doc)

    print(f'Relatórios técnicos processados: {len(rows)}')


async def run_ingestion(
    types: Optional[List[str]] = None, limit: Optional[int] = None
):
    settings = Settings()
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )

    if not settings.OPENAI_API_KEY:
        print('OPENAI_API_KEY não configurada. Abortando ingestão.')
        return

    ai_provider = OpenAIProvider(api_key=settings.OPENAI_API_KEY)
    active_types = [t.upper() for t in types] if types else []

    async with async_session() as session:
        if not active_types or 'ARTICLE' in active_types:
            await process_articles(session, ai_provider, limit)
        if not active_types or 'BOOK' in active_types:
            await process_books(session, ai_provider, limit)
        if not active_types or 'BOOK_CHAPTER' in active_types:
            await process_book_chapters(session, ai_provider, limit)
        if not active_types or 'PATENT' in active_types:
            await process_patents(session, ai_provider, limit)
        if not active_types or 'SOFTWARE' in active_types:
            await process_softwares(session, ai_provider, limit)
        if not active_types or 'REPORT' in active_types:
            await process_reports(session, ai_provider, limit)

        await session.commit()
        print('Ingestão de produções concluída com sucesso.')


def main():
    parser = argparse.ArgumentParser(
        description='Ingestão e indexação vetorial de produções científicas para o modelo SearchDocumentProduction'
    )
    parser.add_argument(
        '--types',
        type=str,
        default=None,
        help='Tipos de produção separados por vírgula (ex: ARTICLE,BOOK,BOOK_CHAPTER,PATENT,SOFTWARE,REPORT)',
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limite de produções a processar por tipo (ex: 10)',
    )
    args = parser.parse_args()

    selected_types = (
        [t.strip() for t in args.types.split(',') if t.strip()]
        if args.types
        else None
    )

    asyncio.run(run_ingestion(types=selected_types, limit=args.limit))


if __name__ == '__main__':
    main()
