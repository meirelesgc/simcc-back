from uuid import UUID

from fastapi import APIRouter

from simcc.schemas import ArticleOptions, QualisOptions
from simcc.schemas.Production.Article import ArticleProduction
from simcc.schemas.Production.Book import BookProduction
from simcc.schemas.Production.BookChapter import BookChapterProduction
from simcc.schemas.Production.Brand import BrandProduction
from simcc.schemas.Production.Event import EventProduction
from simcc.schemas.Production.Guidance import GuidanceProduction
from simcc.schemas.Production.Papers import PapersProduction
from simcc.schemas.Production.Patent import PatentProduction
from simcc.schemas.Production.ProfessionalExperience import (
    ProfessionalExperience,
)
from simcc.schemas.Production.Report import ReportProduction
from simcc.schemas.Production.ResearchProject import ResearchProjectProduction
from simcc.schemas.Production.Software import SoftwareProduction
from simcc.services import ProductionService

router = APIRouter()


@router.get(
    '/professional_experience',
    response_model=list[ProfessionalExperience],
)
def get_professional_experience(
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
    page: int = None,
    lenght: int = None,
):
    return ProductionService.professional_experience(
        researcher_id,
        graduate_program_id,
        dep_id,
        departament,
        year,
        distinct,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
        page,
        lenght,
    )


@router.get(
    '/patent_production_researcher',
    response_model=list[PatentProduction],
)
def list_patent_production(
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
    page: int = None,
    lenght: int = None,
):
    if distinct:
        # DEBITO
        pass
    else:
        patents = ProductionService.list_patent(
            term,
            researcher_id,
            graduate_program_id,
            dep_id,
            departament,
            year,
            institution,
            graduate_program,
            city,
            area,
            modality,
            graduation,
            page,
            lenght,
        )
    return patents


@router.get(
    '/book_production_researcher',
    response_model=list[BookProduction],
)
def list_book_production(
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
    page: int = None,
    lenght: int = None,
):
    if distinct:
        # DEBITO
        pass
    else:
        books = ProductionService.list_book(
            term,
            researcher_id,
            graduate_program_id,
            dep_id,
            departament,
            year,
            distinct,
            institution,
            graduate_program,
            city,
            area,
            modality,
            graduation,
            page,
            lenght,
        )
    return books


@router.get(
    '/brand_production_researcher',
    response_model=list[BrandProduction],
)
def list_brand_production(
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
    page: int = None,
    lenght: int = None,
):
    if distinct:
        # DEBITO
        pass
    return ProductionService.list_brand(
        term,
        researcher_id,
        graduate_program_id,
        dep_id,
        departament,
        year,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
        page,
        lenght,
    )


@router.get(
    '/researcher_report',
    response_model=list[ReportProduction],
)
def list_researcher_report(
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
    page: int = None,
    lenght: int = None,
):
    if distinct:
        # DEBITO
        pass
    return ProductionService.list_researcher_report(
        term,
        researcher_id,
        graduate_program_id,
        dep_id,
        departament,
        year,
        distinct,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
        page,
        lenght,
    )


@router.get(
    '/book_chapter_production_researcher',
    response_model=list[BookChapterProduction],
)
def list_book_chapter_production(
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    distinct: int = 1,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
    page: int = None,
    lenght: int = None,
):
    if distinct:
        # DEBITO
        pass
    else:
        books = ProductionService.list_book_chapter(
            term,
            researcher_id,
            graduate_program_id,
            dep_id,
            departament,
            year,
            institution,
            graduate_program,
            city,
            area,
            modality,
            graduation,
            page,
            lenght,
        )
    return books


@router.get(
    '/outstanding_articles',
    response_model=list[ArticleProduction],
)
def list_outstanding_articles():
    articles = ProductionService.list_bibliographic_production(
        None, None, None, 'ARTICLE', None, None, 1, 20, True
    )
    return articles


@router.get(
    '/bibliographic_production_researcher',
    response_model=list[ArticleProduction],
)
def list_bibliographic_production(
    type: ArticleOptions = 'ARTICLE',
    qualis: QualisOptions | str = str(),
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
    page: int = None,
    lenght: int = None,
):
    articles = ProductionService.list_bibliographic_production(
        type,
        qualis,
        term,
        researcher_id,
        graduate_program_id,
        dep_id,
        departament,
        year,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
        page,
        lenght,
    )
    return articles


@router.get(
    '/software_production_researcher',
    response_model=list[SoftwareProduction],
)
def list_software_production(
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
    page: int = None,
    lenght: int = None,
    distinct: int = 1,
):
    if distinct:
        # DEBITO
        pass
    return ProductionService.list_software(
        term,
        researcher_id,
        graduate_program_id,
        dep_id,
        departament,
        year,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
        page,
        lenght,
        distinct,
    )


@router.get(
    '/bibliographic_production_article',
    response_model=list[ArticleProduction],
)
def list_article_production(
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
    page: int = None,
    lenght: int = None,
    distinct: int = 1,
):
    if distinct:
        # DEBITO
        pass

    return ProductionService.list_article_production(
        term,
        researcher_id,
        graduate_program_id,
        dep_id,
        departament,
        year,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
        page,
        lenght,
        distinct,
    )


@router.get(
    '/guidance_researcher',
    response_model=list[GuidanceProduction],
)
def list_guidance_production(
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
    page: int = None,
    lenght: int = None,
    distinct: int = 1,
):
    if distinct:
        # DEBITO
        pass
    return ProductionService.list_guidance_production(
        term,
        researcher_id,
        graduate_program_id,
        dep_id,
        departament,
        year,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
        page,
        lenght,
        distinct,
    )


@router.get(
    '/researcher_production/events',
    response_model=list[EventProduction],
)
def list_researcher_production_events(
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
    page: int = None,
    lenght: int = None,
    distinct: int = 1,
):
    if distinct:
        # DEBITO
        pass
    return ProductionService.list_researcher_production_events(
        term,
        researcher_id,
        graduate_program_id,
        dep_id,
        departament,
        year,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
        page,
        lenght,
        distinct,
    )


@router.get(
    '/researcher_research_project',
    response_model=list[ResearchProjectProduction],
)
def list_research_project(
    term: str = None,
    researcher_id: UUID | str = None,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    departament: str = None,
    year: int = 2020,
    institution: str = None,
    graduate_program: str = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
    page: int = None,
    lenght: int = None,
    distinct: int = 1,
):
    if distinct:
        # DEBITO
        pass
    return ProductionService.list_research_projects(
        term,
        researcher_id,
        graduate_program_id,
        dep_id,
        departament,
        year,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
        page,
        lenght,
        distinct,
    )


@router.get(
    '/researcher_production/papers_magazine',
    response_model=list[PapersProduction],
)
def list_papers_magazine(
    researcher_id: UUID | str = None,
    year: int | str = None,
    distinct: int = 1,
):
    if distinct:
        return ProductionService.list_distinct_papers_magazine(
            researcher_id, year
        )
    return ProductionService.list_papers_magazine(researcher_id, year)
