from enum import Enum
from typing import Literal, Optional
from uuid import UUID

from pydantic import AliasChoices, BaseModel, Field


class DefaultFilters(BaseModel):
    term: Optional[str] = Field(
        default=None, validation_alias=AliasChoices('term', 'terms')
    )
    researcher_id: Optional[UUID | str] = Field(
        default=None, alias='researcher_id'
    )
    graduate_program_id: Optional[UUID | str] = Field(
        default=None, alias='graduate_program_id'
    )
    dep_id: Optional[str] = None
    departament: Optional[str] = None
    year: int = 2020
    type: Optional[
        Literal[
            'BOOK',
            'BOOK_CHAPTER',
            'ARTICLE',
            'WORK_IN_EVENT',
            'TEXT_IN_NEWSPAPER_MAGAZINE',
            'ABSTRACT',
            'PATENT',
            'AREA',
            'EVENT',
            'NAME',
        ]
        | str
    ] = None
    distinct: int | str = 1
    institution: Optional[str] = None
    graduate_program: Optional[str] = None
    city: Optional[str] = None
    area: Optional[str] = None
    modality: Optional[str] = None
    graduation: Optional[str] = None

    page: Optional[int] = 1
    lenght: Optional[int] = 10

    model_config = {
        'populate_by_name': True,
        'json_encoders': {UUID: str},
    }


class ResearcherOptions(str, Enum):
    ARTICLE = 'ARTICLE'
    ABSTRACT = 'ABSTRACT'


class YearBarema(BaseModel):
    article: Optional[str] = 2020
    work_event: Optional[str] = 2020
    book: Optional[str] = 2020
    chapter_book: Optional[str] = 2020
    patent: Optional[str] = 2020
    software: Optional[str] = 2020
    brand: Optional[str] = 2020
    resource_progress: Optional[str] = 2020
    resource_completed: Optional[str] = 2020
    participation_events: Optional[str] = 2020


class ResearcherBarema(BaseModel):
    researcher_id: UUID
    lattes_id: str

    A1: int
    A2: int
    A3: int
    A4: int
    B1: int
    B2: int
    B3: int
    B4: int
    C: int
    SQ: int

    software: int
    brand: int
    book: int
    book_chapter: int
    patent: int

    ic_completed: int
    ic_in_progress: int
    m_completed: int
    m_in_progress: int
    d_completed: int
    d_in_progress: int
    g_completed: int
    g_in_progress: int
    e_completed: int
    e_in_progress: int

    event_organization: int
    work_in_event: int
    participation_events: int

    lattes_10_id: str
    graduation: str
    researcher: str

    university: str
    city: str
    area: str


class ArticleOptions(str, Enum):
    ARTICLE = 'ARTICLE'
    ABSTRACT = 'ABSTRACT'


class QualisOptions(str, Enum):
    A1: str = 'A1'
    A2: str = 'A2'
    A3: str = 'A3'
    A4: str = 'A4'
    B1: str = 'B1'
    B2: str = 'B2'
    B3: str = 'B3'
    B4: str = 'B4'
    C: str = 'C'
    SQ: str = 'SQ'
