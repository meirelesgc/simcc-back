from typing import Literal, Optional, Union
from uuid import UUID

from pydantic import BaseModel


class ArticleProduction(BaseModel):
    # Identificadores e Relacionamentos
    id: Union[UUID, list[UUID]]
    researcher: Union[str, list[str]]
    researcher_id: Union[UUID, list[UUID]]
    lattes_id: Union[str, list[str]]
    lattes_10_id: Union[str, list[str]]
    authors: Optional[str]

    # Dados Básicos
    title: str
    year: int
    type: str
    language: Optional[str]
    abstract: Optional[str]
    keywords: Optional[str]

    # Dados Específicos
    doi: Optional[str]
    qualis: Literal['A1', 'A2', 'A3', 'A4', 'B1', 'B2', 'B3', 'B4', 'C', 'SQ']
    magazine: str
    jif: Optional[str]
    jcr_link: Optional[str]
    article_institution: Optional[str]
    authors_institution: Optional[str]
    issn: Optional[str]
    landing_page_url: Optional[str]
    pdf: Optional[str]

    # Métricas e Flags
    stars: int = 0
    citations_count: Optional[Union[int, str]]
    has_image: Optional[bool] = False
    relevance: Optional[bool] = False


class BookProduction(BaseModel):
    # Identificadores e Relacionamentos
    id: Union[UUID, list[UUID]]
    researcher: Union[UUID, list[UUID]]
    lattes_id: Union[str, list[str]]
    name: Union[str, list[str]]

    # Dados Básicos
    title: str
    year: int

    # Dados Específicos
    isbn: Optional[str]
    publishing_company: Optional[str]

    # Métricas e Flags
    stars: int = 0
    has_image: Optional[bool] = False
    relevance: Optional[bool] = False


class BookChapterProduction(BaseModel):
    # Identificadores e Relacionamentos
    id: Union[UUID, list[UUID]]
    researcher: Union[UUID, list[UUID]]
    lattes_id: Union[str, list[str]]
    name: Union[str, list[str]]

    # Dados Básicos
    title: str
    year: int

    # Dados Específicos
    isbn: Optional[str]
    publishing_company: Optional[str]

    # Métricas e Flags
    stars: int = 0
    has_image: Optional[bool] = False
    relevance: Optional[bool] = False


class PapersProduction(BaseModel):
    # Identificadores e Relacionamentos
    name: Union[str, list[str]]
    authors: Optional[Union[str, list]]

    # Dados Básicos
    title: str
    title_en: Optional[str]
    year_: int
    language: Optional[str]

    # Dados Específicos
    nature: str
    means_divulgation: str
    homepage: Optional[str]

    # Métricas e Flags
    stars: int = 0
    has_image: Optional[bool] = False
    relevance: Optional[bool] = False
    scientific_divulgation: Optional[bool]
