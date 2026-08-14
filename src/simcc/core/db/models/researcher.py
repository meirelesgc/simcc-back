from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import (
    ARRAY,
    Boolean,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from simcc.core.db.models.base import table_registry


@table_registry.mapped_as_dataclass
class Researcher:
    __tablename__ = 'researcher'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String)
    lattes_id: Mapped[Optional[str]] = mapped_column(
        String, unique=True, default=None
    )
    lattes_10_id: Mapped[Optional[str]] = mapped_column(
        String, unique=True, default=None
    )
    last_update: Mapped[datetime] = mapped_column(
        server_default=text('now()'), init=False
    )
    has_image: Mapped[bool] = mapped_column(
        Boolean, server_default=text('false'), default=False
    )
    citations: Mapped[Optional[str]] = mapped_column(String, default=None)
    orcid: Mapped[Optional[str]] = mapped_column(String, default=None)
    abstract: Mapped[Optional[str]] = mapped_column(Text, default=None)
    abstract_en: Mapped[Optional[str]] = mapped_column(Text, default=None)
    abstract_ai: Mapped[Optional[str]] = mapped_column(Text, default=None)
    other_information: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    city_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('city.id'), default=None
    )
    country_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('country.id'), default=None
    )
    qtt_publications: Mapped[Optional[int]] = mapped_column(
        Integer, default=None
    )
    institution_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('institution.id'),
        default=None,
    )
    graduate_program: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    graduation: Mapped[Optional[str]] = mapped_column(String, default=None)
    status: Mapped[bool] = mapped_column(
        Boolean, server_default=text('true'), default=True
    )
    classification: Mapped[Optional[str]] = mapped_column(String, default=None)
    stars: Mapped[int] = mapped_column(
        Integer, server_default=text('0'), default=0
    )
    update_abstract: Mapped[Optional[bool]] = mapped_column(
        Boolean, default=True
    )
    docente: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    extra_field: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class ResearcherAddress:
    __tablename__ = 'researcher_address'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('researcher.id', ondelete='CASCADE')
    )
    city: Mapped[Optional[str]] = mapped_column(String, default=None)
    organ: Mapped[Optional[str]] = mapped_column(String, default=None)
    unity: Mapped[Optional[str]] = mapped_column(String, default=None)
    institution: Mapped[Optional[str]] = mapped_column(String, default=None)
    public_place: Mapped[Optional[str]] = mapped_column(String, default=None)
    district: Mapped[Optional[str]] = mapped_column(String, default=None)
    cep: Mapped[Optional[str]] = mapped_column(String, default=None)
    mailbox: Mapped[Optional[str]] = mapped_column(String, default=None)
    fax: Mapped[Optional[str]] = mapped_column(String, default=None)
    url_homepage: Mapped[Optional[str]] = mapped_column(String, default=None)
    telephone: Mapped[Optional[str]] = mapped_column(String, default=None)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    uf: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class ResearcherAreaExpertise:
    __tablename__ = 'researcher_area_expertise'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('researcher.id', ondelete='CASCADE')
    )
    sub_area_expertise_id: Mapped[UUID] = mapped_column(
        ForeignKey('sub_area_expertise.id')
    )
    order: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    area_expertise_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('area_expertise.id'), default=None
    )
    great_area_expertise_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('great_area_expertise.id'), default=None
    )
    area_specialty_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('area_specialty.id'), default=None
    )


@table_registry.mapped_as_dataclass
class Education:
    __tablename__ = 'education'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('researcher.id', ondelete='CASCADE')
    )
    degree: Mapped[str] = mapped_column(String)
    education_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    education_start: Mapped[Optional[int]] = mapped_column(
        Integer, default=None
    )
    education_end: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    key_words: Mapped[Optional[str]] = mapped_column(String, default=None)
    institution: Mapped[Optional[str]] = mapped_column(String, default=None)
    status: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class ResearcherProfessionalExperience:
    __tablename__ = 'researcher_professional_experience'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('researcher.id', ondelete='CASCADE')
    )
    enterprise: Mapped[str] = mapped_column(String)
    start_year: Mapped[int] = mapped_column(Integer)
    end_year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    employment_type: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    other_employment_type: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    functional_classification: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    other_functional_classification: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    workload_hours_weekly: Mapped[Optional[int]] = mapped_column(
        Integer, default=None
    )
    exclusive_dedication: Mapped[Optional[bool]] = mapped_column(
        Boolean, default=None
    )
    additional_info: Mapped[Optional[str]] = mapped_column(Text, default=None)


@table_registry.mapped_as_dataclass
class ResearcherProduction:
    __tablename__ = 'researcher_production'

    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('researcher.id', ondelete='CASCADE'), primary_key=True
    )
    city: Mapped[Optional[str]] = mapped_column(String, default=None)
    organ: Mapped[Optional[str]] = mapped_column(String, default=None)
    area_specialty: Mapped[Optional[str]] = mapped_column(String, default=None)
    great_area: Mapped[Optional[str]] = mapped_column(String, default=None)
    great_area_: Mapped[Optional[list[str]]] = mapped_column(
        ARRAY(String), default=None
    )
    articles: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    book_chapters: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    book: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    patent: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    software: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    brand: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    work_in_event: Mapped[Optional[int]] = mapped_column(Integer, default=0)


@table_registry.mapped_as_dataclass
class ResearcherIndProd:
    __tablename__ = 'researcher_ind_prod'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('researcher.id', ondelete='CASCADE'), primary_key=True
    )
    year: Mapped[int] = mapped_column(Integer, primary_key=True)
    ind_prod_article: Mapped[Optional[float]] = mapped_column(
        Float, default=None
    )
    ind_prod_book: Mapped[Optional[float]] = mapped_column(Float, default=None)
    ind_prod_book_chapter: Mapped[Optional[float]] = mapped_column(
        Float, default=None
    )
    ind_prod_software: Mapped[Optional[float]] = mapped_column(
        Float, default=None
    )
    ind_prod_report: Mapped[Optional[float]] = mapped_column(
        Float, default=None
    )
    ind_prod_granted_patent: Mapped[Optional[float]] = mapped_column(
        Float, default=None
    )
    ind_prod_not_granted_patent: Mapped[Optional[float]] = mapped_column(
        Float, default=None
    )
    ind_prod_guidance: Mapped[Optional[float]] = mapped_column(
        Float, default=None
    )


@table_registry.mapped_as_dataclass
class RelevantProduction:
    __tablename__ = 'relevant_production'

    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('researcher.id', ondelete='CASCADE'), primary_key=True
    )
    production_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True
    )
    type: Mapped[str] = mapped_column(String, primary_key=True)
    has_image: Mapped[bool] = mapped_column(Boolean, default=False)
