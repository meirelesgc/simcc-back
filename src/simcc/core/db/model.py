from datetime import date, datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import (
    ARRAY,
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, registry

table_registry = registry()


@table_registry.mapped_as_dataclass
class Country:
    __tablename__ = 'country'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String, unique=True)
    name_pt: Mapped[str] = mapped_column(String, unique=True)
    alpha_2_code: Mapped[Optional[str]] = mapped_column(
        String(2), unique=True, default=None
    )
    alpha_3_code: Mapped[Optional[str]] = mapped_column(
        String(3), unique=True, default=None
    )


@table_registry.mapped_as_dataclass
class State:
    __tablename__ = 'state'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String, unique=True)
    country_id: Mapped[UUID] = mapped_column(ForeignKey('country.id'))
    abbreviation: Mapped[Optional[str]] = mapped_column(
        String, unique=True, default=None
    )


@table_registry.mapped_as_dataclass
class City:
    __tablename__ = 'city'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String)
    country_id: Mapped[UUID] = mapped_column(ForeignKey('country.id'))
    state_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('state.id'), default=None
    )


@table_registry.mapped_as_dataclass
class JCR:
    __tablename__ = 'jcr'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    rank: Mapped[Optional[str]] = mapped_column(String, default=None)
    journalname: Mapped[Optional[str]] = mapped_column(String, default=None)
    jcryear: Mapped[Optional[str]] = mapped_column(String, default=None)
    abbrjournal: Mapped[Optional[str]] = mapped_column(String, default=None)
    issn: Mapped[Optional[str]] = mapped_column(String, default=None)
    eissn: Mapped[Optional[str]] = mapped_column(String, default=None)
    totalcites: Mapped[Optional[str]] = mapped_column(String, default=None)
    totalarticles: Mapped[Optional[str]] = mapped_column(String, default=None)
    citableitems: Mapped[Optional[str]] = mapped_column(String, default=None)
    citedhalflife: Mapped[Optional[str]] = mapped_column(String, default=None)
    citinghalflife: Mapped[Optional[str]] = mapped_column(String, default=None)
    jif2019: Mapped[Optional[float]] = mapped_column(default=None)
    url_revista: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class PeriodicalMagazine:
    __tablename__ = 'periodical_magazine'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    name: Mapped[Optional[str]] = mapped_column(String, default=None)
    issn: Mapped[Optional[str]] = mapped_column(String, default=None)
    qualis: Mapped[Optional[str]] = mapped_column(String, default=None)
    jcr: Mapped[Optional[str]] = mapped_column(String, default=None)
    jcr_link: Mapped[Optional[str]] = mapped_column(String, default=None)
    reference_period: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )


@table_registry.mapped_as_dataclass
class ResearchGroup:
    __tablename__ = 'research_group'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    name: Mapped[Optional[str]] = mapped_column(String, default=None)
    institution: Mapped[Optional[str]] = mapped_column(String, default=None)
    first_leader: Mapped[Optional[str]] = mapped_column(String, default=None)
    first_leader_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True), default=None
    )
    second_leader: Mapped[Optional[str]] = mapped_column(String, default=None)
    second_leader_id: Mapped[Optional[UUID]] = mapped_column(
        PG_UUID(as_uuid=True), default=None
    )
    area: Mapped[Optional[str]] = mapped_column(String, default=None)
    census: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    start_of_collection: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    end_of_collection: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    group_identifier: Mapped[Optional[str]] = mapped_column(
        String, unique=True, default=None
    )
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    institution_name: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    category: Mapped[Optional[str]] = mapped_column(String, default=None)

    __table_args__ = (
        UniqueConstraint(
            'name', 'institution', name='uq_research_group_name_institution'
        ),
    )


@table_registry.mapped_as_dataclass
class ResearchLines:
    __tablename__ = 'research_lines'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    research_group_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('research_group.id'), default=None
    )
    title: Mapped[Optional[str]] = mapped_column(Text, default=None)
    objective: Mapped[Optional[str]] = mapped_column(Text, default=None)
    keyword: Mapped[Optional[str]] = mapped_column(String, default=None)
    group_identifier: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    predominant_major_area: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    predominant_area: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )


@table_registry.mapped_as_dataclass
class Institution:
    __tablename__ = 'institution'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String, unique=True)
    acronym: Mapped[Optional[str]] = mapped_column(
        String, unique=True, default=None
    )
    description: Mapped[Optional[str]] = mapped_column(String, default=None)
    lattes_id: Mapped[Optional[str]] = mapped_column(String, default=None)
    cnpj: Mapped[Optional[str]] = mapped_column(
        String, unique=True, default=None
    )
    image: Mapped[Optional[str]] = mapped_column(String, default=None)
    latitude: Mapped[Optional[float]] = mapped_column(Float, default=None)
    longitude: Mapped[Optional[float]] = mapped_column(Float, default=None)


@table_registry.mapped_as_dataclass
class GreatAreaExpertise:
    __tablename__ = 'great_area_expertise'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String)


@table_registry.mapped_as_dataclass
class AreaExpertise:
    __tablename__ = 'area_expertise'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String)
    great_area_expertise_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('great_area_expertise.id'),
        default=None,
    )


@table_registry.mapped_as_dataclass
class SubAreaExpertise:
    __tablename__ = 'sub_area_expertise'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String)
    area_expertise_id: Mapped[UUID] = mapped_column(
        ForeignKey('area_expertise.id')
    )


@table_registry.mapped_as_dataclass
class AreaSpecialty:
    __tablename__ = 'area_specialty'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String)
    sub_area_expertise_id: Mapped[UUID] = mapped_column(
        ForeignKey('sub_area_expertise.id')
    )


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
    researcher_id: Mapped[UUID] = mapped_column(ForeignKey('researcher.id'))
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
    researcher_id: Mapped[UUID] = mapped_column(ForeignKey('researcher.id'))
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
    researcher_id: Mapped[UUID] = mapped_column(ForeignKey('researcher.id'))
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
class BibliographicProduction:
    __tablename__ = 'bibliographic_production'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    title: Mapped[str] = mapped_column(String)
    type: Mapped[str] = mapped_column(String)
    title_en: Mapped[Optional[str]] = mapped_column(String, default=None)
    doi: Mapped[Optional[str]] = mapped_column(String, default=None)
    nature: Mapped[Optional[str]] = mapped_column(String, default=None)
    year: Mapped[Optional[str]] = mapped_column(String, default=None)
    country_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('country.id'), default=None
    )
    language: Mapped[Optional[str]] = mapped_column(String, default=None)
    means_divulgation: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    homepage: Mapped[Optional[str]] = mapped_column(String, default=None)
    relevance: Mapped[bool] = mapped_column(
        Boolean, server_default=text('false'), default=False
    )
    has_image: Mapped[bool] = mapped_column(
        Boolean, server_default=text('false'), default=False
    )
    scientific_divulgation: Mapped[Optional[bool]] = mapped_column(
        Boolean, default=False
    )
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    authors: Mapped[Optional[str]] = mapped_column(String, default=None)
    year_: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    is_new: Mapped[Optional[bool]] = mapped_column(Boolean, default=True)


@table_registry.mapped_as_dataclass
class BibliographicProductionArticle:
    __tablename__ = 'bibliographic_production_article'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    bibliographic_production_id: Mapped[UUID] = mapped_column(
        ForeignKey('bibliographic_production.id')
    )
    periodical_magazine_id: Mapped[UUID] = mapped_column(
        ForeignKey('periodical_magazine.id')
    )
    volume: Mapped[Optional[str]] = mapped_column(String, default=None)
    fascicle: Mapped[Optional[str]] = mapped_column(String, default=None)
    series: Mapped[Optional[str]] = mapped_column(String, default=None)
    start_page: Mapped[Optional[str]] = mapped_column(String, default=None)
    end_page: Mapped[Optional[str]] = mapped_column(String, default=None)
    place_publication: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    periodical_magazine_name: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    issn: Mapped[Optional[str]] = mapped_column(String, default=None)
    qualis: Mapped[Optional[str]] = mapped_column(String, default='SQ')
    jcr: Mapped[Optional[str]] = mapped_column(String, default=None)
    jcr_link: Mapped[Optional[str]] = mapped_column(String, default=None)
    stars: Mapped[Optional[int]] = mapped_column(Integer, default=0)


@table_registry.mapped_as_dataclass
class BibliographicProductionBook:
    __tablename__ = 'bibliographic_production_book'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    bibliographic_production_id: Mapped[UUID] = mapped_column(
        ForeignKey('bibliographic_production.id')
    )
    isbn: Mapped[Optional[str]] = mapped_column(String, default=None)
    qtt_volume: Mapped[Optional[str]] = mapped_column(String, default=None)
    qtt_pages: Mapped[Optional[str]] = mapped_column(String, default=None)
    num_edition_revision: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    num_series: Mapped[Optional[str]] = mapped_column(String, default=None)
    publishing_company: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    publishing_company_city: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    stars: Mapped[Optional[int]] = mapped_column(Integer, default=0)


@table_registry.mapped_as_dataclass
class BibliographicProductionBookChapter:
    __tablename__ = 'bibliographic_production_book_chapter'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    bibliographic_production_id: Mapped[UUID] = mapped_column(
        ForeignKey('bibliographic_production.id')
    )
    book_title: Mapped[Optional[str]] = mapped_column(String, default=None)
    isbn: Mapped[Optional[str]] = mapped_column(String, default=None)
    start_page: Mapped[Optional[str]] = mapped_column(String, default=None)
    end_page: Mapped[Optional[str]] = mapped_column(String, default=None)
    qtt_volume: Mapped[Optional[str]] = mapped_column(String, default=None)
    organizers: Mapped[Optional[str]] = mapped_column(String, default=None)
    num_edition_revision: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    num_series: Mapped[Optional[str]] = mapped_column(String, default=None)
    publishing_company: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    publishing_company_city: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    stars: Mapped[Optional[int]] = mapped_column(Integer, default=0)


@table_registry.mapped_as_dataclass
class Software:
    __tablename__ = 'software'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    title: Mapped[Optional[str]] = mapped_column(String, default=None)
    platform: Mapped[Optional[str]] = mapped_column(String, default=None)
    goal: Mapped[Optional[str]] = mapped_column(String, default=None)
    relevance: Mapped[bool] = mapped_column(
        Boolean, server_default=text('false'), default=False
    )
    has_image: Mapped[bool] = mapped_column(
        Boolean, server_default=text('false'), default=False
    )
    environment: Mapped[Optional[str]] = mapped_column(String, default=None)
    availability: Mapped[Optional[str]] = mapped_column(String, default=None)
    financing_institutionc: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    is_new: Mapped[Optional[bool]] = mapped_column(
        Boolean, server_default=text('true'), default=True
    )
    stars: Mapped[Optional[int]] = mapped_column(
        Integer, server_default=text('0'), default=0
    )
    code: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class Patent:
    __tablename__ = 'patent'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    title: Mapped[Optional[str]] = mapped_column(String, default=None)
    category: Mapped[Optional[str]] = mapped_column(String, default=None)
    relevance: Mapped[bool] = mapped_column(
        Boolean, server_default=text('false'), default=False
    )
    has_image: Mapped[bool] = mapped_column(
        Boolean, server_default=text('false'), default=False
    )
    development_year: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    details: Mapped[Optional[str]] = mapped_column(Text, default=None)
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    code: Mapped[Optional[str]] = mapped_column(
        String, unique=False, default=None
    )
    grant_date: Mapped[Optional[datetime]] = mapped_column(default=None)
    deposit_date: Mapped[Optional[str]] = mapped_column(String, default=None)
    is_new: Mapped[Optional[bool]] = mapped_column(
        Boolean, server_default=text('true'), default=True
    )
    stars: Mapped[Optional[int]] = mapped_column(
        Integer, server_default=text('0'), default=0
    )


@table_registry.mapped_as_dataclass
class ResearchReport:
    __tablename__ = 'research_report'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    title: Mapped[Optional[str]] = mapped_column(String, default=None)
    project_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    financing_institutionc: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    is_new: Mapped[Optional[bool]] = mapped_column(
        Boolean, server_default=text('true'), default=True
    )
    stars: Mapped[Optional[int]] = mapped_column(
        Integer, server_default=text('0'), default=0
    )


@table_registry.mapped_as_dataclass
class Brand:
    __tablename__ = 'brand'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    title: Mapped[Optional[str]] = mapped_column(String, default=None)
    relevance: Mapped[bool] = mapped_column(
        Boolean, server_default=text('false'), default=False
    )
    has_image: Mapped[bool] = mapped_column(
        Boolean, server_default=text('false'), default=False
    )
    goal: Mapped[Optional[str]] = mapped_column(String, default=None)
    nature: Mapped[Optional[str]] = mapped_column(String, default=None)
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    is_new: Mapped[Optional[bool]] = mapped_column(
        Boolean, server_default=text('true'), default=True
    )
    stars: Mapped[Optional[int]] = mapped_column(
        Integer, server_default=text('0'), default=0
    )


@table_registry.mapped_as_dataclass
class AdvisoryActivity:
    __tablename__ = 'advisory_activity'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(ForeignKey('researcher.id'))
    organ_name: Mapped[Optional[str]] = mapped_column(String)
    start_year: Mapped[Optional[str]] = mapped_column(String)
    sequence_id: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    organ_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    unit_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    unit_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    specification: Mapped[Optional[str]] = mapped_column(Text, default=None)
    is_current: Mapped[Optional[str]] = mapped_column(String, default=None)
    start_month: Mapped[Optional[str]] = mapped_column(String, default=None)
    end_month: Mapped[Optional[str]] = mapped_column(String, default=None)
    end_year: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class ArtisticProduction:
    __tablename__ = 'artistic_production'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(ForeignKey('researcher.id'))
    title: Mapped[str] = mapped_column(Text)
    type: Mapped[str] = mapped_column(Text)
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)


@table_registry.mapped_as_dataclass
class DidacticMaterial:
    __tablename__ = 'didactic_material'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(ForeignKey('researcher.id'))
    title: Mapped[str] = mapped_column(Text)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    nature: Mapped[Optional[str]] = mapped_column(String, default=None)
    description: Mapped[Optional[str]] = mapped_column(Text, default=None)
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)


@table_registry.mapped_as_dataclass
class EventOrganization:
    __tablename__ = 'event_organization'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    title: Mapped[Optional[str]] = mapped_column(String, default=None)
    promoter_institution: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    nature: Mapped[Optional[str]] = mapped_column(String, default=None)
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    local: Mapped[Optional[str]] = mapped_column(String, default=None)
    duration_in_weeks: Mapped[Optional[int]] = mapped_column(
        Integer, default=None
    )
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    is_new: Mapped[Optional[bool]] = mapped_column(
        Boolean, server_default=text('true'), default=True
    )


@table_registry.mapped_as_dataclass
class ResearchProject:
    __tablename__ = 'research_project'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(ForeignKey('researcher.id'))
    start_year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    end_year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    agency_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    agency_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    project_name: Mapped[Optional[str]] = mapped_column(Text, default=None)
    status: Mapped[Optional[str]] = mapped_column(String, default=None)
    nature: Mapped[Optional[str]] = mapped_column(String, default=None)
    number_undergraduates: Mapped[Optional[int]] = mapped_column(
        Integer, server_default=text('0'), default=0
    )
    number_specialists: Mapped[Optional[int]] = mapped_column(
        Integer, server_default=text('0'), default=0
    )
    number_academic_masters: Mapped[Optional[int]] = mapped_column(
        Integer, server_default=text('0'), default=0
    )
    number_phd: Mapped[Optional[int]] = mapped_column(
        Integer, server_default=text('0'), default=0
    )
    description: Mapped[Optional[str]] = mapped_column(Text, default=None)
    stars: Mapped[Optional[int]] = mapped_column(
        Integer, server_default=text('0'), default=0
    )


@table_registry.mapped_as_dataclass
class ResearchProjectComponents:
    __tablename__ = 'research_project_components'
    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    project_id: Mapped[UUID] = mapped_column(ForeignKey('research_project.id'))
    name: Mapped[Optional[str]] = mapped_column(String, default=None)
    lattes_id: Mapped[Optional[str]] = mapped_column(String, default=None)
    citations: Mapped[Optional[str]] = mapped_column(String, default=None)
    coordinator: Mapped[bool] = mapped_column(
        Boolean, server_default=text('false'), default=False
    )


@table_registry.mapped_as_dataclass
class ResearchProjectFoment:
    __tablename__ = 'research_project_foment'

    project_id: Mapped[UUID] = mapped_column(
        ForeignKey('research_project.id'), primary_key=True
    )
    agency_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    agency_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    nature: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class ResearchProjectProduction:
    __tablename__ = 'research_project_production'

    project_id: Mapped[UUID] = mapped_column(
        ForeignKey('research_project.id'), primary_key=True
    )
    title: Mapped[Optional[str]] = mapped_column(Text, default=None)
    type: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class TechnicalWorkProgram:
    __tablename__ = 'technical_work_program'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(ForeignKey('researcher.id'))
    title: Mapped[str] = mapped_column(Text)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    nature: Mapped[Optional[str]] = mapped_column(String, default=None)
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    theme: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class TechnicalWork:
    __tablename__ = 'technical_work'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(ForeignKey('researcher.id'))
    title: Mapped[str] = mapped_column(Text)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    nature: Mapped[Optional[str]] = mapped_column(String, default=None)
    funding_institution: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    duration: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)


@table_registry.mapped_as_dataclass
class TechnicalWorkPresentation:
    __tablename__ = 'technical_work_presentation'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(ForeignKey('researcher.id'))
    title: Mapped[str] = mapped_column(Text)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    nature: Mapped[Optional[str]] = mapped_column(String, default=None)
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    event_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    promoting_institution: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )


@table_registry.mapped_as_dataclass
class TechnologicalProduct:
    __tablename__ = 'technological_product'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(ForeignKey('researcher.id'))
    title: Mapped[str] = mapped_column(Text)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    nature: Mapped[Optional[str]] = mapped_column(String, default=None)
    type: Mapped[Optional[str]] = mapped_column(String, default=None)
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)


@table_registry.mapped_as_dataclass
class BibliographicProductionWorkInEvent:
    __tablename__ = 'bibliographic_production_work_in_event'

    bibliographic_production_id: Mapped[UUID] = mapped_column(
        ForeignKey('bibliographic_production.id'), primary_key=True
    )
    event_classification: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    event_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    event_city: Mapped[Optional[str]] = mapped_column(String, default=None)
    event_year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    proceedings_title: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    volume: Mapped[Optional[str]] = mapped_column(String, default=None)
    issue: Mapped[Optional[str]] = mapped_column(String, default=None)
    series: Mapped[Optional[str]] = mapped_column(String, default=None)
    start_page: Mapped[Optional[str]] = mapped_column(String, default=None)
    end_page: Mapped[Optional[str]] = mapped_column(String, default=None)
    publisher_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    publisher_city: Mapped[Optional[str]] = mapped_column(String, default=None)
    event_name_english: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    identifier_number: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    isbn: Mapped[Optional[str]] = mapped_column(String, default=None)
    stars: Mapped[Optional[int]] = mapped_column(
        Integer, server_default=text('0'), default=0
    )


@table_registry.mapped_as_dataclass
class ProcessOrTechnique:
    __tablename__ = 'process_or_technique'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(ForeignKey('researcher.id'))
    title: Mapped[str] = mapped_column(Text)
    sequence_id: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    nature: Mapped[Optional[str]] = mapped_column(String, default=None)
    title_en: Mapped[Optional[str]] = mapped_column(Text, default=None)
    year: Mapped[Optional[str]] = mapped_column(String, default=None)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    language: Mapped[Optional[str]] = mapped_column(String, default=None)
    dissemination_medium: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    home_page: Mapped[Optional[str]] = mapped_column(Text, default=None)
    doi: Mapped[Optional[str]] = mapped_column(String, default=None)
    is_relevant: Mapped[bool] = mapped_column(
        Boolean, server_default=text('false'), default=False
    )
    has_innovation_potential: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    purpose: Mapped[Optional[str]] = mapped_column(Text, default=None)
    purpose_en: Mapped[Optional[str]] = mapped_column(Text, default=None)
    availability: Mapped[Optional[str]] = mapped_column(String, default=None)
    funding_institution: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    city: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class Mockup:
    __tablename__ = 'mockup'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    title: Mapped[str] = mapped_column(String)
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    production_sequence: Mapped[Optional[int]] = mapped_column(
        Integer, default=None
    )
    title_en: Mapped[Optional[str]] = mapped_column(String, default=None)
    year: Mapped[Optional[str]] = mapped_column(String, default=None)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    language: Mapped[Optional[str]] = mapped_column(String, default=None)
    dissemination_medium: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    homepage: Mapped[Optional[str]] = mapped_column(String, default=None)
    doi: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class Publishing:
    __tablename__ = 'publishing'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    title: Mapped[str] = mapped_column(String)
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    production_sequence: Mapped[Optional[int]] = mapped_column(
        Integer, default=None
    )
    title_en: Mapped[Optional[str]] = mapped_column(String, default=None)
    year: Mapped[Optional[str]] = mapped_column(String, default=None)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    language: Mapped[Optional[str]] = mapped_column(String, default=None)
    dissemination_medium: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    homepage: Mapped[Optional[str]] = mapped_column(String, default=None)
    doi: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class IndustrialDesign:
    __tablename__ = 'industrial_design'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    title: Mapped[str] = mapped_column(String)
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    production_sequence: Mapped[Optional[int]] = mapped_column(
        Integer, default=None
    )
    title_en: Mapped[Optional[str]] = mapped_column(String, default=None)
    year: Mapped[Optional[str]] = mapped_column(String, default=None)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    language: Mapped[Optional[str]] = mapped_column(String, default=None)
    dissemination_medium: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    homepage: Mapped[Optional[str]] = mapped_column(String, default=None)
    doi: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class MaintenanceArtisticWork:
    __tablename__ = 'maintenance_artistic_work'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    title: Mapped[str] = mapped_column(String)
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    production_sequence: Mapped[Optional[int]] = mapped_column(
        Integer, default=None
    )
    title_en: Mapped[Optional[str]] = mapped_column(String, default=None)
    year: Mapped[Optional[str]] = mapped_column(String, default=None)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    language: Mapped[Optional[str]] = mapped_column(String, default=None)
    dissemination_medium: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    homepage: Mapped[Optional[str]] = mapped_column(String, default=None)
    doi: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class LetterMapOrSimilar:
    __tablename__ = 'letter_map_or_similar'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    title: Mapped[str] = mapped_column(String)
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    production_sequence: Mapped[Optional[int]] = mapped_column(
        Integer, default=None
    )
    title_en: Mapped[Optional[str]] = mapped_column(String, default=None)
    year: Mapped[Optional[str]] = mapped_column(String, default=None)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    language: Mapped[Optional[str]] = mapped_column(String, default=None)
    dissemination_medium: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    homepage: Mapped[Optional[str]] = mapped_column(String, default=None)
    doi: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class ShortCourseTaught:
    __tablename__ = 'short_course_taught'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    title: Mapped[str] = mapped_column(String)
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    production_sequence: Mapped[Optional[int]] = mapped_column(
        Integer, default=None
    )
    title_en: Mapped[Optional[str]] = mapped_column(String, default=None)
    year: Mapped[Optional[str]] = mapped_column(String, default=None)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    language: Mapped[Optional[str]] = mapped_column(String, default=None)
    dissemination_medium: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    homepage: Mapped[Optional[str]] = mapped_column(String, default=None)
    doi: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class RadioOrTvProgram:
    __tablename__ = 'radio_or_tv_program'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    title: Mapped[str] = mapped_column(String)
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    production_sequence: Mapped[Optional[int]] = mapped_column(
        Integer, default=None
    )
    title_en: Mapped[Optional[str]] = mapped_column(String, default=None)
    year: Mapped[Optional[str]] = mapped_column(String, default=None)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    language: Mapped[Optional[str]] = mapped_column(String, default=None)
    dissemination_medium: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    homepage: Mapped[Optional[str]] = mapped_column(String, default=None)
    doi: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class ShortCourse:
    __tablename__ = 'short_course'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    title: Mapped[str] = mapped_column(String)
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    production_sequence: Mapped[Optional[int]] = mapped_column(
        Integer, default=None
    )
    title_en: Mapped[Optional[str]] = mapped_column(String, default=None)
    year: Mapped[Optional[str]] = mapped_column(String, default=None)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    language: Mapped[Optional[str]] = mapped_column(String, default=None)
    dissemination_medium: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    homepage: Mapped[Optional[str]] = mapped_column(String, default=None)
    doi: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class SocialMediaWebsiteBlog:
    __tablename__ = 'social_media_website_blog'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    title: Mapped[str] = mapped_column(String)
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    production_sequence: Mapped[Optional[int]] = mapped_column(
        Integer, default=None
    )
    title_en: Mapped[Optional[str]] = mapped_column(String, default=None)
    year: Mapped[Optional[str]] = mapped_column(String, default=None)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    language: Mapped[Optional[str]] = mapped_column(String, default=None)
    dissemination_medium: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    homepage: Mapped[Optional[str]] = mapped_column(String, default=None)
    doi: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class OtherTechnicalProduction:
    __tablename__ = 'other_technical_production'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    title: Mapped[str] = mapped_column(String)
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    production_sequence: Mapped[Optional[int]] = mapped_column(
        Integer, default=None
    )
    title_en: Mapped[Optional[str]] = mapped_column(String, default=None)
    year: Mapped[Optional[str]] = mapped_column(String, default=None)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    language: Mapped[Optional[str]] = mapped_column(String, default=None)
    dissemination_medium: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    homepage: Mapped[Optional[str]] = mapped_column(String, default=None)
    doi: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class Guidance:
    __tablename__ = 'guidance'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    title: Mapped[Optional[str]] = mapped_column(String, default=None)
    nature: Mapped[Optional[str]] = mapped_column(String, default=None)
    oriented: Mapped[Optional[str]] = mapped_column(String, default=None)
    type: Mapped[Optional[str]] = mapped_column(String, default=None)
    status: Mapped[Optional[str]] = mapped_column(String, default=None)
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    is_new: Mapped[Optional[bool]] = mapped_column(
        Boolean, server_default=text('true'), default=True
    )
    stars: Mapped[Optional[int]] = mapped_column(
        Integer, server_default=text('0'), default=0
    )


@table_registry.mapped_as_dataclass
class ParticipationEvents:
    __tablename__ = 'participation_events'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    title: Mapped[Optional[str]] = mapped_column(String, default=None)
    event_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    nature: Mapped[Optional[str]] = mapped_column(String, default=None)
    form_participation: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    type_participation: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    is_new: Mapped[Optional[bool]] = mapped_column(
        Boolean, server_default=text('true'), default=True
    )
    stars: Mapped[Optional[int]] = mapped_column(
        Integer, server_default=text('0'), default=0
    )


@table_registry.mapped_as_dataclass
class Foment:
    __tablename__ = 'foment'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    modality_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    modality_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    call_title: Mapped[Optional[str]] = mapped_column(String, default=None)
    category_level_code: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    funding_program_name: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    institute_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    aid_quantity: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    scholarship_quantity: Mapped[Optional[int]] = mapped_column(
        Integer, default=None
    )


@table_registry.mapped_as_dataclass
class OpenAlexResearcher:
    __tablename__ = 'openalex_researcher'

    __table_args__ = (
        UniqueConstraint(
            'researcher_id', name='uq_openalex_researcher_researcher_id'
        ),
        UniqueConstraint('orcid', name='uq_openalex_researcher_orcid'),
        Index('idx_openalex_researcher_orcid', 'orcid'),
        Index('idx_openalex_researcher_openalex', 'openalex'),
    )

    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('researcher.id', ondelete='CASCADE'), primary_key=True
    )

    h_index: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, default=None
    )

    relevance_score: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True, default=0
    )

    works_count: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, default=None
    )

    cited_by_count: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, default=None
    )

    i10_index: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, default=None
    )

    scopus: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, default=None
    )

    orcid: Mapped[Optional[str]] = mapped_column(
        String(19), nullable=True, default=None
    )

    openalex: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, default=None
    )


@table_registry.mapped_as_dataclass
class OpenAlexArticle:
    __tablename__ = 'openalex_article'

    __table_args__ = (
        UniqueConstraint('article_id', name='uq_openalex_article_article_id'),
        Index('idx_openalex_article_issn', 'issn'),
        Index('idx_openalex_article_language', 'language'),
    )

    article_id: Mapped[UUID] = mapped_column(
        ForeignKey('bibliographic_production.id', ondelete='CASCADE'),
        nullable=False,
        unique=True,
    )

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        server_default=text('gen_random_uuid()'),
        primary_key=True,
    )

    article_institution: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default=None
    )

    issn: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, default=None
    )

    authors_institution: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default=None
    )

    abstract: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default=None
    )

    authors: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default=None
    )

    language: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True, default=None
    )

    citations_count: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, default=0
    )

    pdf: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default=None
    )

    landing_page_url: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default=None
    )

    keywords: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True, default=None
    )


@table_registry.mapped_as_dataclass
class ResearcherProfessionalExperience:
    __tablename__ = 'researcher_professional_experience'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(ForeignKey('researcher.id'))
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
class RegisteredCultivar:
    __tablename__ = 'registered_cultivar'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(ForeignKey('researcher.id'))
    denomination: Mapped[Optional[str]] = mapped_column(String, default=None)
    denomination_en: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    country: Mapped[Optional[str]] = mapped_column(String, default=None)
    code: Mapped[Optional[str]] = mapped_column(
        String, unique=True, default=None
    )


@table_registry.mapped_as_dataclass
class ResearcherProduction:
    __tablename__ = 'researcher_production'

    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('researcher.id'), primary_key=True
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
class ResearchDictionary:
    __tablename__ = 'research_dictionary'

    research_dictionary_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    term: Mapped[Optional[str]] = mapped_column(String, default=None)
    frequency: Mapped[Optional[int]] = mapped_column(Integer, default=1)
    type_: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class GraduateProgram:
    __tablename__ = 'graduate_program'

    graduate_program_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String)
    area: Mapped[str] = mapped_column(String)
    modality: Mapped[str] = mapped_column(String)
    institution_id: Mapped[UUID] = mapped_column(ForeignKey('institution.id'))
    code: Mapped[Optional[str]] = mapped_column(String, default=None)
    name_en: Mapped[Optional[str]] = mapped_column(String, default=None)
    basic_area: Mapped[Optional[str]] = mapped_column(String, default=None)
    cooperation_project: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )

    type: Mapped[Optional[str]] = mapped_column(String, default=None)
    rating: Mapped[Optional[str]] = mapped_column(String, default=None)

    state: Mapped[Optional[str]] = mapped_column(String, default='BA')
    city: Mapped[Optional[str]] = mapped_column(String, default='Salvador')
    region: Mapped[Optional[str]] = mapped_column(String, default='Nordeste')
    url_image: Mapped[Optional[str]] = mapped_column(String, default=None)
    acronym: Mapped[Optional[str]] = mapped_column(String, default=None)
    description: Mapped[Optional[str]] = mapped_column(Text, default=None)
    visible: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    site: Mapped[Optional[str]] = mapped_column(Text, default=None)
    coordinator: Mapped[Optional[str]] = mapped_column(String, default=None)
    email: Mapped[Optional[str]] = mapped_column(String, default=None)
    start: Mapped[Optional[date]] = mapped_column(Date, default=None)
    phone: Mapped[Optional[str]] = mapped_column(String, default=None)
    periodicity: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class GraduateProgramResearcher:
    __tablename__ = 'graduate_program_researcher'

    graduate_program_id: Mapped[UUID] = mapped_column(
        ForeignKey(
            'graduate_program.graduate_program_id',
            onupdate='CASCADE',
            ondelete='CASCADE',
        ),
        primary_key=True,
    )
    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('researcher.id', onupdate='CASCADE', ondelete='CASCADE'),
        primary_key=True,
    )
    type_: Mapped[Optional[str]] = mapped_column(String, default=None)
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    tag: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class GraduateProgramStudent:
    __tablename__ = 'graduate_program_student'

    graduate_program_id: Mapped[UUID] = mapped_column(
        ForeignKey('graduate_program.graduate_program_id'), primary_key=True
    )
    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('researcher.id'), primary_key=True
    )
    year: Mapped[list[int]] = mapped_column(ARRAY(Integer), primary_key=True)


@table_registry.mapped_as_dataclass
class ResearchLinesPrograms:
    __tablename__ = 'research_lines_programs'

    graduate_program_id: Mapped[UUID] = mapped_column(
        ForeignKey('graduate_program.graduate_program_id'), primary_key=True
    )
    name: Mapped[str] = mapped_column(Text, primary_key=True)
    area: Mapped[str] = mapped_column(String)
    start_year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    end_year: Mapped[Optional[int]] = mapped_column(Integer, default=None)


@table_registry.mapped_as_dataclass
class ResearcherIndProd:
    __tablename__ = 'researcher_ind_prod'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('researcher.id'), primary_key=True
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
class GraduateProgramIndProd:
    __tablename__ = 'graduate_program_ind_prod'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    graduate_program_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True))
    year: Mapped[int] = mapped_column(Integer)
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
class ResearchGroupResearcher:
    __tablename__ = 'research_group_researcher'

    research_group_id: Mapped[UUID] = mapped_column(
        ForeignKey('research_group.id'), primary_key=True
    )
    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('researcher.id'), primary_key=True
    )


@table_registry.mapped_as_dataclass
class ProcessAuthor:
    __tablename__ = 'process_author'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    process_id: Mapped[UUID] = mapped_column(
        ForeignKey('process_or_technique.id')
    )
    full_name: Mapped[str] = mapped_column(String)
    citation_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    author_order: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    cnpq_id: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class ProcessKeyword:
    __tablename__ = 'process_keyword'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    process_id: Mapped[UUID] = mapped_column(
        ForeignKey('process_or_technique.id')
    )
    keyword: Mapped[str] = mapped_column(Text)
    order: Mapped[Optional[int]] = mapped_column(Integer, default=None)


@table_registry.mapped_as_dataclass
class ProcessKnowledgeArea:
    __tablename__ = 'process_knowledge_area'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    process_id: Mapped[UUID] = mapped_column(
        ForeignKey('process_or_technique.id')
    )
    major_area: Mapped[Optional[str]] = mapped_column(String, default=None)
    area_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    sub_area_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    specialty_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    order: Mapped[Optional[int]] = mapped_column(Integer, default=None)


@table_registry.mapped_as_dataclass
class RelevantProduction:
    __tablename__ = 'relevant_production'

    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('researcher.id'), primary_key=True
    )
    production_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True
    )
    type: Mapped[str] = mapped_column(String, primary_key=True)
    has_image: Mapped[bool] = mapped_column(Boolean, default=False)


@table_registry.mapped_as_dataclass
class Labs:
    __tablename__ = 'labs'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    hashed_id: Mapped[str] = mapped_column(String)
    type: Mapped[str] = mapped_column(String)
    name: Mapped[str] = mapped_column(Text)
    location: Mapped[Optional[str]] = mapped_column(Text, default=None)
    description: Mapped[Optional[str]] = mapped_column(Text, default=None)
    website: Mapped[Optional[str]] = mapped_column(Text, default=None)
    activities: Mapped[Optional[str]] = mapped_column(Text, default=None)
    areas: Mapped[Optional[str]] = mapped_column(Text, default=None)
    campus: Mapped[Optional[str]] = mapped_column(Text, default=None)
    institution_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('institution.id'), default=None
    )
    researcher_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('researcher.id'), default=None
    )
    responsible: Mapped[Optional[str]] = mapped_column(Text, default=None)


@table_registry.mapped_as_dataclass
class Sdg:
    __tablename__ = 'sdg'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    number: Mapped[int] = mapped_column(Integer)
    name: Mapped[str] = mapped_column(String)


@table_registry.mapped_as_dataclass
class SdgAlignment:
    __tablename__ = 'sdg_alignment'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    reference_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True))
    type: Mapped[str] = mapped_column(String)
    sdg_id: Mapped[UUID] = mapped_column(ForeignKey('sdg.id'))


@table_registry.mapped_as_dataclass
class Routine:
    __tablename__ = 'routine'
    __table_args__ = {'schema': 'logs'}

    type: Mapped[str] = mapped_column(String, primary_key=True)
    error: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    detail: Mapped[Optional[str]] = mapped_column(Text, default=None)


@table_registry.mapped_as_dataclass
class UfmgDepartament:
    __tablename__ = 'departament'
    __table_args__ = {'schema': 'ufmg'}

    dep_id: Mapped[str] = mapped_column(String, primary_key=True)
    org_cod: Mapped[Optional[str]] = mapped_column(String, default=None)
    dep_nom: Mapped[Optional[str]] = mapped_column(String, default=None)
    dep_des: Mapped[Optional[str]] = mapped_column(Text, default=None)
    dep_email: Mapped[Optional[str]] = mapped_column(String, default=None)
    dep_site: Mapped[Optional[str]] = mapped_column(Text, default=None)
    dep_sigla: Mapped[Optional[str]] = mapped_column(String, default=None)
    dep_tel: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class UfmgResearcher:
    __tablename__ = 'researcher'
    __table_args__ = {'schema': 'ufmg'}

    researcher_id: Mapped[UUID] = mapped_column(primary_key=True)
    full_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    gender: Mapped[Optional[str]] = mapped_column(String, default=None)
    status_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    work_regime: Mapped[Optional[str]] = mapped_column(String, default=None)
    job_class: Mapped[Optional[str]] = mapped_column(String, default=None)
    job_title: Mapped[Optional[str]] = mapped_column(String, default=None)
    job_rank: Mapped[Optional[str]] = mapped_column(String, default=None)
    job_reference_code: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    academic_degree: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    organization_entry_date: Mapped[Optional[date]] = mapped_column(
        Date, default=None
    )
    last_promotion_date: Mapped[Optional[date]] = mapped_column(
        Date, default=None
    )
    employment_status_description: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    department_name: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    career_category: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    academic_unit: Mapped[Optional[str]] = mapped_column(String, default=None)
    unit_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    function_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    position_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    leadership_start_date: Mapped[Optional[date]] = mapped_column(
        Date, default=None
    )
    leadership_end_date: Mapped[Optional[date]] = mapped_column(
        Date, default=None
    )
    current_function_name: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    function_location: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    registration_number: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    ufmg_registration_number: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    semester_reference: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )


@table_registry.mapped_as_dataclass
class UfmgTechnician:
    __tablename__ = 'technician'
    __table_args__ = {'schema': 'ufmg'}

    technician_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    full_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    gender: Mapped[Optional[str]] = mapped_column(String, default=None)
    status_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    work_regime: Mapped[Optional[str]] = mapped_column(String, default=None)
    job_class: Mapped[Optional[str]] = mapped_column(String, default=None)
    job_title: Mapped[Optional[str]] = mapped_column(String, default=None)
    job_rank: Mapped[Optional[str]] = mapped_column(String, default=None)
    job_reference_code: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    academic_degree: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    organization_entry_date: Mapped[Optional[date]] = mapped_column(
        Date, default=None
    )
    last_promotion_date: Mapped[Optional[date]] = mapped_column(
        Date, default=None
    )
    employment_status_description: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    department_name: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    career_category: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    academic_unit: Mapped[Optional[str]] = mapped_column(String, default=None)
    unit_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    function_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    position_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    leadership_start_date: Mapped[Optional[date]] = mapped_column(
        Date, default=None
    )
    leadership_end_date: Mapped[Optional[date]] = mapped_column(
        Date, default=None
    )
    current_function_name: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    function_location: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    registration_number: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    ufmg_registration_number: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    semester_reference: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )


@table_registry.mapped_as_dataclass
class UfmgDepartamentTechnician:
    __tablename__ = 'departament_technician'
    __table_args__ = {'schema': 'ufmg'}

    dep_id: Mapped[str] = mapped_column(
        ForeignKey('ufmg.departament.dep_id'), primary_key=True
    )
    technician_id: Mapped[UUID] = mapped_column(
        ForeignKey('ufmg.technician.technician_id'), primary_key=True
    )


@table_registry.mapped_as_dataclass
class UfmgDepartamentResearcher:
    __tablename__ = 'departament_researcher'
    __table_args__ = {'schema': 'ufmg'}

    dep_id: Mapped[str] = mapped_column(primary_key=True)
    researcher_id: Mapped[UUID] = mapped_column(primary_key=True)


@table_registry.mapped_as_dataclass
class UfmgResearcherData:
    __tablename__ = 'researcher_data'
    __table_args__ = {'schema': 'ufmg'}

    cpf: Mapped[str] = mapped_column(String, primary_key=True)
    nome: Mapped[Optional[str]] = mapped_column(String, default=None)
    classe: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    nivel: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    inicio: Mapped[Optional[datetime]] = mapped_column(DateTime, default=None)
    fim: Mapped[Optional[datetime]] = mapped_column(DateTime, default=None)
    tempo_nivel: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    tempo_acumulado: Mapped[Optional[int]] = mapped_column(
        Integer, default=None
    )
    arquivo: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class UfmgMandate:
    __tablename__ = 'mandate'
    __table_args__ = {'schema': 'ufmg'}

    member: Mapped[str] = mapped_column(String, primary_key=True)
    departament: Mapped[str] = mapped_column(String, primary_key=True)
    mandate: Mapped[Optional[str]] = mapped_column(String, default=None)
    email: Mapped[Optional[str]] = mapped_column(String, default=None)
    phone: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class AdminInstitution:
    __tablename__ = 'institution'
    __table_args__ = {'schema': 'admin'}

    institution_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String)
    acronym: Mapped[Optional[str]] = mapped_column(
        String, unique=True, default=None
    )
    lattes_id: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class AdminResearcher:
    __tablename__ = 'researcher'
    __table_args__ = {'schema': 'admin'}

    researcher_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String)
    lattes_id: Mapped[Optional[str]] = mapped_column(
        String, unique=True, default=None
    )
    extra_field: Mapped[Optional[str]] = mapped_column(String, default=None)
    status: Mapped[bool] = mapped_column(Boolean, default=True)


@table_registry.mapped_as_dataclass
class AdminResearcherInstitution:
    __tablename__ = 'researcher_institution'
    __table_args__ = {'schema': 'admin'}

    researcher_institution_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('admin.researcher.researcher_id')
    )
    institution_id: Mapped[UUID] = mapped_column(
        ForeignKey('admin.institution.institution_id')
    )
    start_date: Mapped[Optional[date]] = mapped_column(Date, default=None)
    end_date: Mapped[Optional[date]] = mapped_column(Date, default=None)
    is_current: Mapped[Optional[bool]] = mapped_column(Boolean, default=True)


@table_registry.mapped_as_dataclass
class AdminGraduateProgram:
    __tablename__ = 'graduate_program'
    __table_args__ = {'schema': 'admin'}

    graduate_program_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String)
    area: Mapped[str] = mapped_column(String)
    modality: Mapped[str] = mapped_column(String)
    institution_id: Mapped[UUID] = mapped_column(
        ForeignKey('admin.institution.institution_id')
    )
    code: Mapped[Optional[str]] = mapped_column(
        String, unique=True, default=None
    )
    type: Mapped[Optional[str]] = mapped_column(String, default=None)
    rating: Mapped[Optional[str]] = mapped_column(String, default=None)
    state: Mapped[Optional[str]] = mapped_column(String, default='BA')
    city: Mapped[Optional[str]] = mapped_column(String, default='Salvador')
    region: Mapped[Optional[str]] = mapped_column(String, default='Nordeste')
    url_image: Mapped[Optional[str]] = mapped_column(String, default=None)
    acronym: Mapped[Optional[str]] = mapped_column(String, default=None)
    description: Mapped[Optional[str]] = mapped_column(Text, default=None)
    visible: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    site: Mapped[Optional[str]] = mapped_column(Text, default=None)
    menagers: Mapped[Optional[list[str]]] = mapped_column(
        ARRAY(String), default=None
    )


@table_registry.mapped_as_dataclass
class AdminGraduateProgramResearcher:
    __tablename__ = 'graduate_program_researcher'
    __table_args__ = {'schema': 'admin'}

    graduate_program_id: Mapped[UUID] = mapped_column(
        ForeignKey('admin.graduate_program.graduate_program_id'),
        primary_key=True,
    )
    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('admin.researcher.researcher_id'), primary_key=True
    )
    year: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    type_: Mapped[Optional[str]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class AdminGraduateProgramStudent:
    __tablename__ = 'graduate_program_student'
    __table_args__ = {'schema': 'admin'}

    graduate_program_id: Mapped[UUID] = mapped_column(
        ForeignKey('admin.graduate_program.graduate_program_id'),
        primary_key=True,
    )
    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('admin.researcher.researcher_id'), primary_key=True
    )
    year: Mapped[list[int]] = mapped_column(ARRAY(Integer), primary_key=True)


@table_registry.mapped_as_dataclass
class AdminWeights:
    __tablename__ = 'weights'
    __table_args__ = {'schema': 'admin'}

    institution_id: Mapped[UUID] = mapped_column(
        ForeignKey('admin.institution.institution_id'), primary_key=True
    )
    a1: Mapped[Optional[float]] = mapped_column(Float, default=None)
    a2: Mapped[Optional[float]] = mapped_column(Float, default=None)
    a3: Mapped[Optional[float]] = mapped_column(Float, default=None)
    a4: Mapped[Optional[float]] = mapped_column(Float, default=None)
    b1: Mapped[Optional[float]] = mapped_column(Float, default=None)
    b2: Mapped[Optional[float]] = mapped_column(Float, default=None)
    b3: Mapped[Optional[float]] = mapped_column(Float, default=None)
    b4: Mapped[Optional[float]] = mapped_column(Float, default=None)
    c: Mapped[Optional[float]] = mapped_column(Float, default=None)
    sq: Mapped[Optional[float]] = mapped_column(Float, default=None)
    book: Mapped[Optional[float]] = mapped_column(Float, default=None)
    book_chapter: Mapped[Optional[float]] = mapped_column(Float, default=None)
    software: Mapped[Optional[str]] = mapped_column(String, default=None)
    patent_granted: Mapped[Optional[str]] = mapped_column(String, default=None)
    patent_not_granted: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    report: Mapped[Optional[str]] = mapped_column(String, default=None)
    f1: Mapped[Optional[float]] = mapped_column(Float, default=0.0)
    f2: Mapped[Optional[float]] = mapped_column(Float, default=0.0)
    f3: Mapped[Optional[float]] = mapped_column(Float, default=0.0)
    f4: Mapped[Optional[float]] = mapped_column(Float, default=0.0)
    f5: Mapped[Optional[float]] = mapped_column(Float, default=0.0)


@table_registry.mapped_as_dataclass
class AdminRoles:
    __tablename__ = 'roles'
    __table_args__ = {'schema': 'admin'}

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    role: Mapped[str] = mapped_column(String, unique=True)


@table_registry.mapped_as_dataclass
class AdminPermission:
    __tablename__ = 'permission'
    __table_args__ = {'schema': 'admin'}

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    role_id: Mapped[UUID] = mapped_column(ForeignKey('admin.roles.id'))
    permission: Mapped[str] = mapped_column(String)


@table_registry.mapped_as_dataclass
class AdminUsers:
    __tablename__ = 'users'
    __table_args__ = {'schema': 'admin'}

    user_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    display_name: Mapped[str] = mapped_column(String)
    email: Mapped[str] = mapped_column(String, unique=True)
    uid: Mapped[str] = mapped_column(String, unique=True)
    photo_url: Mapped[Optional[str]] = mapped_column(Text, default=None)
    lattes_id: Mapped[Optional[str]] = mapped_column(String, default=None)
    institution_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('admin.institution.institution_id'), default=None
    )
    provider: Mapped[Optional[str]] = mapped_column(String, default=None)
    linkedin: Mapped[Optional[str]] = mapped_column(String, default=None)
    verify: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    shib_id: Mapped[Optional[str]] = mapped_column(String, default=None)
    shib_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    birth_date: Mapped[Optional[str]] = mapped_column(String, default=None)
    course_level: Mapped[Optional[str]] = mapped_column(String, default=None)
    first_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    registration: Mapped[Optional[str]] = mapped_column(String, default=None)
    gender: Mapped[Optional[str]] = mapped_column(String, default=None)
    last_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    email_status: Mapped[Optional[str]] = mapped_column(String, default=None)
    visible_email: Mapped[Optional[bool]] = mapped_column(
        Boolean, default=None
    )


@table_registry.mapped_as_dataclass
class AdminUsersRoles:
    __tablename__ = 'users_roles'
    __table_args__ = {'schema': 'admin'}

    role_id: Mapped[UUID] = mapped_column(
        ForeignKey('admin.roles.id'), primary_key=True
    )
    user_id: Mapped[UUID] = mapped_column(
        ForeignKey('admin.users.user_id'), primary_key=True
    )


@table_registry.mapped_as_dataclass
class AdminNewsletterSubscribers:
    __tablename__ = 'newsletter_subscribers'
    __table_args__ = {'schema': 'admin'}

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    email: Mapped[str] = mapped_column(String, unique=True)
    subscribed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, server_default=text('now()'), default=None
    )


@table_registry.mapped_as_dataclass
class AdminFeedback:
    __tablename__ = 'feedback'
    __table_args__ = {'schema': 'admin'}

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    name: Mapped[str] = mapped_column(String)
    email: Mapped[str] = mapped_column(String)
    rating: Mapped[int] = mapped_column(Integer)
    description: Mapped[Optional[str]] = mapped_column(Text, default=None)


@table_registry.mapped_as_dataclass
class AdminUfmgResearcher:
    __tablename__ = 'researcher'
    __table_args__ = {'schema': 'admin_ufmg'}

    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('admin.researcher.researcher_id'), primary_key=True
    )
    full_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    gender: Mapped[Optional[str]] = mapped_column(String, default=None)
    status_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    work_regime: Mapped[Optional[str]] = mapped_column(String, default=None)
    job_class: Mapped[Optional[str]] = mapped_column(String, default=None)
    job_title: Mapped[Optional[str]] = mapped_column(String, default=None)
    job_rank: Mapped[Optional[str]] = mapped_column(String, default=None)
    job_reference_code: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    academic_degree: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    organization_entry_date: Mapped[Optional[date]] = mapped_column(
        Date, default=None
    )
    last_promotion_date: Mapped[Optional[date]] = mapped_column(
        Date, default=None
    )
    employment_status_description: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    department_name: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    career_category: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    academic_unit: Mapped[Optional[str]] = mapped_column(String, default=None)
    unit_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    function_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    position_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    leadership_start_date: Mapped[Optional[date]] = mapped_column(
        Date, default=None
    )
    leadership_end_date: Mapped[Optional[date]] = mapped_column(
        Date, default=None
    )
    current_function_name: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    function_location: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    registration_number: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    ufmg_registration_number: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    semester_reference: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )


@table_registry.mapped_as_dataclass
class AdminUfmgTechnician:
    __tablename__ = 'technician'
    __table_args__ = {'schema': 'admin_ufmg'}

    technician_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    full_name: Mapped[Optional[str]] = mapped_column(String, default=None)
    gender: Mapped[Optional[str]] = mapped_column(String, default=None)
    status_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    work_regime: Mapped[Optional[str]] = mapped_column(String, default=None)
    job_class: Mapped[Optional[str]] = mapped_column(String, default=None)
    job_title: Mapped[Optional[str]] = mapped_column(String, default=None)
    job_rank: Mapped[Optional[str]] = mapped_column(String, default=None)
    job_reference_code: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    academic_degree: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    organization_entry_date: Mapped[Optional[date]] = mapped_column(
        Date, default=None
    )
    last_promotion_date: Mapped[Optional[date]] = mapped_column(
        Date, default=None
    )
    employment_status_description: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    department_name: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    career_category: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    academic_unit: Mapped[Optional[str]] = mapped_column(String, default=None)
    unit_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    function_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    position_code: Mapped[Optional[str]] = mapped_column(String, default=None)
    leadership_start_date: Mapped[Optional[date]] = mapped_column(
        Date, default=None
    )
    leadership_end_date: Mapped[Optional[date]] = mapped_column(
        Date, default=None
    )
    current_function_name: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    function_location: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    registration_number: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    ufmg_registration_number: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    semester_reference: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )


@table_registry.mapped_as_dataclass
class AdminUfmgDepartment:
    __tablename__ = 'department'
    __table_args__ = {'schema': 'admin_ufmg'}

    dep_id: Mapped[str] = mapped_column(String, primary_key=True)
    org_cod: Mapped[Optional[str]] = mapped_column(String, default=None)
    dep_nom: Mapped[Optional[str]] = mapped_column(String, default=None)
    dep_des: Mapped[Optional[str]] = mapped_column(Text, default=None)
    dep_email: Mapped[Optional[str]] = mapped_column(String, default=None)
    dep_site: Mapped[Optional[str]] = mapped_column(String, default=None)
    dep_sigla: Mapped[Optional[str]] = mapped_column(String, default=None)
    dep_tel: Mapped[Optional[str]] = mapped_column(String, default=None)
    img_data: Mapped[Optional[bytes]] = mapped_column(String, default=None)


@table_registry.mapped_as_dataclass
class AdminUfmgDepartmentTechnician:
    __tablename__ = 'department_technician'
    __table_args__ = {'schema': 'admin_ufmg'}

    dep_id: Mapped[str] = mapped_column(
        ForeignKey('admin_ufmg.department.dep_id'), primary_key=True
    )
    technician_id: Mapped[UUID] = mapped_column(
        ForeignKey('admin_ufmg.technician.technician_id'), primary_key=True
    )


@table_registry.mapped_as_dataclass
class AdminUfmgDepartmentResearcher:
    __tablename__ = 'department_researcher'
    __table_args__ = {'schema': 'admin_ufmg'}

    dep_id: Mapped[str] = mapped_column(
        ForeignKey('admin_ufmg.department.dep_id'), primary_key=True
    )
    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('admin.researcher.researcher_id'), primary_key=True
    )


@table_registry.mapped_as_dataclass
class AdminUfmgDisciplines:
    __tablename__ = 'disciplines'
    __table_args__ = {'schema': 'admin_ufmg'}

    discipline_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    dep_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey('admin_ufmg.department.dep_id'), default=None
    )
    semester: Mapped[Optional[str]] = mapped_column(String, default=None)
    department: Mapped[Optional[str]] = mapped_column(String, default=None)
    academic_activity_code: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    academic_activity_name: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    academic_activity_ch: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    demanding_courses: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    oft: Mapped[Optional[str]] = mapped_column(String, default=None)
    available_slots: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    occupied_slots: Mapped[Optional[str]] = mapped_column(String, default=None)
    percent_occupied_slots: Mapped[Optional[str]] = mapped_column(
        String, default=None
    )
    schedule: Mapped[Optional[str]] = mapped_column(String, default=None)
    language: Mapped[Optional[str]] = mapped_column(String, default=None)
    researcher_id: Mapped[Optional[list[UUID]]] = mapped_column(
        ARRAY(PG_UUID(as_uuid=True)), default=None
    )
    researcher_name: Mapped[Optional[list[str]]] = mapped_column(
        ARRAY(String), default=None
    )
    status: Mapped[Optional[str]] = mapped_column(String, default=None)
    workload: Mapped[Optional[list[str]]] = mapped_column(
        ARRAY(String), default=None
    )
