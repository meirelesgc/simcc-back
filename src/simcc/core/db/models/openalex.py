from typing import Optional
from uuid import UUID

from sqlalchemy import (
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
from sqlalchemy.orm import Mapped, mapped_column

from simcc.core.db.models.base import table_registry


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
