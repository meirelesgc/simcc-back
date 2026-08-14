from typing import Optional
from uuid import UUID

from sqlalchemy import (
    ForeignKey,
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
class ResearchGroup:
    __tablename__ = 'research_group'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    __table_args__ = (
        UniqueConstraint(
            'name', 'institution', name='uq_research_group_name_institution'
        ),
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
class ResearchGroupResearcher:
    __tablename__ = 'research_group_researcher'

    research_group_id: Mapped[UUID] = mapped_column(
        ForeignKey('research_group.id'), primary_key=True
    )
    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('researcher.id', ondelete='CASCADE'), primary_key=True
    )
