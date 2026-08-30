from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, registry
from sqlalchemy.sql import func

from simcc.core.db.models.base import table_registry

legacy_logs_registry = registry()


@legacy_logs_registry.mapped_as_dataclass
class Routine:
    __tablename__ = 'routine'
    __table_args__ = (
        UniqueConstraint('type', name='uk_routine_type'),
        {'schema': 'logs'},
    )
    type: Mapped[str] = mapped_column(String, primary_key=True)
    error: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    detail: Mapped[Optional[str]] = mapped_column(Text, default=None)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), default=None
    )


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
    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('researcher.id', ondelete='CASCADE')
    )
    location: Mapped[Optional[str]] = mapped_column(Text, default=None)
    description: Mapped[Optional[str]] = mapped_column(Text, default=None)
    website: Mapped[Optional[str]] = mapped_column(Text, default=None)
    activities: Mapped[Optional[str]] = mapped_column(Text, default=None)
    areas: Mapped[Optional[str]] = mapped_column(Text, default=None)
    campus: Mapped[Optional[str]] = mapped_column(Text, default=None)
    institution_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('institution.id'), default=None
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
