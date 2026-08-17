from datetime import datetime
from typing import Optional
from uuid import UUID

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    DateTime,
    ForeignKey,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from simcc.core.db.models.base import table_registry


@table_registry.mapped_as_dataclass
class SearchDocumentResearcher:
    __tablename__ = 'search_document_researcher'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('researcher.id', ondelete='CASCADE'),
        unique=True,
    )
    document_content: Mapped[str] = mapped_column(Text)
    embedding: Mapped[Optional[list[float]]] = mapped_column(
        Vector(1536), default=None
    )
    last_indexed_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=text('now()'), default_factory=datetime.now
    )


@table_registry.mapped_as_dataclass
class SearchDocumentProduction:
    __tablename__ = 'search_document_production'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    production_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        unique=True,
    )
    type: Mapped[str] = mapped_column(String)
    document_content: Mapped[str] = mapped_column(Text)
    embedding: Mapped[Optional[list[float]]] = mapped_column(
        Vector(1536), default=None
    )
    last_indexed_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=text('now()'), default_factory=datetime.now
    )
