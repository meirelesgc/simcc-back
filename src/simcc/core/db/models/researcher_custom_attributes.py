from typing import Any, Optional
from uuid import UUID

from sqlalchemy import ForeignKey, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from simcc.core.db.models.base import table_registry


@table_registry.mapped_as_dataclass
class ResearcherCustomAttributes:
    __tablename__ = 'researcher_custom_attributes'

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        server_default=text('gen_random_uuid()'),
        init=False,
    )
    researcher_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey('researcher.id', ondelete='CASCADE'),
        unique=True,
        index=True,
    )
    gender: Mapped[Optional[str]] = mapped_column(
        String, nullable=True, default=None
    )
    zip_code: Mapped[Optional[str]] = mapped_column(
        String, nullable=True, default=None
    )
    work_regime: Mapped[Optional[str]] = mapped_column(
        String, nullable=True, default=None
    )
    custom_attributes: Mapped[Optional[dict[str, Any]]] = mapped_column(
        JSONB, nullable=True, default_factory=dict
    )
