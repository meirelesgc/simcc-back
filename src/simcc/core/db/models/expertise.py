from typing import Optional
from uuid import UUID

from sqlalchemy import (
    ForeignKey,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from simcc.core.db.models.base import table_registry


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
