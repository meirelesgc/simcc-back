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
    name: Mapped[str] = mapped_column(String, unique=True)
    country_id: Mapped[UUID] = mapped_column(ForeignKey('country.id'))
    state_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey('state.id'), default=None
    )
