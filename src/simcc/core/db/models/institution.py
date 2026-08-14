from typing import Optional
from uuid import UUID

from sqlalchemy import (
    Float,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from simcc.core.db.models.base import table_registry


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
