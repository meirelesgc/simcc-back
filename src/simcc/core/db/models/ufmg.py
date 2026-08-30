from datetime import date, datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import (
    Date,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, registry

legacy_ufmg_registry = registry()


@legacy_ufmg_registry.mapped_as_dataclass
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


@legacy_ufmg_registry.mapped_as_dataclass
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


@legacy_ufmg_registry.mapped_as_dataclass
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


@legacy_ufmg_registry.mapped_as_dataclass
class UfmgDepartamentTechnician:
    __tablename__ = 'departament_technician'
    __table_args__ = {'schema': 'ufmg'}

    dep_id: Mapped[str] = mapped_column(
        ForeignKey('ufmg.departament.dep_id'), primary_key=True
    )
    technician_id: Mapped[UUID] = mapped_column(
        ForeignKey('ufmg.technician.technician_id'), primary_key=True
    )


@legacy_ufmg_registry.mapped_as_dataclass
class UfmgDepartamentResearcher:
    __tablename__ = 'departament_researcher'
    __table_args__ = {'schema': 'ufmg'}

    dep_id: Mapped[str] = mapped_column(
        ForeignKey('ufmg.departament.dep_id', ondelete='CASCADE'),
        primary_key=True,
    )
    researcher_id: Mapped[UUID] = mapped_column(
        ForeignKey('ufmg.researcher.researcher_id', ondelete='CASCADE'),
        primary_key=True,
    )


@legacy_ufmg_registry.mapped_as_dataclass
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


@legacy_ufmg_registry.mapped_as_dataclass
class UfmgMandate:
    __tablename__ = 'mandate'
    __table_args__ = {'schema': 'ufmg'}

    member: Mapped[str] = mapped_column(String, primary_key=True)
    departament: Mapped[str] = mapped_column(String, primary_key=True)
    mandate: Mapped[Optional[str]] = mapped_column(String, default=None)
    email: Mapped[Optional[str]] = mapped_column(String, default=None)
    phone: Mapped[Optional[str]] = mapped_column(String, default=None)
