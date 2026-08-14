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
    Integer,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from simcc.core.db.models.base import table_registry


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
        ForeignKey('admin.researcher.researcher_id', ondelete='CASCADE')
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
        ForeignKey('admin.researcher.researcher_id', ondelete='CASCADE'),
        primary_key=True,
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
        ForeignKey('admin.researcher.researcher_id', ondelete='CASCADE'),
        primary_key=True,
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
    display_name: Mapped[Optional[str]] = mapped_column(String)
    email: Mapped[str] = mapped_column(String, unique=True)
    uid: Mapped[Optional[str]] = mapped_column(String, unique=True)
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
    orcid_id: Mapped[Optional[str]] = mapped_column(String, default=None)
    username: Mapped[Optional[str]] = mapped_column(String, default=None)
    icon_url: Mapped[Optional[str]] = mapped_column(Text, default=None)
    cover_url: Mapped[Optional[str]] = mapped_column(Text, default=None)


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
        ForeignKey('admin.researcher.researcher_id', ondelete='CASCADE'),
        primary_key=True,
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
        ForeignKey('admin.researcher.researcher_id', ondelete='CASCADE'),
        primary_key=True,
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
