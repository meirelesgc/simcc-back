from sqlalchemy import text

from simcc.queries import institution_query, researcher_query
from simcc.queries.institution_query import InstitutionFrequencyQuery
from simcc.queries.metrics_query import (
    AcademicDegreeMetricsQuery,
    BrandMetricsQuery,
    EducationMetricsQuery,
    GreatAreaMetricsQuery,
    GuidanceMetricsQuery,
    LattesUpdateMetricsQuery,
    MagazineMetricsQuery,
    PatentMetricsQuery,
    ResearcherMetricsQuery,
    ResearchProjectMetricsQuery,
    ResearchReportMetricsQuery,
    ScholarshipMetricsQuery,
    SoftwareMetricsQuery,
    SpeakerMetricsQuery,
    YearlyProductionMetricsQuery,
)
from simcc.queries.term_query import OriginalWordsQuery
from simcc.schemas import DefaultFilters


async def search_researchers(
    session, filters, search_type=None, name=None, pagination=None
):
    query = researcher_query.ResearcherSearchQuery(
        session, search_type=search_type, name=name
    )
    query.apply_filters(filters)
    query.apply_pagination(pagination)
    return await query.execute()


async def get_metrics_academic_degree(session, filters):
    query = AcademicDegreeMetricsQuery(session)
    query.apply_filters(filters)
    return await query.execute()


async def get_metrics_great_area(session, filters):
    query = GreatAreaMetricsQuery(session)
    query.apply_filters(filters)
    return await query.execute()


async def get_metrics_yearly_production(session, filters, production_type):
    query = YearlyProductionMetricsQuery(session, production_type)
    query.apply_filters(filters)
    return await query.execute()


async def get_metrics_researcher(session, filters):
    query = ResearcherMetricsQuery(session)
    query.apply_filters(filters)
    return await query.execute()


async def get_metrics_patents(session, filters):
    query = PatentMetricsQuery(session)
    query.apply_filters(filters)
    return await query.execute()


async def get_metrics_guidance(session, filters):
    query = GuidanceMetricsQuery(session)
    query.apply_filters(filters)
    return await query.execute()


async def get_metrics_speaker(session, filters):
    query = SpeakerMetricsQuery(session)
    query.apply_filters(filters)
    return await query.execute()


async def get_metrics_education(session, filters):
    query = EducationMetricsQuery(session)
    query.apply_filters(filters)
    return await query.execute()


async def get_metrics_software(session, filters):
    query = SoftwareMetricsQuery(session)
    query.apply_filters(filters)
    return await query.execute()


async def get_metrics_research_report(session, filters):
    query = ResearchReportMetricsQuery(session)
    query.apply_filters(filters)
    return await query.execute()


async def get_metrics_brand(session, filters, nature=None):
    query = BrandMetricsQuery(session, nature=nature)
    query.apply_filters(filters)
    return await query.execute()


async def get_metrics_research_project(session, filters):
    query = ResearchProjectMetricsQuery(session)
    query.apply_filters(filters)
    return await query.execute()


async def get_metrics_lattes_update(session, filters):
    query = LattesUpdateMetricsQuery(session)
    query.apply_filters(filters)
    return await query.execute()


async def get_metrics_scholarship(session, filters):
    query = ScholarshipMetricsQuery(session)
    query.apply_filters(filters)
    return await query.execute()


async def get_metrics_magazine(session, issn=None, initials=None):
    query = MagazineMetricsQuery(session)
    if issn:
        query._apply_issn_filter(issn)
    if initials:
        query._apply_initials_filter(initials)
    return await query.execute()


async def list_labs(session, lattes_id=None, researcher_id=None):
    params = {}
    filters = []
    join_researcher = ''

    if lattes_id:
        params['lattes_id'] = lattes_id
        join_researcher = 'LEFT JOIN researcher r ON r.id = l.researcher_id'
        filters.append(' AND r.lattes_id = :lattes_id')

    if researcher_id:
        params['researcher_id'] = str(researcher_id)
        filters.append(' AND l.researcher_id = :researcher_id')

    SCRIPT_SQL = f"""
        SELECT l.id, l.hashed_id, l.type, l.location, l.name, l.description, l.website,
            l.activities, l.areas, l.campus, l.institution_id, l.researcher_id, l.responsible
        FROM labs l
            {join_researcher}
        WHERE 1 = 1
            {''.join(filters)}
    """

    result = await session.execute(text(SCRIPT_SQL), params)
    return result.mappings().all()


async def list_institutions(session):
    query = institution_query.InstitutionQuery(session)
    return await query.execute()


async def get_institution(session, institution_id):
    query = institution_query.InstitutionQuery(session, institution_id)
    result = await query.execute()
    return result[0] if result else None


async def list_researcher_terms(session, filters):
    query = researcher_query.ResearcherTermQuery(session)
    query.apply_filters(filters)
    return await query.execute()


async def list_co_authorship(session, researcher_id):
    query = researcher_query.CoAuthorshipQuery(session, researcher_id)
    return await query.execute()


async def list_original_words(session, initials, type_):
    query = OriginalWordsQuery(session, initials, type_)
    return await query.execute()


async def list_institution_frequency(session, terms, institution, type_):
    query = InstitutionFrequencyQuery(session, terms, institution, type_)
    return await query.execute()


async def get_departament_rt(session):
    res_query = institution_query.RtMetricsQuery(session, 'researcher')
    teachers = await res_query.execute()

    tech_query = institution_query.RtMetricsQuery(session, 'technician')
    technicians = await tech_query.execute()

    return {'teachers': teachers, 'technician': technicians}


async def get_researcher_id(session, lattes_id=None, name=None):
    if lattes_id:
        sql = 'SELECT id FROM researcher WHERE lattes_id = :lattes_id LIMIT 1'
        result = await session.execute(text(sql), {'lattes_id': lattes_id})
    elif name:
        sql = 'SELECT id FROM researcher WHERE name = :name LIMIT 1'
        result = await session.execute(text(sql), {'name': name})
    else:
        return None

    row = result.scalar()
    return row


async def get_researcher(session, researcher_id):
    filters = DefaultFilters(researcher_id=researcher_id)
    result = await search_researchers(session, filters)
    return result[0] if result else None


async def list_graduate_programs_by_ids(session, researcher_ids: list):
    if not researcher_ids:
        return []

    SCRIPT_SQL = """
        SELECT
            gpr.researcher_id AS id,
            JSONB_AGG(DISTINCT JSONB_BUILD_OBJECT(
                'graduate_program_id', gp.graduate_program_id,
                'code', gp.code,
                'name', gp.name,
                'name_en', gp.name_en,
                'basic_area', gp.basic_area,
                'cooperation_project', gp.cooperation_project,
                'area', gp.area,
                'modality', gp.modality,
                'type', gp.type,
                'rating', gp.rating,
                'institution_id', gp.institution_id,
                'state', gp.state,
                'city', gp.city,
                'region', gp.region,
                'url_image', gp.url_image,
                'acronym', gp.acronym,
                'description', gp.description,
                'visible', gp.visible,
                'site', gp.site,
                'coordinator', gp.coordinator,
                'email', gp.email,
                'start', gp.start,
                'phone', gp.phone,
                'periodicity', gp.periodicity
            )) AS graduate_programs
        FROM graduate_program_researcher gpr
        LEFT JOIN graduate_program gp
            ON gpr.graduate_program_id = gp.graduate_program_id
        WHERE gpr.researcher_id = ANY(:researcher_ids)
        GROUP BY gpr.researcher_id;
    """
    result = await session.execute(
        text(SCRIPT_SQL),
        {'researcher_ids': [str(rid) for rid in researcher_ids]},
    )
    return result.mappings().all()


async def list_research_groups_by_ids(session, researcher_ids: list):
    if not researcher_ids:
        return []

    SCRIPT_SQL = """
        SELECT r.id AS id,
            JSONB_AGG(JSONB_BUILD_OBJECT(
                'group_id', rg.id,
                'name', rg.name,
                'area', rg.area,
                'census', rg.census,
                'start_of_collection', rg.start_of_collection,
                'end_of_collection', rg.end_of_collection,
                'group_identifier', rg.group_identifier,
                'year', rg.year,
                'institution_name', rg.institution_name,
                'category', rg.category
            )) AS research_groups
        FROM researcher r
        INNER JOIN research_group rg
            ON rg.second_leader_id = r.id
            OR rg.first_leader_id = r.id
        WHERE r.id = ANY(:researcher_ids)
        GROUP BY r.id
    """
    result = await session.execute(
        text(SCRIPT_SQL),
        {'researcher_ids': [str(rid) for rid in researcher_ids]},
    )
    return result.mappings().all()


async def list_subsidy_by_ids(session, researcher_ids: list):
    if not researcher_ids:
        return []

    SCRIPT_SQL = """
        SELECT s.researcher_id AS id,
            JSONB_AGG(JSONB_BUILD_OBJECT(
                'id', s.id,
                'modality_code', s.modality_code,
                'modality_name', s.modality_name,
                'call_title', s.call_title,
                'category_level_code', s.category_level_code,
                'funding_program_name', s.funding_program_name,
                'institute_name', s.institute_name,
                'aid_quantity', s.aid_quantity,
                'scholarship_quantity', s.scholarship_quantity
            )) AS subsidy
        FROM foment s
        WHERE s.researcher_id = ANY(:researcher_ids)
        GROUP BY s.researcher_id
    """
    result = await session.execute(
        text(SCRIPT_SQL),
        {'researcher_ids': [str(rid) for rid in researcher_ids]},
    )
    return result.mappings().all()


async def list_departments_by_ids(session, researcher_ids: list):
    if not researcher_ids:
        return []

    SCRIPT_SQL = """
        SELECT dpr.researcher_id AS id,
            JSONB_AGG(JSONB_BUILD_OBJECT(
                'dep_id', dp.dep_id,
                'org_cod', dp.org_cod,
                'dep_nom', dp.dep_nom,
                'dep_des', dp.dep_des,
                'dep_email', dp.dep_email,
                'dep_site', dp.dep_site,
                'dep_sigla', dp.dep_sigla,
                'dep_tel', dp.dep_tel
            )) AS departments
        FROM ufmg.departament_researcher dpr
        LEFT JOIN ufmg.departament dp
            ON dpr.dep_id = dp.dep_id
        WHERE dpr.researcher_id = ANY(:researcher_ids)
        GROUP BY dpr.researcher_id;
    """
    result = await session.execute(
        text(SCRIPT_SQL),
        {'researcher_ids': [str(rid) for rid in researcher_ids]},
    )
    return result.mappings().all()


async def list_ufmg_data_by_ids(session, researcher_ids: list):
    if not researcher_ids:
        return []

    SCRIPT_SQL = """
        SELECT
            researcher_id AS id,
            full_name,
            gender,
            status_code,
            work_regime,
            job_class,
            job_title,
            job_rank,
            job_reference_code,
            academic_degree,
            organization_entry_date,
            last_promotion_date,
            employment_status_description,
            department_name,
            career_category,
            academic_unit,
            unit_code,
            function_code,
            position_code,
            leadership_start_date,
            leadership_end_date,
            current_function_name,
            function_location,
            registration_number,
            ufmg_registration_number,
            semester_reference
        FROM ufmg.researcher
        WHERE researcher_id = ANY(:researcher_ids);
    """
    result = await session.execute(
        text(SCRIPT_SQL),
        {'researcher_ids': [str(rid) for rid in researcher_ids]},
    )
    return result.mappings().all()


async def list_user_data_by_lattes_ids(session, lattes_ids: list):
    if not lattes_ids:
        return []

    SCRIPT_SQL = """
        SELECT
            u.lattes_id,
            JSONB_BUILD_OBJECT(
                'lattes_id', u.lattes_id,
                'uid', u.uid,
                'email', u.email,
                'gender', u.gender,
                'verify', u.verify,
                'shib_id', u.shib_id,
                'user_id', u.user_id,
                'linkedin', u.linkedin,
                'provider', u.provider,
                'last_name', u.last_name,
                'photo_url', u.photo_url,
                'shib_code', u.shib_code,
                'birth_date', u.birth_date,
                'first_name', u.first_name,
                'course_level', u.course_level,
                'display_name', u.display_name,
                'email_status', u.email_status,
                'registration', u.registration,
                'institution_id', u.institution_id,
                'visible_email', u.visible_email
            ) AS user
        FROM admin.users u
        WHERE u.lattes_id = ANY(:lattes_ids);
    """
    result = await session.execute(
        text(SCRIPT_SQL), {'lattes_ids': lattes_ids}
    )
    return result.mappings().all()


async def get_researcher_filter(session):
    query = researcher_query.ResearcherFilterQuery(session)
    result = await query.execute()
    return result[0] if result else {}
