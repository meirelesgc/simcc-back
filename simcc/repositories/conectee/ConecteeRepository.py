from simcc.repositories import conn
from simcc.repositories.util import names_filter, websearch_filter
from simcc.schemas.Conectee import ResearcherData


async def get_docentes(conn, filters):
    params = {}
    join_filter = str()
    type_filter = str()
    year_filter = str()
    filter_name = str()
    join_departament = str()
    join_program = str()
    join_institution = str()
    join_researcher_production = str()
    join_foment = str()
    where_extra = str()

    match filters.type:
        case 'ABSTRACT':
            type_filter, term_params = websearch_filter(
                'r.abstract', filters.term
            )
            params.update(term_params)
        case (
            'BOOK'
            | 'BOOK_CHAPTER'
            | 'ARTICLE'
            | 'WORK_IN_EVENT'
            | 'TEXT_IN_NEWSPAPER_MAGAZINE'
        ):
            join_filter = f"""
                INNER JOIN bibliographic_production bp ON bp.researcher_id = ur.researcher_id AND bp.type = '{filters.type}'
            """
            if filters.term:
                term_filter, term_params = websearch_filter(
                    'bp.title', filters.term
                )
                type_filter += term_filter
                params.update(term_params)
            if filters.year:
                year_filter = 'AND bp.year::int >= %(year)s'
                params['year'] = filters.year
        case 'PATENT':
            join_filter = (
                'INNER JOIN patent p ON p.researcher_id = ur.researcher_id'
            )
            if filters.term:
                term_filter, term_params = websearch_filter(
                    'p.title', filters.term
                )
                type_filter += term_filter
                params.update(term_params)
            if filters.year:
                year_filter = 'AND p.development_year::int >= %(year)s'
                params['year'] = filters.year
        case 'AREA':
            join_filter = 'INNER JOIN researcher_production rp ON rp.researcher_id = ur.researcher_id'
            if filters.term:
                term_filter, term_params = websearch_filter(
                    'rp.great_area', filters.term
                )
                type_filter += term_filter
                params.update(term_params)
        case 'EVENT':
            join_filter = 'INNER JOIN event_organization e ON e.researcher_id = ur.researcher_id'
            if filters.term:
                term_filter, term_params = websearch_filter(
                    'e.title', filters.term
                )
                type_filter += term_filter
                params.update(term_params)
            if filters.year:
                year_filter = 'AND e.year::int >= %(year)s'
                params['year'] = filters.year
        case 'NAME':
            if filters.term:
                name_filter_str, term_params = names_filter(
                    'ur.full_name', filters.term
                )
                filter_name += name_filter_str
                params.update(term_params)

    if filters.dep_id or filters.departament:
        join_departament = """
            INNER JOIN ufmg.departament_researcher dpr ON dpr.researcher_id = ur.researcher_id
            INNER JOIN ufmg.departament dp ON dp.dep_id = dpr.dep_id
        """
        if filters.dep_id:
            params['dep_id'] = filters.dep_id.split(';')
            where_extra += ' AND dp.dep_id = ANY(%(dep_id)s)'
        if filters.departament:
            params['departament'] = filters.departament.split(';')
            where_extra += ' AND dp.dep_nom = ANY(%(departament)s)'

    if filters.graduate_program_id or filters.graduate_program:
        join_program = """
            INNER JOIN graduate_program_researcher gpr ON gpr.researcher_id = ur.researcher_id
            INNER JOIN graduate_program gp ON gpr.graduate_program_id = gp.graduate_program_id
        """
        if filters.graduate_program_id:
            params['program_id'] = str(filters.graduate_program_id)
            where_extra += " AND gpr.graduate_program_id = %(program_id)s AND gpr.type_ = 'PERMANENTE'"
        if filters.graduate_program:
            params['graduate_program'] = filters.graduate_program.split(';')
            where_extra += " AND gp.name = ANY(%(graduate_program)s) AND gpr.type_ = 'PERMANENTE'"

    if filters.researcher_id:
        params['researcher_id'] = str(filters.researcher_id)
        where_extra += ' AND ur.researcher_id = %(researcher_id)s'

    if filters.city or filters.area:
        join_researcher_production = 'LEFT JOIN researcher_production rp_prod ON rp_prod.researcher_id = ur.researcher_id'
        if filters.city:
            params['city'] = filters.city.split(';')
            where_extra += ' AND rp_prod.city = ANY(%(city)s)'
        if filters.area:
            params['area'] = filters.area.replace(' ', '_').split(';')
            where_extra += " AND STRING_TO_ARRAY(REPLACE(rp_prod.great_area, ' ', '_'), ';') && %(area)s"

    if filters.modality:
        join_foment = 'INNER JOIN foment f ON f.researcher_id = ur.researcher_id'
        if filters.modality != '*':
            params['modality'] = filters.modality.split(';')
            where_extra += ' AND f.modality_name = ANY(%(modality)s)'

    if filters.graduation:
        params['graduation'] = filters.graduation.split(';')
        where_extra += ' AND ur.academic_degree = ANY(%(graduation)s)'

    distinct_clause = 'DISTINCT' if filters.distinct else ''

    SCRIPT_SQL = f"""
        SELECT {distinct_clause} ur.researcher_id, ur.full_name, ur.gender, ur.status_code, ur.work_regime,
               ur.job_class, ur.job_title, ur.job_rank, ur.job_reference_code, ur.academic_degree,
               ur.organization_entry_date, ur.last_promotion_date,
               ur.employment_status_description, ur.department_name, ur.career_category,
               ur.academic_unit, ur.unit_code, ur.function_code, ur.position_code,
               ur.leadership_start_date, ur.leadership_end_date, ur.current_function_name,
               ur.function_location, ur.registration_number, ur.ufmg_registration_number,
               ur.semester_reference
        FROM ufmg.researcher ur
        {join_filter}
        {join_departament}
        {join_program}
        {join_researcher_production}
        {join_foment}
        WHERE 1 = 1
        {type_filter}
        {filter_name}
        {year_filter}
        {where_extra}
        ORDER BY ur.full_name;
    """

    return await conn.select(SCRIPT_SQL, params)


def get_departament():
    SCRIPT_SQL = """
        WITH researchers AS (
            SELECT dep_id, ARRAY_AGG(r.name) AS researchers
            FROM ufmg.departament_researcher dp
                LEFT JOIN researcher r ON dp.researcher_id = r.id
            GROUP BY dep_id
            HAVING COUNT(r.id) >= 1
        )
        SELECT d.dep_id, d.org_cod, d.dep_nom, d.dep_des, d.dep_email, d.dep_site,
            d.dep_sigla, d.dep_tel, COALESCE(r.researchers, ARRAY[]::text[]) AS researchers
        FROM ufmg.departament d
            LEFT JOIN researchers r
                ON r.dep_id = d.dep_id
        WHERE 1 = 1
        """
    return conn.select(SCRIPT_SQL)


def get_work_regime():
    SCRIPT_SQL = """
        SELECT work_regime AS rt, COUNT(*)
        FROM ufmg.researcher
        GROUP BY rt
        """
    result = conn.select(SCRIPT_SQL)
    return result


def post_congregation(congregation: list):
    SCRIPT_SQL = """
        INSERT INTO ufmg.mandate(member, departament, mandate, email, phone)
        VALUES (%(MEMBRO)s, %(DEPARTAMENTO)s, %(MANDATO)s, %(E-MAIL)s, %(TELEFONE)s);
        """
    conn.execmany(SCRIPT_SQL, congregation)


def get_researcher(cpf=None, name=None):
    params = {}
    cpf_filter = str()
    name_filter = str()

    if cpf:
        cpf = cpf.replace('.', '').replace('-', '')
        params['cpf'] = cpf
        cpf_filter = "AND REPLACE(REPLACE(cpf, '.', ''), '-', '') = %(cpf)s"

    if name:
        params['name'] = name + '%'
        name_filter = 'AND nome ILIKE %(name)s'

    SCRIPT_SQL = f"""
        SELECT nome, cpf, classe, nivel, inicio, fim, tempo_nivel,
            tempo_acumulado, arquivo
        FROM ufmg.researcher_data
        WHERE 1 = 1
            {cpf_filter}
            {name_filter};
        """
    result = conn.select(SCRIPT_SQL, params)
    return result


def insert_researcher(researcher: ResearcherData):
    SCRIPT_SQL = """
        DELETE FROM ufmg.researcher_data WHERE cpf = %(cpf)s;
        """
    conn.exec(SCRIPT_SQL, researcher)

    SCRIPT_SQL = """
        INSERT INTO ufmg.researcher_data
            (nome, cpf, classe, nivel, inicio, fim, tempo_nivel, tempo_acumulado,
            arquivo)
        VALUES (%(nome)s, %(cpf)s, %(classe)s, %(nivel)s, %(inicio)s, %(fim)s,
            %(tempo_nivel)s, %(tempo_acumulado)s, %(arquivo)s);
        """
    conn.exec(SCRIPT_SQL, researcher)


def list_researchers():
    SCRIPT_SQL = """
        SELECT work_regime AS rt, COUNT(*)
        FROM ufmg.researcher
        GROUP BY rt
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_technicians():
    SCRIPT_SQL = """
        SELECT work_regime AS rt, COUNT(*)
        FROM ufmg.technician
        GROUP BY rt
        """
    result = conn.select(SCRIPT_SQL)
    return result


def get_technician():
    SCRIPT_SQL = """
        SELECT technician_id, full_name, gender, status_code, work_regime,
            job_class, job_title, job_rank, job_reference_code, academic_degree,
            organization_entry_date, last_promotion_date,
            employment_status_description, department_name, career_category,
            academic_unit, unit_code, function_code, position_code,
            leadership_start_date, leadership_end_date, current_function_name,
            function_location, registration_number, ufmg_registration_number,
            semester_reference
        FROM ufmg.technician;
        """
    result = conn.select(SCRIPT_SQL)
    return result
