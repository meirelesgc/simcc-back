from simcc.repositories import conn
from simcc.schemas.Conectee import ResearcherData


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
        SELECT rt, COUNT(rt)
        FROM ufmg.researcher
        GROUP BY rt
        """
    result = conn.select(SCRIPT_SQL)
    return result


def list_technicians():
    SCRIPT_SQL = """
        SELECT rt, COUNT(rt)
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
