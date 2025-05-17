from uuid import UUID

from simcc.core.connection import Connection


async def list_graduate_programs(
    conn: Connection, program_id: UUID, university: str = None
):
    params = {}

    filter_program = str()
    if program_id:
        params['program_id'] = program_id
        filter_program = 'AND gp.graduate_program_id = %(program_id)s'

    filter_university = str()
    join_university = str()
    if university:
        join_university = """
            LEFT JOIN institution i
                ON gp.institution_id = i.id
            """
        params['university'] = university + '%'
        filter_university = 'AND i.name ILIKE %(university)s'

    SCRIPT_SQL = f"""
        WITH permanent AS (
            SELECT graduate_program_id, COUNT(*) AS qtd_permanente
            FROM graduate_program_researcher
            WHERE type_ = 'PERMANENTE'
            GROUP BY graduate_program_id
        ),
        collaborators AS (
            SELECT graduate_program_id, COUNT(*) AS qtd_colaborador
            FROM graduate_program_researcher
            WHERE type_ = 'COLABORADOR'
            GROUP BY graduate_program_id
        ),
        students AS (
            SELECT graduate_program_id, COUNT(*) AS qtd_estudantes
            FROM graduate_program_student
            GROUP BY graduate_program_id
        ),
        researchers AS (
            SELECT graduate_program_id, ARRAY_AGG(r.name) AS researchers
            FROM graduate_program_researcher gpr
                LEFT JOIN researcher r ON gpr.researcher_id = r.id
            GROUP BY graduate_program_id
            HAVING COUNT(r.id) >= 1
        )
        SELECT gp.graduate_program_id, code, gp.name, UPPER(area) AS area,
            UPPER(modality) AS modality, INITCAP(type) AS type, rating,
            institution_id, state, UPPER(city) AS city, region, url_image,
            gp.acronym, gp.description, visible, site, qtd_permanente,
            qtd_colaborador, qtd_estudantes, i.name AS institution,
            COALESCE(r.researchers, ARRAY[]::text[]) AS researchers
        FROM public.graduate_program gp
            LEFT JOIN permanent p
                ON gp.graduate_program_id = p.graduate_program_id
            LEFT JOIN students s
                ON gp.graduate_program_id = s.graduate_program_id
            LEFT JOIN collaborators c
                ON gp.graduate_program_id = c.graduate_program_id
            LEFT JOIN institution i
                ON i.id = gp.institution_id
            LEFT JOIN researchers r
                ON r.graduate_program_id = gp.graduate_program_id
            {join_university}
        WHERE 1 = 1
            {filter_program}
            {filter_university};
        """
    return await conn.select(SCRIPT_SQL, params)
