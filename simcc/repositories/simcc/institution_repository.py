from uuid import UUID

from simcc.repositories import conn


async def get_institution_(conn, institution_id):
    params = {}
    filters = str()
    one = False

    if institution_id:
        one = True
        params['institution_id'] = institution_id
        filters += 'AND i.id = %(institution_id)s'

    SCRIPT_SQL = f"""
        WITH researcher_count AS (
            SELECT institution_id, COUNT(DISTINCT id)
                AS count_r
            FROM researcher
            GROUP BY institution_id
        ),
        graduate_program_count AS (
            SELECT institution_id, COUNT(DISTINCT graduate_program_id)
                AS count_gp
            FROM graduate_program
            GROUP BY institution_id
        ),
        graduate_program_researcher_count AS (
            SELECT gp.institution_id, SUM(gpr.count_r) AS count_gpr
            FROM graduate_program gp
            LEFT JOIN (
                SELECT graduate_program_id, COUNT(DISTINCT researcher_id)
                    AS count_r
                FROM graduate_program_researcher
                GROUP BY graduate_program_id
            ) gpr ON gpr.graduate_program_id = gp.graduate_program_id
            GROUP BY gp.institution_id
        ),
        graduate_program_student_count AS (
            SELECT gp.institution_id, SUM(gps.count_s) AS count_gps
            FROM graduate_program gp
            LEFT JOIN (
                SELECT graduate_program_id, COUNT(DISTINCT researcher_id)
                    AS count_s
                FROM graduate_program_student
                GROUP BY graduate_program_id
            ) gps ON gps.graduate_program_id = gp.graduate_program_id
            GROUP BY gp.institution_id
        ),
        ufmg_researcher_count AS (
            SELECT r.institution_id, COUNT(ur.researcher_id) AS count_d
            FROM ufmg.researcher ur
            LEFT JOIN researcher r ON r.id = ur.researcher_id
            GROUP BY r.institution_id
        ),
        technician_count AS (
            SELECT COUNT(*) AS count_t FROM ufmg.technician
        )
        SELECT i.name, i.id, COALESCE(r.count_r, 0) AS count_r,
            COALESCE(gp.count_gp, 0) AS count_gp, COALESCE(gpr.count_gpr, 0)
            AS count_gpr, COALESCE(gps.count_gps, 0) AS count_gps,
            COALESCE(d.count_d, 0) AS count_d, COALESCE(t.count_t, 0)
            AS count_t, i.acronym
        FROM institution i
            LEFT JOIN researcher_count r
                ON r.institution_id = i.id
            LEFT JOIN graduate_program_count gp
                ON gp.institution_id = i.id
            LEFT JOIN graduate_program_researcher_count gpr
                ON gpr.institution_id = i.id
            LEFT JOIN graduate_program_student_count gps
                ON gps.institution_id = i.id
            LEFT JOIN ufmg_researcher_count d
                ON d.institution_id = i.id
            CROSS JOIN technician_count t
        WHERE 1 = 1
            AND i.acronym IS NOT NULL 
            {filters}
            AND deleted_at IS NULL;
    """
    return await conn.select(SCRIPT_SQL, params, one)


def get_institution(institution_id: UUID = None, researcher_id: UUID = None):
    one = True

    params = {}

    filter_id = str()
    if institution_id:
        params['institution_id'] = institution_id
        filter_id = ' AND i.id = %(institution_id)s'

    join_researcher = str()
    filter_researcher = str()
    if researcher_id:
        params['researcher_id'] = researcher_id
        filter_researcher = 'AND r.id = %(researcher_id)s'
        join_researcher = 'RIGHT JOIN researcher r ON i.id = r.institution_id'

    SCRIPT_SQL = f"""
        SELECT *
        FROM institution i
            {join_researcher}
        WHERE 1 = 1
            {filter_id}
            {filter_researcher}
        """

    result = conn.select(SCRIPT_SQL, params, one=one)
    return result
