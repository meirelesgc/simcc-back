from uuid import UUID

from simcc.repositories import conn


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
