from simcc.repositories import conn


def logger_researcher_routine(researcher_id, type_, error, detail=None):
    params = {
        'researcher_id': researcher_id,
        'type': type_,
        'error': error,
        'detail': detail,
    }
    SCRIPT_SQL = """
        INSERT INTO logs.researcher_routine
        (researcher_id, type, error, detail)
        VALUES (%(researcher_id)s, %(type)s, %(error)s, %(detail)s);
        """
    conn.exec(SCRIPT_SQL, params)


def logger_routine(type_, error, detail=None):
    params = {
        'type': type_,
        'error': error,
        'detail': detail,
    }
    SCRIPT_SQL = """
        INSERT INTO logs.routine
        (type, error, detail)
        VALUES (%(type)s, %(error)s, %(detail)s);
        """
    conn.exec(SCRIPT_SQL, params)
