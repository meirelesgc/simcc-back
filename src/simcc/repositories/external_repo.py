from sqlalchemy import text

from simcc.queries import external_query, institution_query


async def list_article_production(
    session, program_id=None, dep_id=None, year=2020
):
    query = external_query.ResearcherArticleProductionQuery(
        session, program_id, dep_id, year
    )
    return await query.execute()


async def get_departament(session, dep_id=None):
    query = external_query.DepartmentSearchQuery(session, dep_id)
    return await query.execute()


async def get_docentes(session, filters):
    query = external_query.DocenteSearchQuery(session)
    query.apply_filters(filters)
    return await query.execute()


async def get_researcher_data(session, cpf=None, name=None):
    query = external_query.ResearcherDataQuery(session, cpf, name)
    return await query.execute()


async def get_technician(session):
    query = external_query.TechnicianQuery(session)
    return await query.execute()


async def list_words(session, term: str, stopwords: list[str]):
    query = external_query.WordFrequencyQuery(session, term, stopwords)
    return await query.execute()


async def get_departament_rt(session):
    res_query = institution_query.RtMetricsQuery(session, 'researcher')
    teachers = await res_query.execute()

    tech_query = institution_query.RtMetricsQuery(session, 'technician')
    technicians = await tech_query.execute()

    return {'teachers': teachers, 'technician': technicians}


async def post_congregation(session, congregation: list):
    SCRIPT_SQL = """
        INSERT INTO ufmg.mandate(member, departament, mandate, email, phone)
        VALUES (:MEMBRO, :DEPARTAMENTO, :MANDATO, :EMAIL, :TELEFONE);
    """
    # Assuming the repository handles the session execution
    await session.execute(text(SCRIPT_SQL), congregation)
