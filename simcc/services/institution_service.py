from simcc.repositories.simcc import institution_repository


async def get_institution(conn, institution_id):
    return await institution_repository.get_institution_(conn, institution_id)
