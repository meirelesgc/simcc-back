from uuid import UUID

import pandas as pd
from numpy import nan

from simcc.repositories.simcc import InstitutionRepository, researcher_repository
from simcc.schemas.Researcher import CoAuthorship, Researcher


def merge_researcher_data(researchers: pd.DataFrame) -> pd.DataFrame:
    sources = {
        'graduate_programs': researcher_repository.list_graduate_programs(),
        'research_groups': researcher_repository.list_research_groups(),
        'subsidy': researcher_repository.list_foment_data(),
        'departments': researcher_repository.list_departament_data(),
    }

    for column, source in sources.items():
        if source:
            dataframe = pd.DataFrame(source)
            researchers = researchers.merge(dataframe, on='id', how='left')
        else:
            researchers[column] = None

    ufmg_data = researcher_repository.list_ufmg_data()
    if ufmg_data:
        ufmg_df = pd.DataFrame(ufmg_data)[['id']]
        ufmg_df['ufmg'] = ufmg_data
        researchers = researchers.merge(ufmg_df, on='id', how='left')
    else:
        researchers['ufmg'] = None
    user_data = researcher_repository.list_user_data()
    if user_data:
        user_df = pd.DataFrame(user_data)[['lattes_id', 'user']]
        researchers = researchers.merge(user_df, on='lattes_id', how='left')
    else:
        researchers['user'] = None

    return researchers


async def get_researcher_filter(conn):
    return await researcher_repository.get_researcher_filter(conn)


async def search_in_area_specialty(conn, filters):
    researchers = await researcher_repository.search_in_area_specialty(
        conn, filters
    )
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')


async def search_in_participation_event(conn, filters):
    researchers = await researcher_repository.search_in_participation_event(
        conn, filters
    )

    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')


def search_in_book(
    type,
    term,
    graduate_program_id,
    dep_id,
    departament,
    institution,
    graduate_program,
    city,
    area,
    modality,
    graduation,
    page,
    lenght,
):
    researchers = researcher_repository.search_in_book(
        type,
        term,
        graduate_program_id,
        dep_id,
        departament,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
        page,
        lenght,
    )
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')


async def search_in_articles(conn, default_filters):
    researchers = await researcher_repository.search_in_articles(
        conn, default_filters
    )
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')


async def search_in_abstracts(conn, default_filters):
    researchers = await researcher_repository.search_in_abstracts(
        conn, default_filters
    )
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')


def list_outstanding_researchers(
    name: str,
    graduate_program_id: UUID,
    dep_id: UUID,
    page: int,
    lenght: int,
) -> list[Researcher]:
    researchers = researcher_repository.list_outstanding_researchers(
        name, graduate_program_id, dep_id, page, lenght
    )
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')


async def serch_in_name(conn, default_filters, name):
    researchers = await researcher_repository.search_in_name(
        conn, default_filters, name
    )
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')


def list_co_authorship(researcher_id: UUID) -> list[CoAuthorship]:
    co_authorship = researcher_repository.list_co_authorship(researcher_id)

    if not co_authorship:
        return []

    co_authorship = pd.DataFrame(co_authorship)
    institution = InstitutionRepository.get_institution(
        researcher_id=researcher_id
    )
    researcher = researcher_repository.get_researcher(researcher_id)

    def co_authorship_type(co_authorship_institution):
        if co_authorship_institution == institution['name']:
            return 'internal'
        return 'external'

    co_authorship['type'] = co_authorship['institution'].apply(
        co_authorship_type
    )
    co_authorship['initials'] = (
        co_authorship['name']
        .str.replace('.', '', regex=True)
        .str.split()
        .apply(lambda x: ''.join(word[0] for word in x))
    )

    agg_config = {
        'among': 'sum',
        'type': lambda x: 'internal' if 'internal' in x.values else 'external',
    }
    columns = ['name', 'initials']
    co_authorship = co_authorship.groupby(columns).agg(agg_config).reset_index()
    co_authorship = co_authorship[co_authorship['name'] != researcher['name']]
    return co_authorship.to_dict(orient='records')


def search_in_patents(
    term,
    graduate_program_id,
    dep_id,
    departament,
    institution,
    graduate_program,
    city,
    area,
    modality,
    graduation,
    page,
    lenght,
):
    researchers = researcher_repository.search_in_patents(
        term,
        graduate_program_id,
        dep_id,
        departament,
        institution,
        graduate_program,
        city,
        area,
        modality,
        graduation,
        page,
        lenght,
    )
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')


async def list_foment_researchers(default_filters, conn):
    researchers = await researcher_repository.list_foment_researchers(
        default_filters, conn
    )
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, str())
    return researchers.to_dict(orient='records')


async def get_academic_degree(conn, default_filters):
    return await researcher_repository.academic_degree(conn, default_filters)


async def get_great_area(conn, default_filters):
    return await researcher_repository.get_great_area(conn, default_filters)
