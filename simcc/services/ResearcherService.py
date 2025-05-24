from uuid import UUID

import pandas as pd
from numpy import nan

from simcc.repositories.simcc import InstitutionRepository, ResearcherRepository
from simcc.schemas.Researcher import CoAuthorship, Researcher


async def get_researcher_filter(conn):
    return await ResearcherRepository.get_researcher_filter(conn)


def search_in_area_specialty(
    term: str = None,
    graduate_program_id: UUID | str = None,
    university: str = None,
    page: int = None,
    lenght: int = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    researchers = ResearcherRepository.search_in_area_specialty(
        term,
        graduate_program_id,
        university,
        page,
        lenght,
        city,
        area,
        modality,
        graduation,
    )
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')


def search_in_participation_event(
    term: str = None,
    graduate_program_id: UUID | str = None,
    university: str = None,
    type: str = None,
    page: int = None,
    lenght: int = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    researchers = ResearcherRepository.search_in_participation_event(
        term,
        graduate_program_id,
        university,
        type,
        page,
        lenght,
        city,
        area,
        modality,
        graduation,
    )
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')


def search_in_book(
    term: str = None,
    graduate_program_id: UUID | str = None,
    university: str = None,
    type: str = None,
    page: int = None,
    lenght: int = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
):
    researchers = ResearcherRepository.search_in_book(
        term,
        graduate_program_id,
        university,
        type,
        page,
        lenght,
        city,
        area,
        modality,
        graduation,
    )
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')


def merge_researcher_data(researchers: pd.DataFrame) -> pd.DataFrame:
    sources = {
        'graduate_programs': ResearcherRepository.list_graduate_programs(),
        'research_groups': ResearcherRepository.list_research_groups(),
        'subsidy': ResearcherRepository.list_foment_data(),
        'departments': ResearcherRepository.list_departament_data(),
    }

    for column, source in sources.items():
        if source:
            dataframe = pd.DataFrame(source)
            researchers = researchers.merge(dataframe, on='id', how='left')
        else:
            researchers[column] = None

    ufmg_data = ResearcherRepository.list_ufmg_data()
    if ufmg_data:
        ufmg_df = pd.DataFrame(ufmg_data)[['id']]
        ufmg_df['ufmg'] = ufmg_data
        researchers = researchers.merge(ufmg_df, on='id', how='left')
    else:
        researchers['ufmg'] = None

    return researchers


def search_in_articles(
    terms: str = None,
    graduate_program_id: UUID = None,
    university: str = None,
    page: int = None,
    lenght: int = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
) -> list[Researcher]:
    researchers = ResearcherRepository.search_in_articles(
        terms,
        graduate_program_id,
        university,
        page,
        lenght,
        city,
        area,
        modality,
        graduation,
    )
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')


def search_in_abstracts(
    terms: str,
    graduate_program_id: UUID,
    university: str,
    page: int = None,
    lenght: int = None,
    city: str = None,
    area: str = None,
    modality: str = None,
    graduation: str = None,
) -> list[Researcher]:
    researchers = ResearcherRepository.search_in_abstracts(
        terms,
        graduate_program_id,
        university,
        page,
        lenght,
        city,
        area,
        modality,
        graduation,
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
    researchers = ResearcherRepository.list_outstanding_researchers(
        name, graduate_program_id, dep_id, page, lenght
    )
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')


def serch_in_name(
    name: str,
    graduate_program_id: UUID,
    dep_id: UUID,
    page: int,
    lenght: int,
    area: str,
    graduate_program: str,
    city: str,
    institution: str,
    modality: str,
    graduation: str,
) -> list[Researcher]:
    researchers = ResearcherRepository.search_in_name(
        name,
        graduate_program_id,
        dep_id,
        page,
        lenght,
        area,
        graduate_program,
        city,
        institution,
        modality,
        graduation,
    )
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')


def list_co_authorship(researcher_id: UUID) -> list[CoAuthorship]:
    co_authorship = ResearcherRepository.list_co_authorship(researcher_id)

    if not co_authorship:
        return []

    co_authorship = pd.DataFrame(co_authorship)
    institution = InstitutionRepository.get_institution(
        researcher_id=researcher_id
    )
    researcher = ResearcherRepository.get_researcher(researcher_id)

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


def professional_experience(
    researcher_id,
    graduate_program_id: UUID | str = None,
    dep_id: str = None,
    year: int = None,
    page: int = None,
    lenght: int = None,
):
    return ResearcherRepository.professional_experience(
        researcher_id, graduate_program_id, dep_id, year, page, lenght
    )


def search_in_patents(
    term: str = None,
    graduate_program_id: UUID | str = None,
    university: str = None,
    page: int = None,
    lenght: int = None,
):
    researchers = ResearcherRepository.search_in_patents(
        term, graduate_program_id, university, page, lenght
    )
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, '')
    return researchers.to_dict(orient='records')


def list_foment_researchers():
    researchers = ResearcherRepository.list_foment_researchers()
    if not researchers:
        return []

    researchers = pd.DataFrame(researchers)
    researchers = merge_researcher_data(researchers)

    researchers = researchers.replace(nan, str())
    return researchers.to_dict(orient='records')


def academic_degree():
    return ResearcherRepository.academic_degree()
