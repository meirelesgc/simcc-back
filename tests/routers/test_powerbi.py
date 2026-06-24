import csv
import os
from http import HTTPStatus

import pytest

from simcc.core.db.model import (
    AreaExpertise,
    AreaSpecialty,
    GreatAreaExpertise,
    ResearcherAreaExpertise,
    SubAreaExpertise,
)
from tests.factories import (
    CityFactory,
    CountryFactory,
    InstitutionFactory,
    ResearcherFactory,
)


@pytest.mark.asyncio
async def test_fat_area_specialty_csv(client, session):
    # Create researcher and dependencies
    country = CountryFactory()
    session.add(country)
    await session.flush()

    city = CityFactory(country_id=country.id)
    session.add(city)
    await session.flush()

    inst = InstitutionFactory()
    session.add(inst)
    await session.flush()

    researcher = ResearcherFactory(institution_id=inst.id, city_id=city.id)
    session.add(researcher)
    await session.flush()

    # Create Area Specialty hierarchy
    great_area = GreatAreaExpertise(name='CIENCIAS EXATAS E DA TERRA')
    session.add(great_area)
    await session.flush()

    area = AreaExpertise(
        name='COMPUTACAO', great_area_expertise_id=great_area.id
    )
    session.add(area)
    await session.flush()

    sub_area = SubAreaExpertise(
        name='SISTEMAS DE COMPUTACAO', area_expertise_id=area.id
    )
    session.add(sub_area)
    await session.flush()

    specialty = AreaSpecialty(
        name='BANCO DE DADOS', sub_area_expertise_id=sub_area.id
    )
    session.add(specialty)
    await session.flush()

    # Link researcher and area specialty
    r_area_expertise = ResearcherAreaExpertise(
        researcher_id=researcher.id,
        sub_area_expertise_id=sub_area.id,
        area_expertise_id=area.id,
        great_area_expertise_id=great_area.id,
        area_specialty_id=specialty.id,
        order=1,
    )
    session.add(r_area_expertise)
    await session.commit()

    # Ensure storage folder exists
    storage_path = 'storage/powerBI'
    os.makedirs(storage_path, exist_ok=True)

    # Call endpoint
    response = client.get('/fat_area_specialty.csv')

    assert response.status_code == HTTPStatus.OK
    assert response.headers['content-type'] == 'text/csv; charset=utf-8'

    # Check generated CSV file existence
    csv_file_path = os.path.join(storage_path, 'fat_area_specialty.csv')
    assert os.path.exists(csv_file_path)

    # Read and verify CSV content
    with open(csv_file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == 1
    assert rows[0]['area_specialty_id'] == str(specialty.id)
    assert rows[0]['researcher_id'] == str(researcher.id)
    assert rows[0]['area_specialty'] == 'BANCO DE DADOS'

    # Clean up file
    if os.path.exists(csv_file_path):
        os.remove(csv_file_path)


@pytest.mark.asyncio
async def test_all_powerbi_endpoints(client, session):
    # Ensure storage folder exists
    storage_path = 'storage/powerBI'
    os.makedirs(storage_path, exist_ok=True)
    
    endpoints = [
        '/dim_titulacao.csv',
        '/fat_area_specialty.csv',
        '/fat_great_area.csv',
        '/dim_area_specialty.csv',
        '/dim_great_area.csv',
        '/fat_openalex_researcher.csv',
        '/researcher_area_leader.csv',
        '/fat_openalex_article.csv',
        '/dim_area_leader.csv',
        '/npai.png',
        '/iapos.png',
        '/dim_city.csv',
        '/ufmg_researcher.csv',
        '/DimensaoAno.xlsx',
        '/DimensaoTipoProducao.xlsx',
        '/platform_image.xlsx',
        '/Qualis.xlsx',
        '/data.csv',
        '/cimatec_graduate_program_student.csv',
        '/dim_graduate_program_acronym.csv',
        '/graduate_program_researcher_year_unnest.csv',
        '/graduate_program_student_year_unnest.csv',
        '/dim_departament_technician.csv',
        '/dim_departament_researcher.csv',
        '/fat_group_leaders.csv',
        '/dim_research_group.csv',
        '/dim_category_level_code.csv',
        '/fat_foment.csv',
        '/fat_production_tecnical_year_novo_csv_db.csv',
        '/dim_institution.csv',
        '/researcher_city.csv',
        '/dim_researcher.csv',
        '/fat_simcc_bibliographic_production.csv',
        '/production_tecnical_year.csv',
        '/researcher.csv',
        '/article_qualis_year_institution.csv',
        '/production_researcher.csv',
        '/article_qualis_year.csv',
        '/production_year_distinct.csv',
        '/production_year.csv',
        '/production_coauthors_csv_db.csv',
        '/fat_researcher_ind_prod.csv',
        '/graduate_program_ind_prod.csv',
        '/researcher_production_novo_csv_db.csv',
        '/article_distinct_novo_csv_db.csv',
        '/production_distinct_novo_csv_db.csv',
        '/cimatec_graduate_program_researcher.csv',
        '/cimatec_graduate_program.csv',
        '/dim_departament.csv',
        '/dim_research_project.csv',
        '/fat_research_project_foment.csv',
        '/dim_bibliographic_production_terms.csv',
        '/dim_tecnical_production_terms.csv',
        '/dim_logs_routine.csv',
        '/fat_logs_routine.csv',
        '/fat_event_organization.csv',
        '/fat_participation_events.csv',
        '/materialized_vision.csv',
        '/dim_article_keyword.csv',
        '/fat_article_keyword_.csv',
        '/fat_article_co_authorship.csv',
        '/fat_keywords_cooccurrences.csv',
        '/fat_co_authorship.csv',
        '/guidance.csv',
        '/supervisor.csv',
        '/guidance_per_year.csv',
        '/in_progress_per_year.csv',
        '/dim_tags.csv',
        '/fat_tags.csv',
        '/EtapaOrientacao.xlsx',
        '/ind_guidance_ori.csv',
        '/ind_guidance_distori.csv',
        '/ind_guidance_coaut.csv',
        '/dim_sdg.csv',
        '/fat_sdg_articles.csv',
        '/fat_sdg_alignment_researcher.csv',
        '/fat_guidance_history.csv',
        '/dim_territorio_identidade.csv'
    ]
    
    for endpoint in endpoints:
        response = client.get(endpoint)
        assert response.status_code == HTTPStatus.OK

