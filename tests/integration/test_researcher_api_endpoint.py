from http import HTTPStatus
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from simcc import app
from simcc.core.db.models.institution import Institution
from simcc.core.db.models.researcher import Researcher
from simcc.core.db.models.researcher_institution import (
    ResearcherInstitutionData,
)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_researchers_by_institution_endpoint(session):
    # 1. Cria instituição com acrônimo único
    unique_acronym = f'U{uuid4().hex[:6].upper()}'
    inst = Institution(
        name=f'Universidade {unique_acronym}',
        acronym=unique_acronym,
    )
    session.add(inst)
    await session.commit()
    await session.refresh(inst)

    # 2. Cria pesquisador com dados customizados
    unique_lattes = str(uuid4().int)[:16]
    researcher = Researcher(
        name='Pesquisador Endpoint Integracao',
        lattes_id=unique_lattes,
        institution_id=inst.id,
    )
    session.add(researcher)
    await session.commit()
    await session.refresh(researcher)

    inst_data = ResearcherInstitutionData(
        researcher_id=researcher.id,
        zip_code='40000-000',
        work_regime='DE',
        custom_attributes={
            'genero': 'Mulher Cis',
            'siape': '9876543',
            'department': 'DCOMP',
        },
    )
    session.add(inst_data)
    await session.commit()

    # 3. Chama endpoint /researcher com institution_id
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url='http://test'
    ) as ac:
        response = await ac.get(f'/researcher?institution_id={inst.id}')

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) >= 1

    # Valida enriquecimento de dados customizados
    matched = next((r for r in data if r['id'] == str(researcher.id)), None)
    assert matched is not None
    assert matched['name'] == 'Pesquisador Endpoint Integracao'
    assert matched['custom_attributes'] is not None
    assert matched['custom_attributes']['genero'] == 'Mulher Cis'
    assert matched['custom_attributes']['zip_code'] == '40000-000'
    assert matched['custom_attributes']['work_regime'] == 'DE'
    assert matched['custom_attributes']['siape'] == '9876543'
    assert matched['custom_attributes']['department'] == 'DCOMP'

