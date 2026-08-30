from unittest.mock import AsyncMock

import pytest

from simcc.repositories import researcher_repo
from simcc.services import researcher_service


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enrich_researchers_with_custom_attributes(monkeypatch):
    session = AsyncMock()

    async def mock_list_gp(session, ids):
        return []

    async def mock_list_rg(session, ids):
        return []

    async def mock_list_subsidy(session, ids):
        return []

    async def mock_list_dep(session, ids):
        return []

    async def mock_list_ufmg(session, ids):
        return []

    async def mock_list_user(session, ids):
        return []

    async def mock_list_inst(session, ids):
        return [
            {
                'id': 'd8091801-1402-4db6-9e8c-550f75727196',
                'zip_code': '44300-000',
                'work_regime': 'DE',
                'custom_attributes': {
                    'siape': '1673892',
                    'department': 'CAHL',
                    'city': 'CACHOEIRA',
                },
            }
        ]

    monkeypatch.setattr(
        researcher_repo, 'list_graduate_programs_by_ids', mock_list_gp
    )
    monkeypatch.setattr(
        researcher_repo, 'list_research_groups_by_ids', mock_list_rg
    )
    monkeypatch.setattr(
        researcher_repo, 'list_subsidy_by_ids', mock_list_subsidy
    )
    monkeypatch.setattr(
        researcher_repo, 'list_departments_by_ids', mock_list_dep
    )
    monkeypatch.setattr(
        researcher_repo, 'list_ufmg_data_by_ids', mock_list_ufmg
    )
    monkeypatch.setattr(
        researcher_repo, 'list_user_data_by_lattes_ids', mock_list_user
    )
    monkeypatch.setattr(
        researcher_repo,
        'list_institution_data_by_researcher_ids',
        mock_list_inst,
    )

    researchers = [
        {
            'id': 'd8091801-1402-4db6-9e8c-550f75727196',
            'lattes_id': '8343393957854863',
            'name': 'ADRIANO ANUNCIACAO OLIVEIRA',
        },
        {
            'id': 'a1091801-1402-4db6-9e8c-550f75727199',
            'lattes_id': '4198535318645664',
            'name': 'ALBANY MENDONCA SILVA',
        },
    ]

    expected_len = 2
    enriched = await researcher_service.enrich_researchers(
        session, researchers
    )
    assert len(enriched) == expected_len

    # Primeiro pesquisador com dados institucionais
    r1 = enriched[0]
    assert r1['custom_attributes'] is not None
    assert r1['custom_attributes']['zip_code'] == '44300-000'
    assert r1['custom_attributes']['work_regime'] == 'DE'
    assert r1['custom_attributes']['siape'] == '1673892'
    assert r1['custom_attributes']['department'] == 'CAHL'
    assert r1['custom_attributes']['city'] == 'CACHOEIRA'

    # Segundo pesquisador sem dados institucionais
    r2 = enriched[1]
    assert r2['custom_attributes'] is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enrich_researchers_with_arbitrary_custom_attributes(
    monkeypatch,
):
    session = AsyncMock()

    async def mock_list_gp(session, ids):
        return []

    async def mock_list_rg(session, ids):
        return []

    async def mock_list_subsidy(session, ids):
        return []

    async def mock_list_dep(session, ids):
        return []

    async def mock_list_ufmg(session, ids):
        return []

    async def mock_list_user(session, ids):
        return []

    async def mock_list_inst(session, ids):
        return [
            {
                'id': 'd8091801-1402-4db6-9e8c-550f75727196',
                'gender': 'Mulher Cis',
                'zip_code': '44300-000',
                'work_regime': 'DE',
                'custom_attributes': {
                    'categoria': 'Docente',
                    'siape': '1673892',
                },
            }
        ]

    monkeypatch.setattr(
        researcher_repo, 'list_graduate_programs_by_ids', mock_list_gp
    )
    monkeypatch.setattr(
        researcher_repo, 'list_research_groups_by_ids', mock_list_rg
    )
    monkeypatch.setattr(
        researcher_repo, 'list_subsidy_by_ids', mock_list_subsidy
    )
    monkeypatch.setattr(
        researcher_repo, 'list_departments_by_ids', mock_list_dep
    )
    monkeypatch.setattr(
        researcher_repo, 'list_ufmg_data_by_ids', mock_list_ufmg
    )
    monkeypatch.setattr(
        researcher_repo, 'list_user_data_by_lattes_ids', mock_list_user
    )
    monkeypatch.setattr(
        researcher_repo,
        'list_institution_data_by_researcher_ids',
        mock_list_inst,
    )

    researchers = [
        {
            'id': 'd8091801-1402-4db6-9e8c-550f75727196',
            'lattes_id': '8343393957854863',
            'name': 'MARIA CLARA',
        }
    ]

    enriched = await researcher_service.enrich_researchers(
        session, researchers
    )
    assert len(enriched) == 1
    assert enriched[0]['custom_attributes']['gender'] == 'Mulher Cis'
    assert enriched[0]['custom_attributes']['categoria'] == 'Docente'
    assert enriched[0]['custom_attributes']['siape'] == '1673892'
    assert enriched[0]['custom_attributes']['work_regime'] == 'DE'
