import pytest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from simcc.services.researcher_service import enrich_researchers


@pytest.mark.asyncio
async def test_enrich_researchers_mapping():
    # Setup mock data
    researcher_id_1 = uuid4()
    researcher_id_2 = uuid4()
    lattes_id_1 = "LATTES_1"
    lattes_id_2 = "LATTES_2"

    researchers = [
        {"id": researcher_id_1, "lattes_id": lattes_id_1},
        {"id": researcher_id_2, "lattes_id": lattes_id_2},
    ]

    # Mock repository responses
    mock_gp = [
        {"id": researcher_id_1, "graduate_programs": [{"name": "GP1"}]},
        {"id": researcher_id_2, "graduate_programs": [{"name": "GP2"}]},
    ]
    mock_rg = [
        {"id": researcher_id_1, "research_groups": [{"name": "RG1"}]}
    ]
    mock_subsidy = [
        {"id": researcher_id_2, "subsidy": [{"name": "S1"}]}
    ]
    mock_dep = [
        {"id": researcher_id_1, "departments": [{"name": "D1"}]}
    ]
    mock_ufmg = [
        {"id": researcher_id_1, "full_name": "Full Name 1"}
    ]
    mock_user = [
        {"lattes_id": lattes_id_1, "user": {"email": "user1@test.com"}}
    ]

    session = AsyncMock()

    with patch("simcc.repositories.researcher_repo.list_graduate_programs_by_ids", AsyncMock(return_value=mock_gp)), \
         patch("simcc.repositories.researcher_repo.list_research_groups_by_ids", AsyncMock(return_value=mock_rg)), \
         patch("simcc.repositories.researcher_repo.list_subsidy_by_ids", AsyncMock(return_value=mock_subsidy)), \
         patch("simcc.repositories.researcher_repo.list_departments_by_ids", AsyncMock(return_value=mock_dep)), \
         patch("simcc.repositories.researcher_repo.list_ufmg_data_by_ids", AsyncMock(return_value=mock_ufmg)), \
         patch("simcc.repositories.researcher_repo.list_user_data_by_lattes_ids", AsyncMock(return_value=mock_user)):
        
        enriched = await enrich_researchers(session, researchers)

        # Assertions for researcher 1
        assert enriched[0]["graduate_programs"] == [{"name": "GP1"}]
        assert enriched[0]["research_groups"] == [{"name": "RG1"}]
        assert enriched[0]["departments"] == [{"name": "D1"}]
        assert enriched[0]["ufmg"]["full_name"] == "Full Name 1"
        assert enriched[0]["user"]["email"] == "user1@test.com"
        assert enriched[0]["subsidy"] == []

        # Assertions for researcher 2
        assert enriched[1]["graduate_programs"] == [{"name": "GP2"}]
        assert enriched[1]["research_groups"] == []
        assert enriched[1]["subsidy"] == [{"name": "S1"}]
        assert enriched[1]["ufmg"] is None
        assert enriched[1]["user"] is None

@pytest.mark.asyncio
async def test_enrich_researchers_with_immutable_rows():
    # Setup mock data using a mock that behaves like RowMapping (immutable)
    researcher_id = uuid4()
    lattes_id = "LATTES_1"
    
    # Simulating what RowMapping.mappings().all() might return or similar immutable structure
    class MockRow:
        def __init__(self, data):
            # Avoid using __dict__ so it behaves more like RowMapping
            object.__setattr__(self, '_data', data)
        def __getitem__(self, key):
            return self._data[key]
        def __iter__(self):
            return iter(self._data)
        def keys(self):
            return self._data.keys()
        def get(self, key, default=None):
            return self._data.get(key, default)
        def __setattr__(self, key, value):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{key}'")
        def __setitem__(self, key, value):
            raise TypeError(f"'{type(self).__name__}' object does not support item assignment")

    researchers = [MockRow({"id": researcher_id, "lattes_id": lattes_id})]

    session = AsyncMock()

    with patch("simcc.repositories.researcher_repo.list_graduate_programs_by_ids", AsyncMock(return_value=[])), \
         patch("simcc.repositories.researcher_repo.list_research_groups_by_ids", AsyncMock(return_value=[])), \
         patch("simcc.repositories.researcher_repo.list_subsidy_by_ids", AsyncMock(return_value=[])), \
         patch("simcc.repositories.researcher_repo.list_departments_by_ids", AsyncMock(return_value=[])), \
         patch("simcc.repositories.researcher_repo.list_ufmg_data_by_ids", AsyncMock(return_value=[])), \
         patch("simcc.repositories.researcher_repo.list_user_data_by_lattes_ids", AsyncMock(return_value=[])):
        
        # This currently fails with AttributeError because MockRow is immutable and enrich_researchers tries to set attrs
        # We want to ensure it either converts to dict or handles it safely
        enriched = await enrich_researchers(session, researchers)
        
        # We expect the result to be enriched (even if empty lists) and accessible
        # If it was converted to dict, we check that
        assert "graduate_programs" in (enriched[0] if isinstance(enriched[0], dict) else enriched[0].__dict__)
