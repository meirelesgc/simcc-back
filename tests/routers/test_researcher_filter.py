import pytest
from httpx import AsyncClient

def test_get_researcher_filter(client):
    response = client.get("/researcher_filter")
    assert response.status_code == 200
    data = response.json()
    assert "area" in data
    assert "graduation" in data
    assert "city" in data
    assert "institution" in data
    assert "modality" in data
    assert "graduate_program" in data
    assert "departament" in data
    
    assert isinstance(data["area"], list)
