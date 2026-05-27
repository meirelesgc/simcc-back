from http import HTTPStatus
from datetime import datetime
import pytest
from simcc import app
from simcc.core.security import get_current_user
from tests.factories import create_researcher_with_full_graph

@pytest.fixture(autouse=True)
def override_current_user():
    app.dependency_overrides[get_current_user] = lambda: {'id': 'dummy'}
    yield
    app.dependency_overrides.pop(get_current_user, None)

@pytest.mark.asyncio
async def test_lattes_update_format(client, session):
    researcher = await create_researcher_with_full_graph(session)
    
    response = client.get(
        '/researchers',
        params={
            'researcher_id': str(researcher.id),
        },
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) == 1
    lattes_update = data[0]['lattes_update']
    print(f"\nDEBUG: lattes_update value is: {lattes_update}")
    
    # Check if the format is DD/MM/YYYY
    try:
        datetime.strptime(lattes_update, '%d/%m/%Y')
    except ValueError:
        pytest.fail(f"lattes_update format is incorrect: {lattes_update}. Expected DD/MM/YYYY")
