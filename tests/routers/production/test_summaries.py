def test_get_general_production_metrics(client):
    # This endpoint was failing with 500 due to SQLAlchemy session concurrency
    # Testing all aliases
    paths = [
        '/ResearcherData/DadosGerais',
        '/production/general-data',
        '/researcher/DadosGerais',
    ]
    for path in paths:
        response = client.get(path)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        # Even with empty database, it should return a list of years (default last 10 years)
        assert len(data) > 0
        assert 'year' in data[0]


def test_get_general_production_metrics_with_researcher_id(client):
    researcher_id = '72f9a7d3-1d01-447a-8f4b-0e86976a47a1'
    response = client.get(
        '/researcher/DadosGerais', params={'researcher_id': researcher_id}
    )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) > 0
