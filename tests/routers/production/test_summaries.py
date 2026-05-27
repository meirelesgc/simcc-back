def test_get_general_production_metrics(client):
    # This endpoint was failing with 500 due to SQLAlchemy session concurrency
    # Testing both aliases
    for path in ['/ResearcherData/DadosGerais', '/production/general-data']:
        response = client.get(path)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        # Even with empty database, it should return a list of years (default last 10 years)
        assert len(data) > 0
        assert 'year' in data[0]
