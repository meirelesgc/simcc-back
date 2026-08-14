from http import HTTPStatus


def test_root_deve_retornar_ok_e_ola_mundo(client):
    response = client.get('/')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Olá Mundo!'}


def test_static_logs_page(client):
    response = client.get('/static/logs/')

    assert response.status_code == HTTPStatus.OK
    assert 'Simcc Central Log Hub' in response.text

