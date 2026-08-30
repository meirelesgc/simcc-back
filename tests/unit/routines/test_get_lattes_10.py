from http import HTTPStatus
from unittest.mock import MagicMock, patch

import pytest
import respx
import httpx

from scripts.routines.get_lattes_10 import get_lattes_id_10


def test_get_lattes_id_10_success():
    lattes_id = '1290345301125532'
    target_url = f'https://buscatextual.cnpq.br/buscatextual/cv?id={lattes_id}'

    with respx.mock(assert_all_called=False) as respx_mock:
        respx_mock.get(target_url).mock(
            return_value=httpx.Response(
                HTTPStatus.FOUND,
                headers={
                    'Location': 'http://buscatextual.cnpq.br/buscatextual/visualizacv.do?metodo=apresentar&id=K4771266D7'
                },
            )
        )
        result = get_lattes_id_10(lattes_id)
        assert result == 'K4771266D7'


def test_get_lattes_id_10_redirect_to_error_jsp():
    lattes_id = '1776071841412605'
    target_url = f'https://buscatextual.cnpq.br/buscatextual/cv?id={lattes_id}'

    with respx.mock(assert_all_called=False) as respx_mock:
        respx_mock.get(target_url).mock(
            return_value=httpx.Response(
                HTTPStatus.FOUND,
                headers={
                    'Location': 'http://buscatextual.cnpq.br/buscatextual/erro.jsp'
                },
            )
        )
        result = get_lattes_id_10(lattes_id)
        assert result is None


def test_get_lattes_id_10_empty_or_none():
    assert get_lattes_id_10('') is None
    assert get_lattes_id_10(None) is None


def test_get_lattes_id_10_non_302_status():
    lattes_id = '1290345301125532'
    target_url = f'https://buscatextual.cnpq.br/buscatextual/cv?id={lattes_id}'

    with respx.mock(assert_all_called=False) as respx_mock:
        respx_mock.get(target_url).mock(
            return_value=httpx.Response(HTTPStatus.OK, text='<html>...</html>')
        )
        result = get_lattes_id_10(lattes_id)
        assert result is None


def test_get_lattes_id_10_network_error():
    lattes_id = '1290345301125532'
    target_url = f'https://buscatextual.cnpq.br/buscatextual/cv?id={lattes_id}'

    with respx.mock(assert_all_called=False) as respx_mock:
        respx_mock.get(target_url).mock(
            side_effect=httpx.ConnectTimeout('Connection timed out')
        )
        result = get_lattes_id_10(lattes_id)
        assert result is None
