from pathlib import Path
import pytest

from simcc.core.utils import (
    get_institution_cover_path,
    get_institution_cover_url,
    get_institution_logo_path,
    get_institution_logo_url,
)


@pytest.mark.unit
def test_get_institution_logo_existing():
    # Testa instituicoes conhecidas presentes no storage
    path_ufba = get_institution_logo_path('UFBA')
    assert path_ufba is not None
    assert path_ufba.name == 'UFBA.png'
    assert path_ufba.exists()

    url_ufba = get_institution_logo_url('UFBA')
    assert url_ufba == '/storage/institutions/picture/UFBA.png'


@pytest.mark.unit
def test_get_institution_cover_existing():
    # Testa capas existentes
    path_ufba = get_institution_cover_path('UFBA')
    assert path_ufba is not None
    assert path_ufba.name == 'UFBA.jpg'
    assert path_ufba.exists()

    url_ufba = get_institution_cover_url('UFBA')
    assert url_ufba == '/storage/institutions/covers/UFBA.jpg'

    # Testa extensao jpeg (UFOB)
    path_ufob = get_institution_cover_path('ufob')
    assert path_ufob is not None
    assert path_ufob.name == 'UFOB.jpeg'
    assert path_ufob.exists()

    url_ufob = get_institution_cover_url('ufob')
    assert url_ufob == '/storage/institutions/covers/UFOB.jpeg'


@pytest.mark.unit
def test_get_institution_assets_nonexistent():
    assert get_institution_logo_path('INEXISTENTE') is None
    assert get_institution_logo_url('INEXISTENTE') is None
    assert get_institution_cover_path('INEXISTENTE') is None
    assert get_institution_cover_url('INEXISTENTE') is None

    assert get_institution_logo_path(None) is None
    assert get_institution_logo_url(None) is None
    assert get_institution_cover_path(None) is None
    assert get_institution_cover_url(None) is None
