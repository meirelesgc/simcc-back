from http import HTTPStatus

import pytest

from simcc import app
from simcc.core.security import get_current_user
from tests.factories import (
    ArticleFactory,
    BibliographicProductionFactory,
    BookChapterFactory,
    BookFactory,
    CityFactory,
    CountryFactory,
    InstitutionFactory,
    PeriodicalMagazineFactory,
    ResearcherFactory,
)


@pytest.fixture(autouse=True)
def override_current_user():
    app.dependency_overrides[get_current_user] = lambda: {'id': 'dummy'}
    yield
    app.dependency_overrides.pop(get_current_user, None)


@pytest.mark.asyncio
async def test_list_book_production(client, session):
    # Setup data
    country = CountryFactory()
    session.add(country)
    await session.flush()

    inst = InstitutionFactory()
    session.add(inst)
    await session.flush()

    city = CityFactory(country_id=country.id)
    session.add(city)
    await session.flush()

    res = ResearcherFactory(institution_id=inst.id, city_id=city.id)
    session.add(res)
    await session.flush()

    book_prod_country = CountryFactory()
    session.add(book_prod_country)
    await session.flush()

    book_prod = BibliographicProductionFactory(
        researcher_id=res.id, country_id=book_prod_country.id, type='BOOK'
    )
    session.add(book_prod)
    await session.flush()

    book = BookFactory(bibliographic_production_id=book_prod.id)
    session.add(book)

    await session.commit()

    response = client.get('/production/book')

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) > 0
    assert data[0]['title'] == book_prod.title


@pytest.mark.asyncio
async def test_list_book_chapter_production(client, session):
    # Setup data
    country = CountryFactory()
    session.add(country)
    await session.flush()

    inst = InstitutionFactory()
    session.add(inst)
    await session.flush()

    city = CityFactory(country_id=country.id)
    session.add(city)
    await session.flush()

    res = ResearcherFactory(institution_id=inst.id, city_id=city.id)
    session.add(res)
    await session.flush()

    chapter_prod_country = CountryFactory()
    session.add(chapter_prod_country)
    await session.flush()

    chapter_prod = BibliographicProductionFactory(
        researcher_id=res.id,
        country_id=chapter_prod_country.id,
        type='BOOK_CHAPTER',
    )
    session.add(chapter_prod)
    await session.flush()

    chapter = BookChapterFactory(bibliographic_production_id=chapter_prod.id)
    session.add(chapter)

    await session.commit()

    response = client.get('/production/book-chapter')

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) > 0
    assert data[0]['title'] == chapter_prod.title


@pytest.mark.asyncio
async def test_list_article_production(client, session):
    # Setup data
    country = CountryFactory()
    session.add(country)
    await session.flush()

    inst = InstitutionFactory()
    session.add(inst)
    await session.flush()

    city = CityFactory(country_id=country.id)
    session.add(city)
    await session.flush()

    res = ResearcherFactory(institution_id=inst.id, city_id=city.id)
    session.add(res)
    await session.flush()

    article_prod_country = CountryFactory()
    session.add(article_prod_country)
    await session.flush()

    article_prod = BibliographicProductionFactory(
        researcher_id=res.id,
        country_id=article_prod_country.id,
        type='ARTICLE',
    )
    session.add(article_prod)
    await session.flush()

    magazine = PeriodicalMagazineFactory()
    session.add(magazine)
    await session.flush()

    article = ArticleFactory(
        bibliographic_production_id=article_prod.id,
        periodical_magazine_id=magazine.id,
    )
    session.add(article)

    await session.commit()

    response = client.get('/production/article')

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) > 0
    assert data[0]['title'] == article_prod.title


@pytest.mark.asyncio
async def test_list_paper_production(client, session):
    # Setup data
    country = CountryFactory()
    session.add(country)
    await session.flush()

    inst = InstitutionFactory()
    session.add(inst)
    await session.flush()

    city = CityFactory(country_id=country.id)
    session.add(city)
    await session.flush()

    res = ResearcherFactory(institution_id=inst.id, city_id=city.id)
    session.add(res)
    await session.flush()

    paper_prod_country = CountryFactory()
    session.add(paper_prod_country)
    await session.flush()

    paper_prod = BibliographicProductionFactory(
        researcher_id=res.id,
        country_id=paper_prod_country.id,
        type='TEXT_IN_NEWSPAPER_MAGAZINE',
    )
    session.add(paper_prod)

    await session.commit()

    response = client.get('/production/paper')

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) > 0
    assert data[0]['title'] == paper_prod.title


@pytest.mark.asyncio
async def test_list_magazine(client, session):
    magazine = PeriodicalMagazineFactory(name='Journal of Artificial Intelligence', issn='1234-5678', jcr='5.5')
    session.add(magazine)
    await session.commit()

    # Search by initials
    response = client.get('/magazine', params={'initials': 'Journal'})
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) > 0
    assert data[0]['magazine'] == magazine.name

    # Search by ISSN
    response = client.get('/magazine', params={'issn': '12345678'})
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data) > 0
    assert data[0]['issn'] == magazine.issn
