import pytest_asyncio

from tests.factories import (
    CityFactory,
    CountryFactory,
    InstitutionFactory,
    OpenAlexResearcherFactory,
    ResearcherFactory,
    ResearcherProductionFactory,
)


@pytest_asyncio.fixture
def create_country(session):
    async def _create_country(**kwargs):
        country = CountryFactory.build(**kwargs)
        session.add(country)
        await session.flush()
        return country

    return _create_country


@pytest_asyncio.fixture
def create_city(session, create_country):
    async def _create_city(**kwargs):
        if 'country_id' not in kwargs:
            country = await create_country()
            kwargs['country_id'] = country.id
        city = CityFactory.build(**kwargs)
        session.add(city)
        await session.flush()
        return city

    return _create_city


@pytest_asyncio.fixture
def create_institution(session):
    async def _create_institution(**kwargs):
        institution = InstitutionFactory.build(**kwargs)
        session.add(institution)
        await session.flush()
        return institution

    return _create_institution


@pytest_asyncio.fixture
def create_researcher(session, create_city, create_institution):
    async def _create_researcher(**kwargs):
        if 'city_id' not in kwargs:
            city = await create_city()
            kwargs['city_id'] = city.id
            if 'country_id' not in kwargs:
                kwargs['country_id'] = city.country_id
        if 'institution_id' not in kwargs:
            institution = await create_institution()
            kwargs['institution_id'] = institution.id
        researcher = ResearcherFactory.build(**kwargs)
        session.add(researcher)
        await session.flush()
        return researcher

    return _create_researcher


@pytest_asyncio.fixture
def create_researcher_production(session, create_researcher):
    async def _create_researcher_production(**kwargs):
        if 'researcher_id' not in kwargs:
            researcher = await create_researcher()
            kwargs['researcher_id'] = researcher.id
        researcher_production = ResearcherProductionFactory.build(**kwargs)
        session.add(researcher_production)
        await session.flush()
        return researcher_production

    return _create_researcher_production


@pytest_asyncio.fixture
def create_openalex_researcher(session, create_researcher):
    async def _create_openalex_researcher(**kwargs):
        if 'researcher_id' not in kwargs:
            researcher = await create_researcher()
            kwargs['researcher_id'] = researcher.id
        openalex_researcher = OpenAlexResearcherFactory.build(**kwargs)
        session.add(openalex_researcher)
        await session.flush()
        return openalex_researcher

    return _create_openalex_researcher
