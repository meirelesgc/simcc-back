import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from testcontainers.postgres import PostgresContainer

from simcc.app import app
from simcc.core.connection import Connection
from simcc.core.database import get_conn
from tests import factories

POSTGRES_IMAGE = 'pgvector/pgvector:pg17'
INIT_SQL_FILE = 'init.sql'
DB_CONN_MAX_SIZE = 20
DB_CONN_TIMEOUT = 10
INIT_PATH = 'init.sql'


@pytest.fixture(scope='session', autouse=True)
def postgres():
    with PostgresContainer('pgvector/pgvector:pg17') as pg:
        yield pg


async def restore_db(conn: Connection):
    SCRIPT_SQL = """
        DROP SCHEMA IF EXISTS public CASCADE;
        DROP SCHEMA IF EXISTS logs CASCADE;
        DROP SCHEMA IF EXISTS ufmg CASCADE;
        DROP SCHEMA IF EXISTS embeddings CASCADE;

        CREATE SCHEMA public;
    """
    await conn.exec(SCRIPT_SQL)

    with open(INIT_PATH, 'r', encoding='utf-8') as buffer:
        await conn.exec(buffer.read())


@pytest_asyncio.fixture
async def conn(postgres):
    conn = Connection(
        f'postgresql://{postgres.username}:{postgres.password}'
        f'@{postgres.get_container_host_ip()}:{postgres.get_exposed_port(5432)}'
        f'/{postgres.dbname}',
        max_size=DB_CONN_MAX_SIZE,
        timeout=DB_CONN_TIMEOUT,
    )

    await conn.connect()

    await restore_db(conn)

    yield conn

    await conn.disconnect()


@pytest.fixture
def client(conn: Connection):
    async def get_conn_override():
        yield conn

    app.dependency_overrides[get_conn] = get_conn_override

    yield TestClient(app)

    app.dependency_overrides.clear()


@pytest_asyncio.fixture
def create_country(conn: Connection):
    async def _create_country(**kwargs):
        country = factories.CountryFactory.build()
        SCRIPT_SQL = """
            INSERT INTO public.country(
            id, name, name_pt, alpha_2_code, alpha_3_code)
            VALUES (%(id)s, %(name)s, %(name_pt)s, %(alpha_2_code)s, %(alpha_3_code)s);
            """
        await conn.exec(SCRIPT_SQL, country)
        return country

    return _create_country


@pytest_asyncio.fixture
def create_state(conn: Connection, create_country):
    async def _create_state(**kwargs):
        country = await create_country()

        state = factories.StateFactory.build()

        state['country_id'] = country['id']
        SCRIPT_SQL = """
            INSERT INTO public.state(
            id, name, abbreviation, country_id)
            VALUES (%(id)s, %(name)s, %(abbreviation)s, %(country_id)s);
            """
        await conn.exec(SCRIPT_SQL, state)
        return state

    return _create_state


@pytest_asyncio.fixture
def create_city(conn: Connection, create_state):
    async def _create_city(**kwargs):
        state = await create_state()

        city = factories.CityFactory.build(**kwargs)

        city['state_id'] = state['id']
        city['country_id'] = state['country_id']

        SCRIPT_SQL = """
            INSERT INTO public.city(
                id, name, country_id, state_id)
            VALUES (%(id)s, %(name)s, %(country_id)s, %(state_id)s);
            """
        await conn.exec(SCRIPT_SQL, city)
        return city

    return _create_city


@pytest_asyncio.fixture
def create_institution(conn: Connection):
    async def _create_institution(**kwargs):
        institution = factories.InstitutionFactory.build(**kwargs)

        SCRIPT_SQL = """
            INSERT INTO public.institution(
                id, name, acronym, description, lattes_id,
                cnpj, image, latitude, longitude)
            VALUES (
                %(id)s, %(name)s, %(acronym)s, %(description)s, %(lattes_id)s,
                %(cnpj)s, %(image)s, %(latitude)s, %(longitude)s
            );
            """
        await conn.exec(SCRIPT_SQL, institution)
        return institution

    return _create_institution


@pytest_asyncio.fixture
def create_researcher(conn: Connection, create_city, create_institution):
    async def _create_researcher(**kwargs):
        city = await create_city()

        institution_id = kwargs.pop('institution_id', None)
        if not institution_id:
            institution = await create_institution()
            institution_id = institution['id']

        researcher = factories.ResearcherFactory.build(**kwargs)

        researcher['city_id'] = city['id']
        researcher['country_id'] = city['country_id']
        researcher['institution_id'] = institution_id

        SCRIPT_SQL = """
            INSERT INTO public.researcher(
                id, name, lattes_id, lattes_10_id, citations, orcid,
                abstract, abstract_en, abstract_ai, other_information,
                city_id, country_id, classification, has_image, qtt_publications,
                institution_id, graduate_program, graduation, update_abstract,
                docente, student, extra_field, status
            )
            VALUES (%(id)s, %(name)s, %(lattes_id)s, %(lattes_10_id)s, %(citations)s, %(orcid)s,
                %(abstract)s, %(abstract_en)s, %(abstract_ai)s, %(other_information)s,
                %(city_id)s, %(country_id)s, %(classification)s, %(has_image)s, %(qtt_publications)s,
                %(institution_id)s, %(graduate_program)s, %(graduation)s, %(update_abstract)s,
                %(docente)s, %(student)s, %(extra_field)s, %(status)s);
            """
        await conn.exec(SCRIPT_SQL, researcher)
        return researcher

    return _create_researcher


@pytest_asyncio.fixture
def create_participation_event(conn: Connection, create_researcher):
    async def _create_participation_event(**kwargs):
        researcher_id = kwargs.pop('researcher_id', None)

        if not researcher_id:
            researcher = await create_researcher()
            researcher_id = researcher['id']

        event = factories.ParticipationEventsFactory.build(**kwargs)

        event['researcher_id'] = researcher_id

        SCRIPT_SQL = """
            INSERT INTO public.participation_events(id, title, event_name, nature, form_participation,
                type_participation, researcher_id, year, is_new)
            VALUES (%(id)s, %(title)s, %(event_name)s, %(nature)s, %(form_participation)s,
                %(type_participation)s, %(researcher_id)s, %(year)s, %(is_new)s);
            """
        await conn.exec(SCRIPT_SQL, event)
        return event

    return _create_participation_event


@pytest_asyncio.fixture
def create_graduate_program(conn: Connection, create_institution):
    async def _create_graduate_program(**kwargs):
        institution_id = kwargs.pop('institution_id', None)

        if not institution_id:
            institution = await create_institution()
            institution_id = institution['id']

        program = factories.GraduateProgramFactory.build(**kwargs)

        program['institution_id'] = institution_id

        SCRIPT_SQL = """
            INSERT INTO public.graduate_program(graduate_program_id, code, name, name_en, basic_area,
                cooperation_project, area, modality, type, rating,
                institution_id, state, city, region, url_image,
                acronym, description, visible, site, coordinator,
                email, start, phone, periodicity)
            VALUES (%(graduate_program_id)s, %(code)s, %(name)s, %(name_en)s, %(basic_area)s,
                %(cooperation_project)s, %(area)s, %(modality)s, %(type)s, %(rating)s,
                %(institution_id)s, %(state)s, %(city)s, %(region)s, %(url_image)s,
                %(acronym)s, %(description)s, %(visible)s, %(site)s, %(coordinator)s,
                %(email)s, %(start)s, %(phone)s, %(periodicity)s);
        """
        await conn.exec(SCRIPT_SQL, program)
        return program

    return _create_graduate_program


@pytest_asyncio.fixture
def link_researcher_to_program(
    conn: Connection, create_researcher, create_graduate_program
):
    async def _link_researcher_to_program(**kwargs):
        researcher_id = kwargs.pop('researcher_id', None)
        graduate_program_id = kwargs.pop('graduate_program_id', None)

        if not researcher_id:
            researcher = await create_researcher()
            researcher_id = researcher['id']

        if not graduate_program_id:
            program = await create_graduate_program()
            graduate_program_id = program['graduate_program_id']

        link_data = {
            'researcher_id': researcher_id,
            'graduate_program_id': graduate_program_id,
            'year': kwargs.get('year', [2024]),
            'type_': kwargs.get('type_', 'PERMANENTE'),
        }

        SQL = """
            INSERT INTO public.graduate_program_researcher(graduate_program_id, researcher_id, year, type_)
            VALUES (%(graduate_program_id)s, %(researcher_id)s, %(year)s, %(type_)s);
            """
        await conn.exec(SQL, link_data)
        return link_data

    return _link_researcher_to_program


@pytest_asyncio.fixture
def create_department(conn: Connection):
    async def _create_department(**kwargs):
        department = factories.DepartmentFactory.build(**kwargs)

        SCRIPT_SQL = """
            INSERT INTO ufmg.departament(dep_id, org_cod, dep_nom, dep_des, dep_email,
                dep_site, dep_sigla, dep_tel)
            VALUES (%(dep_id)s, %(org_cod)s, %(dep_nom)s, %(dep_des)s, %(dep_email)s,
                %(dep_site)s, %(dep_sigla)s, %(dep_tel)s);
            """
        await conn.exec(SCRIPT_SQL, department)
        return department

    return _create_department


@pytest_asyncio.fixture
def link_researcher_to_department(
    conn: Connection, create_researcher, create_department
):
    async def _link_researcher_to_department(**kwargs):
        researcher_id = kwargs.pop('researcher_id', None)
        dep_id = kwargs.pop('dep_id', None)

        if not researcher_id:
            researcher = await create_researcher()
            researcher_id = researcher['id']

        if not dep_id:
            department = await create_department()
            dep_id = department['dep_id']

        link_data = {
            'researcher_id': researcher_id,
            'dep_id': dep_id,
        }

        SCRIPT_SQL = """
            INSERT INTO ufmg.departament_researcher(dep_id, researcher_id)
            VALUES (%(dep_id)s, %(researcher_id)s);
        """
        await conn.exec(SCRIPT_SQL, link_data)
        return link_data

    return _link_researcher_to_department


@pytest_asyncio.fixture
def create_foment(conn: Connection, create_researcher):
    async def _create_foment(**kwargs):
        researcher_id = kwargs.pop('researcher_id', None)

        if not researcher_id:
            researcher = await create_researcher(**kwargs)
            researcher_id = researcher['id']

        foment_data = factories.FomentFactory.build(**kwargs)

        foment_data['researcher_id'] = researcher_id

        SCRIPT_SQL = """
            INSERT INTO public.foment(id, researcher_id, modality_code, modality_name, call_title,
                category_level_code, funding_program_name, institute_name,
                aid_quantity, scholarship_quantity)
            VALUES (%(id)s, %(researcher_id)s, %(modality_code)s, %(modality_name)s, %(call_title)s,
                %(category_level_code)s, %(funding_program_name)s, %(institute_name)s,
                %(aid_quantity)s, %(scholarship_quantity)s);
            """
        await conn.exec(SCRIPT_SQL, foment_data)
        return foment_data

    return _create_foment


@pytest_asyncio.fixture
def create_researcher_production(conn: Connection, create_researcher):
    async def _create_researcher_production(**kwargs):
        researcher_id = kwargs.pop('researcher_id', None)

        if not researcher_id:
            researcher = await create_researcher()
            researcher_id = researcher['id']

        production_data = factories.ResearcherProductionFactory.build(**kwargs)

        production_data['researcher_id'] = researcher_id
        SCRIPT_SQL = """
            INSERT INTO public.researcher_production(researcher_production_id, researcher_id, articles, book_chapters,
                book, work_in_event, patent, software, brand, great_area,
                great_area_, area_specialty, city, organ)
            VALUES (%(researcher_production_id)s, %(researcher_id)s, %(articles)s, %(book_chapters)s,
                %(book)s, %(work_in_event)s, %(patent)s, %(software)s, %(brand)s, %(great_area)s,
                %(great_area_)s, %(area_specialty)s, %(city)s, %(organ)s);
            """
        await conn.exec(SCRIPT_SQL, production_data)
        return production_data

    return _create_researcher_production


@pytest_asyncio.fixture
def create_research_group(conn: Connection):
    async def _create_research_group(**kwargs):
        group = factories.ResearchGroupFactory.build(**kwargs)

        SCRIPT_SQL = """
            INSERT INTO public.research_group(
                id, name, institution, first_leader, first_leader_id,
                second_leader, second_leader_id, area, census,
                start_of_collection, end_of_collection, group_identifier,
                year, institution_name, category
            )
            VALUES (
                %(id)s, %(name)s, %(institution)s, %(first_leader)s, %(first_leader_id)s,
                %(second_leader)s, %(second_leader_id)s, %(area)s, %(census)s,
                %(start_of_collection)s, %(end_of_collection)s, %(group_identifier)s,
                %(year)s, %(institution_name)s, %(category)s
            );
        """
        await conn.exec(SCRIPT_SQL, group)
        return group

    return _create_research_group


@pytest_asyncio.fixture
def link_researcher_to_research_group(
    conn: Connection, create_researcher, create_research_group
):
    async def _link_researcher_to_research_group(**kwargs):
        researcher_id = kwargs.pop('researcher_id', None)
        research_group_id = kwargs.pop('research_group_id', None)

        if not researcher_id:
            researcher = await create_researcher()
            researcher_id = researcher['id']

        if not research_group_id:
            group = await create_research_group()
            research_group_id = group['id']

        link_data = {
            'researcher_id': researcher_id,
            'research_group_id': research_group_id,
        }

        SQL = """
            INSERT INTO public.research_group_researcher(research_group_id, researcher_id)
            VALUES (%(research_group_id)s, %(researcher_id)s);
            """
        await conn.exec(SQL, link_data)
        return link_data

    return _link_researcher_to_research_group


@pytest_asyncio.fixture
def create_bibliographic_production(
    conn: Connection, create_country, create_researcher
):
    async def _create_bibliographic_production(**kwargs):
        country_id = kwargs.pop('country_id', None)
        researcher_id = kwargs.pop('researcher_id', None)

        if not country_id:
            country = await create_country()
            country_id = country['id']

        if not researcher_id:
            researcher = await create_researcher()
            researcher_id = researcher['id']

        production_data = factories.BibliographicProductionFactory.build(
            **kwargs
        )

        production_data['country_id'] = country_id
        production_data['researcher_id'] = researcher_id

        SQL = """
            INSERT INTO public.bibliographic_production(id, title, title_en, type, doi, nature, year, country_id,
                language, means_divulgation, homepage, relevance, has_image,
                scientific_divulgation, researcher_id, authors, year_, is_new)
            VALUES (%(id)s, %(title)s, %(title_en)s, %(type)s, %(doi)s, %(nature)s,
                %(year)s, %(country_id)s, %(language)s, %(means_divulgation)s,
                %(homepage)s, %(relevance)s, %(has_image)s,
                %(scientific_divulgation)s, %(researcher_id)s, %(authors)s,
                %(year)s, %(is_new)s);
            """

        await conn.exec(SQL, production_data)

        return production_data

    return _create_bibliographic_production


@pytest_asyncio.fixture
def create_bibliographic_production_book(
    conn: Connection, create_country, create_researcher
):
    async def _create_bibliographic_production_book(**kwargs):
        country_id = kwargs.pop('country_id', None)
        researcher_id = kwargs.pop('researcher_id', None)

        if not country_id:
            country = await create_country()
            country_id = country['id']
        if not researcher_id:
            researcher = await create_researcher()
            researcher_id = researcher['id']

        production = factories.BibliographicProductionFactory.build(**kwargs)
        production['country_id'] = country_id
        production['researcher_id'] = researcher_id
        production['type'] = 'BOOK'

        book_data = factories.BibliographicProductionBookFactory.build(
            **kwargs, bibliographic_production_id=production['id']
        )
        book_data['bibliographic_production'] = production
        SQL = """
            INSERT INTO public.bibliographic_production(id, title, title_en,
                type, doi, nature, year, country_id, language, means_divulgation,
                homepage, relevance, has_image, scientific_divulgation,
                researcher_id, authors, year_, is_new)
            VALUES (%(id)s, %(title)s, %(title_en)s, %(type)s, %(doi)s, %(nature)s, %(year)s, %(country_id)s,
                %(language)s, %(means_divulgation)s, %(homepage)s, %(relevance)s, %(has_image)s,
                %(scientific_divulgation)s, %(researcher_id)s, %(authors)s, %(year)s, %(is_new)s);
            """
        await conn.exec(SQL, production)

        SQL = """
            INSERT INTO public.bibliographic_production_book(id, bibliographic_production_id, isbn, qtt_volume, qtt_pages,
                num_edition_revision, num_series, publishing_company, publishing_company_city)
            VALUES (%(id)s, %(bibliographic_production_id)s, %(isbn)s, %(qtt_volume)s, %(qtt_pages)s,
                %(num_edition_revision)s, %(num_series)s, %(publishing_company)s, %(publishing_company_city)s);
            """
        await conn.exec(SQL, book_data)
        return book_data

    return _create_bibliographic_production_book


@pytest_asyncio.fixture
def create_periodical_magazine(conn: Connection):
    async def _create_periodical_magazine(**kwargs):
        magazine_data = factories.PeriodicalMagazineFactory.build()
        magazine_data.update(kwargs)

        SCRIPT_SQL = """
            INSERT INTO public.periodical_magazine(id, name, issn, qualis, jcr, jcr_link)
            VALUES (%(id)s, %(name)s, %(issn)s, %(qualis)s, %(jcr)s, %(jcr_link)s);
            """

        await conn.exec(SCRIPT_SQL, magazine_data)

        return magazine_data

    return _create_periodical_magazine


@pytest_asyncio.fixture
def create_bibliographic_production_article(
    conn: Connection, create_bibliographic_production, create_periodical_magazine
):
    async def _create_bibliographic_production_article(**kwargs):
        production = await create_bibliographic_production(
            **kwargs, type='ARTICLE'
        )

        magazine = await create_periodical_magazine()

        article_data = factories.BibliographicProductionArticleFactory.build(
            **kwargs
        )

        article_data['bibliographic_production_id'] = production['id']
        article_data['periodical_magazine_id'] = magazine['id']

        SQL_ARTICLE = """
            INSERT INTO public.bibliographic_production_article(
                id, bibliographic_production_id, periodical_magazine_id, volume,
                fascicle, series, start_page, end_page, place_publication,
                periodical_magazine_name, issn, qualis, jcr, jcr_link)
            VALUES (%(id)s, %(bibliographic_production_id)s, %(periodical_magazine_id)s,
                %(volume)s, %(fascicle)s, %(series)s, %(start_page)s, %(end_page)s,
                %(place_publication)s, %(periodical_magazine_name)s, %(issn)s,
                %(qualis)s, %(jcr)s, %(jcr_link)s);
            """

        await conn.exec(SQL_ARTICLE, article_data)

        article_data['bibliographic_production'] = production
        article_data['periodical_magazine'] = magazine

        return article_data

    return _create_bibliographic_production_article


@pytest_asyncio.fixture
def create_patent(conn: Connection, create_researcher):
    async def _create_patent(**kwargs):
        researcher_id = kwargs.pop('researcher_id', None)

        if not researcher_id:
            researcher = await create_researcher()
            researcher_id = researcher['id']

        patent_data = factories.PatentFactory.build(**kwargs)
        patent_data['researcher_id'] = researcher_id

        SCRIPT_SQL = """
            INSERT INTO public.patent(id, title, category, relevance, has_image,
                                      development_year, details, researcher_id, code,
                                      grant_date, deposit_date, is_new, created_at)
            VALUES (%(id)s, %(title)s, %(category)s, %(relevance)s, %(has_image)s,
                    %(development_year)s, %(details)s, %(researcher_id)s, %(code)s,
                    %(grant_date)s, %(deposit_date)s, %(is_new)s, %(created_at)s);
        """
        await conn.exec(SCRIPT_SQL, patent_data)
        return patent_data

    return _create_patent
