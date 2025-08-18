import datetime
import random
import string
import uuid
from itertools import product

import factory
from faker import Faker

fake = Faker()

BRAND_GOALS = [
    'Product Identification',
    'Service Differentiation',
    'Corporate Identity',
    'Market Positioning',
]
BRAND_NATURES = ['Figurative', 'Nominative', 'Mixed', 'Three-dimensional']

EMPLOYMENT_TYPES = [
    'Empregado com carteira assinada',
    'Servidor público',
    'Autônomo',
    'Estagiário',
    'Bolsista',
]
PARTICIPATION_NATURES = [
    'Apresentação de Trabalho',
    'Congresso',
    'Simpósio',
    'Seminário',
    'Outra',
]
PARTICIPATION_FORMS = ['Ouvinte', 'Palestrante', 'Organizador', 'Moderador']
PARTICIPATION_TYPES = ['Nacional', 'Internacional', 'Regional', 'Local']
FUNCTIONAL_CLASSIFICATIONS = [
    'Pesquisador',
    'Professor',
    'Analista de Sistemas',
    'Gerente de Projetos',
    'Consultor',
]


CLASSIFICATION_CLASSES = ('A+', 'A', 'B+', 'B', 'C+', 'C', 'D+', 'D', 'E+', 'E')
TYPE_PARTICIPATION = (
    'Apresentação Oral',
    'Conferencista',
    'Moderador',
    'Simposista',
)
GREAT_AREA = [
    'Ciências Exatas e da Terra',
    'Ciências Biológicas',
    'Engenharias',
    'Ciências da Saúde',
    'Ciências Agrárias',
    'Ciências Sociais Aplicadas',
    'Ciências Humanas',
    'Linguística, Letras e Artes',
]
GREAT_AREA_ = [
    'Física',
    'Química',
    'Matemática',
    'Biologia Celular',
    'Genética',
]
AREA_SPECIALTY = [
    'Inteligência Artificial',
    'Bioquímica',
    'Engenharia de Software',
    'Saúde Coletiva',
    'Direito Civil',
]
BIBLIOGRAPHIC_PRODUCTION_TYPES = [
    'BOOK',
    'BOOK_CHAPTER',
    'ARTICLE',
    'WORK_IN_EVENT',
    'TEXT_IN_NEWSPAPER_MAGAZINE',
]
LANG_CODES = ['en', 'pt', 'es', 'fr', 'de', 'it']
letters = string.ascii_uppercase
alpha2_list = [''.join(p) for p in product(letters, repeat=2)]
alpha3_list = [''.join(p) for p in product(letters, repeat=3)]


def last_quadriennial_year():
    now = datetime.date.today()
    current_year = now.year
    last_quad = current_year - (current_year % 4)
    return last_quad - 4


class CountryFactory(factory.Factory):
    class Meta:
        model = dict

    id = factory.LazyFunction(lambda: str(uuid.uuid4()))
    name = factory.Sequence(lambda n: f'Country {n}')
    name_pt = factory.Sequence(lambda n: f'País {n}')
    alpha_2_code = factory.Sequence(lambda n: alpha2_list[n % len(alpha2_list)])
    alpha_3_code = factory.Sequence(lambda n: alpha3_list[n % len(alpha3_list)])


class StateFactory(factory.Factory):
    class Meta:
        model = dict

    id = factory.LazyFunction(lambda: str(uuid.uuid4()))
    name = factory.Sequence(lambda n: f'Estado {n}')
    abbreviation = factory.Sequence(lambda n: alpha2_list[n % len(alpha2_list)])
    country = factory.SubFactory(CountryFactory)


class CityFactory(factory.Factory):
    class Meta:
        model = dict

    id = factory.Faker('uuid4')
    name = factory.Faker('city')
    state = factory.SubFactory(StateFactory)
    country = factory.LazyAttribute(lambda obj: obj.state['country'])


class InstitutionFactory(factory.Factory):
    class Meta:
        model = dict

    id = factory.Faker('uuid4')
    name = factory.Sequence(lambda n: f'INST{n}')
    acronym = factory.Sequence(lambda n: f'INST{n}')
    description = factory.Faker('paragraph', nb_sentences=3)
    cnpj = factory.Faker('numerify', text='##############')
    lattes_id = factory.Faker('numerify', text='############')
    image = factory.Faker('image_url')
    latitude = factory.Faker('latitude')
    longitude = factory.Faker('longitude')


class ResearcherFactory(factory.Factory):
    class Meta:
        model = dict

    id = factory.Faker('uuid4')
    name = factory.Faker('text', max_nb_chars=255)
    lattes_id = factory.Sequence(lambda n: f'{n:016}'[:16])
    lattes_10_id = factory.Sequence(lambda n: f'{n:010}'[:10])
    orcid = factory.Faker('numerify', text='####-####-####-####')
    citations = factory.Faker('numerify', text='###')
    abstract = factory.Faker('paragraph', nb_sentences=5)
    abstract_en = factory.Faker('paragraph', nb_sentences=5)
    abstract_ai = factory.Faker('paragraph', nb_sentences=5)
    other_information = factory.Faker('text', max_nb_chars=5000)

    institution = factory.SubFactory(InstitutionFactory)
    city = factory.SubFactory(CityFactory)
    country = factory.LazyAttribute(lambda o: o.city['country'])

    classification = factory.Faker(
        'random_element', elements=CLASSIFICATION_CLASSES
    )
    has_image = factory.Faker('boolean')
    qtt_publications = factory.Faker('random_int', min=0, max=250)
    graduate_program = factory.Faker('text', max_nb_chars=255)
    graduation = factory.Faker('text', max_nb_chars=30)
    update_abstract = factory.Faker('boolean')
    docente = factory.Faker('boolean')
    student = factory.Faker('boolean')
    extra_field = factory.Faker('text', max_nb_chars=255)
    status = True


class ParticipationEventsFactory(factory.Factory):
    class Meta:
        model = dict

    id = factory.Faker('uuid4')
    title = factory.Faker('sentence', nb_words=6)
    event_name = factory.Faker('bs')
    nature = factory.Faker('word')
    form_participation = factory.Faker('word')
    type_participation = factory.Faker(
        'random_element', elements=TYPE_PARTICIPATION
    )

    researcher = factory.SubFactory(ResearcherFactory)

    year = factory.Faker('pyint', min_value=last_quadriennial_year())
    is_new = factory.Faker('boolean', chance_of_getting_true=80)


class GraduateProgramFactory(factory.DictFactory):
    graduate_program_id = factory.Faker('uuid4')
    code = factory.Faker('bothify', text='??##-##')
    name = factory.Faker('bs')
    name_en = factory.Faker('bs')
    basic_area = factory.Faker('job')
    cooperation_project = factory.Faker('sentence', nb_words=3)
    area = factory.Faker('job')
    modality = factory.Faker(
        'random_element',
        elements=('Mestrado', 'Doutorado', 'Mestrado/Doutorado'),
    )

    type = factory.Faker(
        'random_element', elements=('Acadêmico', 'Profissional')
    )

    rating = factory.Faker('random_element', elements=('3', '4', '5', '6', '7'))

    state = factory.Faker('state_abbr')
    city = factory.Faker('city')
    region = factory.Faker(
        'random_element',
        elements=('Nordeste', 'Sudeste', 'Sul', 'Norte', 'Centro-Oeste'),
    )
    url_image = factory.Faker('image_url')
    acronym = factory.Sequence(lambda n: f'PPG{n}')
    description = factory.Faker('text', max_nb_chars=500)
    visible = factory.Faker('boolean')
    site = factory.Faker('url')
    coordinator = factory.Faker('name')
    email = factory.Faker('company_email')
    start = factory.Faker('date_between', start_date='-20y', end_date='-2y')
    phone = factory.Faker('phone_number')
    periodicity = factory.Faker(
        'random_element', elements=('Anual', 'Semestral')
    )


class DepartmentFactory(factory.Factory):
    class Meta:
        model = dict

    dep_id = factory.Faker('word')
    org_cod = factory.Faker('pystr', max_chars=20)
    dep_nom = factory.LazyAttribute(lambda o: fake.company())
    dep_des = factory.Faker('paragraph', nb_sentences=5)
    dep_email = factory.LazyAttribute(lambda o: fake.email())
    dep_site = factory.Faker('url')
    dep_sigla = factory.Sequence(lambda n: f'DEPTO{n + 1}')
    dep_tel = factory.LazyAttribute(lambda o: fake.phone_number())


class ResearcherProductionFactory(factory.Factory):
    class Meta:
        model = dict

    researcher_production_id = factory.Faker('uuid4')

    articles = factory.Faker('pyint', min_value=0, max_value=50)
    book_chapters = factory.Faker('pyint', min_value=0, max_value=20)
    book = factory.Faker('pyint', min_value=0, max_value=5)
    work_in_event = factory.Faker('pyint', min_value=0, max_value=100)
    patent = factory.Faker('pyint', min_value=0, max_value=3)
    software = factory.Faker('pyint', min_value=0, max_value=10)
    brand = factory.Faker('pyint', min_value=0, max_value=2)

    great_area = factory.Faker('random_element', elements=GREAT_AREA)
    great_area_ = factory.LazyAttribute(
        lambda o: fake.random_choices(
            elements=GREAT_AREA_, length=fake.random_int(min=1, max=3)
        )
    )
    area_specialty = factory.Faker('random_element', elements=AREA_SPECIALTY)
    city = factory.LazyAttribute(lambda o: fake.city())
    organ = factory.LazyAttribute(lambda o: fake.company())


class FomentFactory(factory.Factory):
    class Meta:
        model = dict

    id = factory.Faker('uuid4')
    researcher = factory.SubFactory(ResearcherFactory)
    modality_code = factory.Sequence(lambda n: f'MOD-{n:03}')
    modality_name = factory.Faker('bs')
    call_title = factory.Faker('sentence', nb_words=6)
    category_level_code = factory.Sequence(lambda n: f'CAT-{n:02}')
    funding_program_name = factory.Faker('catch_phrase')
    institute_name = factory.Faker('company')
    aid_quantity = factory.Faker('pyint', min_value=0, max_value=5)
    scholarship_quantity = factory.Faker('pyint', min_value=1, max_value=10)


class ResearchGroupFactory(factory.Factory):
    class Meta:
        model = dict

    id = factory.Faker('uuid4')

    name = factory.Faker('company')
    institution = factory.Faker('company')
    first_leader = factory.Faker('name')
    first_leader_id = factory.Faker('uuid4')
    second_leader = factory.Faker('name')
    second_leader_id = factory.Faker('uuid4')
    area = factory.Faker('bs')
    census = factory.Faker('pyint', min_value=10, max_value=500)
    start_of_collection = factory.Faker('year')
    end_of_collection = factory.Faker('year')
    group_identifier = factory.Sequence(lambda n: f'GRP-ID-{n:06}')
    year = factory.Faker('pyint', min_value=2000, max_value=2024)
    institution_name = factory.Faker('company')
    category = factory.Faker('word')


class BibliographicProductionFactory(factory.Factory):
    class Meta:
        model = dict

    id = factory.Faker('uuid4')
    title = factory.Faker('sentence', nb_words=6)
    title_en = factory.Faker('sentence', nb_words=6, locale='en_US')

    type = factory.Faker(
        'random_element', elements=BIBLIOGRAPHIC_PRODUCTION_TYPES
    )

    doi = factory.Faker('doi')
    nature = factory.Faker('word')

    language = factory.LazyFunction(lambda: random.choice(LANG_CODES))
    means_divulgation = factory.Faker(
        'random_element', elements=('WEB', 'PRINT', 'EVENTO')
    )
    homepage = factory.Faker('url')
    relevance = factory.Faker('boolean')
    has_image = factory.Faker('boolean')
    scientific_divulgation = factory.Faker('boolean')

    authors = factory.LazyFunction(
        lambda: ', '.join(fake.name() for _ in range(random.randint(1, 4)))
    )

    is_new = factory.Faker('boolean')

    country = factory.SubFactory(CountryFactory)
    researcher = factory.SubFactory(ResearcherFactory)

    country_id = factory.LazyAttribute(lambda obj: obj.country['id'])
    researcher_id = factory.LazyAttribute(lambda obj: obj.researcher['id'])
    year = factory.Faker('pyint', min_value=last_quadriennial_year())
    year_ = factory.Faker('pyint', min_value=last_quadriennial_year())


class BibliographicProductionBookFactory(factory.Factory):
    class Meta:
        model = dict

    id = factory.Faker('uuid4')
    isbn = factory.Faker('isbn13', separator='')
    qtt_volume = factory.LazyFunction(lambda: str(random.randint(1, 5)))
    qtt_pages = factory.LazyFunction(lambda: str(random.randint(50, 800)))
    num_edition_revision = factory.LazyFunction(
        lambda: str(random.randint(1, 10))
    )
    num_series = factory.LazyFunction(lambda: str(random.randint(1, 3)))

    publishing_company = factory.Faker('company')
    publishing_company_city = factory.Faker('city')


QUALIS_OPTIONS = ('A1', 'A2', 'A3', 'A4', 'B1', 'B2', 'B3', 'B4', 'C')


def generate_issn():
    part1 = random.randint(1000, 9999)
    part2 = random.randint(1000, 9999)
    return f'{part1}-{part2}'


class PeriodicalMagazineFactory(factory.Factory):
    class Meta:
        model = dict

    id = factory.Faker('uuid4')
    name = factory.Faker('company', locale='pt_BR')
    issn = factory.LazyFunction(generate_issn)
    qualis = factory.Faker('random_element', elements=QUALIS_OPTIONS)
    jcr = factory.LazyFunction(lambda: f'{random.uniform(0.1, 10.0):.3f}')
    jcr_link = factory.Faker('url')


class BibliographicProductionArticleFactory(factory.Factory):
    class Meta:
        model = dict

    id = factory.Faker('uuid4')

    volume = factory.LazyFunction(lambda: str(random.randint(1, 50)))
    fascicle = factory.LazyFunction(lambda: str(random.randint(1, 12)))
    series = factory.LazyFunction(lambda: str(random.randint(1, 5)))

    start_page = factory.LazyFunction(lambda: str(random.randint(1, 100)))
    end_page = factory.LazyAttribute(
        lambda obj: str(int(obj.start_page) + random.randint(5, 30))
    )

    place_publication = factory.Faker('city', locale='pt_BR')

    periodical_magazine_name = factory.Faker('company', locale='pt_BR')
    issn = factory.LazyFunction(generate_issn)
    qualis = factory.Faker('random_element', elements=QUALIS_OPTIONS)
    jcr = factory.LazyFunction(lambda: f'{random.uniform(0.1, 10.0):.3f}')
    jcr_link = factory.Faker('url')


PATENT_CATEGORIES = [
    'Patente de Invenção (PI)',
    'Patente de Modelo de Utilidade (MU)',
    'Certificado de Adição de Invenção (C)',
    'Desenho Industrial (DI)',
    'Programa de Computador (PC)',
]


class PatentFactory(factory.Factory):
    class Meta:
        model = dict

    id = factory.Faker('uuid4')
    created_at = factory.Faker('date_time_this_decade', tzinfo=None)

    title = factory.Faker('sentence', nb_words=8)
    category = factory.Faker('random_element', elements=PATENT_CATEGORIES)

    relevance = factory.Faker('boolean')
    has_image = factory.Faker('boolean')

    development_year = factory.Faker('pyint', min_value=last_quadriennial_year())
    details = factory.Faker('paragraph', nb_sentences=5)
    code = factory.Sequence(lambda n: f'BR{datetime.datetime.now().year}{n:07d}')
    grant_date = factory.Faker(
        'date_time_between', start_date='-4y', end_date='now', tzinfo=None
    )
    deposit_date = factory.LazyFunction(
        lambda: fake.date_between(start_date='-4y', end_date='now').strftime(
            '%d/%m/%Y'
        )
    )
    is_new = factory.Faker('boolean', chance_of_getting_true=75)
    researcher = factory.SubFactory(ResearcherFactory)
    researcher_id = factory.LazyAttribute(lambda obj: obj.researcher['id'])


class BrandFactory(factory.Factory):
    class Meta:
        model = dict

    id = factory.Faker('uuid4')
    created_at = factory.Faker('date_time_this_decade', tzinfo=None)
    title = factory.Faker('sentence', nb_words=6)
    relevance = factory.Faker('boolean', chance_of_getting_true=30)
    has_image = factory.Faker('boolean', chance_of_getting_true=80)
    goal = factory.Faker('random_element', elements=BRAND_GOALS)
    nature = factory.Faker('random_element', elements=BRAND_NATURES)
    year = factory.Faker('pyint', min_value=last_quadriennial_year())
    is_new = factory.Faker('boolean', chance_of_getting_true=50)
    researcher = factory.SubFactory(ResearcherFactory)
    researcher_id = factory.LazyAttribute(lambda obj: obj.researcher['id'])


class ResearcherProfessionalExperienceFactory(factory.Factory):
    class Meta:
        model = dict

    id = factory.Faker('uuid4')
    researcher_id = factory.Faker('uuid4')
    enterprise = factory.Faker('company', locale='pt_BR')
    start_year = factory.Faker('pyint', min_value=last_quadriennial_year())
    end_year = factory.Faker('pyint', min_value=last_quadriennial_year())
    employment_type = factory.Faker('random_element', elements=EMPLOYMENT_TYPES)
    other_employment_type = factory.Faker('job', locale='pt_BR')
    functional_classification = factory.Faker(
        'random_element', elements=FUNCTIONAL_CLASSIFICATIONS
    )
    other_functional_classification = factory.Faker('job', locale='pt_BR')
    workload_hours_weekly = factory.Faker(
        'random_element', elements=['20', '30', '40', '44']
    )
    exclusive_dedication = factory.Faker('boolean')
    additional_info = factory.Faker('paragraph', nb_sentences=3, locale='pt_BR')
