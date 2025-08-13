import random
import string
import uuid
from itertools import product

import factory
from faker import Faker

fake = Faker()

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
letters = string.ascii_uppercase
alpha2_list = [''.join(p) for p in product(letters, repeat=2)]
alpha3_list = [''.join(p) for p in product(letters, repeat=3)]


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

    year = factory.LazyFunction(lambda: random.randint(2021, 2035))
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
