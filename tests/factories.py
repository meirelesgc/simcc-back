import factory

from simcc.core.db.model import (
    BibliographicProduction,
    BibliographicProductionArticle,
    BibliographicProductionBook,
    BibliographicProductionBookChapter,
    City,
    Country,
    GraduateProgram,
    GraduateProgramResearcher,
    Institution,
    Patent,
    PeriodicalMagazine,
    Researcher,
    ResearcherProduction,
    Software,
)


# Standard Factories
class CountryFactory(factory.Factory):
    class Meta:
        model = Country

    name = factory.Sequence(lambda n: f'Country {n}')
    name_pt = factory.Sequence(lambda n: f'Pais {n}')
    alpha_2_code = factory.Sequence(lambda n: f'{n:02}'[-2:])
    alpha_3_code = factory.Sequence(lambda n: f'{n:03}'[-3:])


class InstitutionFactory(factory.Factory):
    class Meta:
        model = Institution

    name = factory.Faker('company')
    acronym = factory.Sequence(lambda n: f'INST{n}')
    description = factory.Faker('catch_phrase')


class CityFactory(factory.Factory):
    class Meta:
        model = City

    name = factory.Faker('city')


class ResearcherFactory(factory.Factory):
    class Meta:
        model = Researcher

    name = factory.Faker('name')
    lattes_id = factory.Sequence(lambda n: f'LATTES_{n}')
    lattes_10_id = factory.Sequence(lambda n: f'LATTES_10_{n}')
    status = True
    graduation = 'DOUTORADO'


class BibliographicProductionFactory(factory.Factory):
    class Meta:
        model = BibliographicProduction

    title = factory.Faker('sentence')
    type = 'ARTICLE'
    year = factory.Faker('year')
    year_ = factory.LazyAttribute(lambda o: int(o.year))
    nature = 'PERIODICO'
    means_divulgation = 'MEIO_MAGNETICO'


class PeriodicalMagazineFactory(factory.Factory):
    class Meta:
        model = PeriodicalMagazine

    name = factory.Faker('sentence')
    issn = factory.Faker('isbn10')
    qualis = 'A1'


class ArticleFactory(factory.Factory):
    class Meta:
        model = BibliographicProductionArticle

    qualis = 'A1'
    periodical_magazine_name = factory.Faker('sentence')
    issn = factory.Faker('isbn10')
    periodical_magazine_id = factory.SubFactory(PeriodicalMagazineFactory)


class BookFactory(factory.Factory):
    class Meta:
        model = BibliographicProductionBook

    isbn = factory.Faker('isbn13')
    publishing_company = factory.Faker('company')


class BookChapterFactory(factory.Factory):
    class Meta:
        model = BibliographicProductionBookChapter

    isbn = factory.Faker('isbn13')
    publishing_company = factory.Faker('company')


class PatentFactory(factory.Factory):
    class Meta:
        model = Patent

    title = factory.Faker('sentence')
    development_year = '2022'
    category = 'INVENCAO'


class SoftwareFactory(factory.Factory):
    class Meta:
        model = Software

    title = factory.Faker('sentence')
    year = 2022
    platform = 'WEB'


class GraduateProgramFactory(factory.Factory):
    class Meta:
        model = GraduateProgram

    name = factory.Faker('company')
    area = 'CIENCIAS EXATAS'
    modality = 'ACADEMICO'


class GraduateProgramResearcherFactory(factory.Factory):
    class Meta:
        model = GraduateProgramResearcher

    year = 2020


# Composite Helpers
async def create_researcher_with_full_graph(session):
    country = CountryFactory.build()
    session.add(country)
    await session.flush()

    institution = InstitutionFactory.build()
    session.add(institution)
    await session.flush()

    city = CityFactory.build(country_id=country.id)
    session.add(city)
    await session.flush()

    researcher = ResearcherFactory.build(
        institution_id=institution.id,
        city_id=city.id,
        abstract='This is a test abstract for machine learning search.',
    )
    session.add(researcher)
    await session.flush()

    rp = ResearcherProduction(
        researcher_id=researcher.id,
        city=city.name,
        great_area='CIENCIAS_EXATAS_E_DA_TERRA',
        great_area_=['CIENCIAS_EXATAS_E_DA_TERRA'],
        articles=1,
        book=1,
    )
    session.add(rp)
    await session.flush()

    bp1 = BibliographicProductionFactory.build(
        researcher_id=researcher.id,
        type='ARTICLE',
        title='Advanced Machine Learning Algorithms',
    )
    session.add(bp1)
    await session.flush()

    magazine = PeriodicalMagazineFactory.build()
    session.add(magazine)
    await session.flush()

    article = ArticleFactory.build(
        bibliographic_production_id=bp1.id,
        periodical_magazine_id=magazine.id,
        periodical_magazine_name=magazine.name,
        issn=magazine.issn,
    )
    session.add(article)

    bp2 = BibliographicProductionFactory.build(
        researcher_id=researcher.id,
        type='BOOK',
        title='The Future of Computing',
    )
    session.add(bp2)
    await session.flush()

    book = BookFactory.build(bibliographic_production_id=bp2.id)
    session.add(book)

    await session.commit()
    await session.refresh(researcher)
    return researcher
