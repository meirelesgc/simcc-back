import factory
from simcc.core.db.model import (
    Country,
    City,
    Institution,
    Researcher,
    ResearcherProduction,
    OpenAlexResearcher
)

class CountryFactory(factory.Factory):
    class Meta:
        model = Country

    name = factory.Sequence(lambda n: f"Country_{n}")
    name_pt = factory.Sequence(lambda n: f"Pais_{n}")
    alpha_2_code = factory.Sequence(lambda n: chr(65 + (n // 26) % 26) + chr(65 + n % 26))
    alpha_3_code = factory.Sequence(lambda n: "A" + chr(65 + (n // 26) % 26) + chr(65 + n % 26))

class CityFactory(factory.Factory):
    class Meta:
        model = City

    name = factory.Sequence(lambda n: f"City_{n}")
    country_id = None

class InstitutionFactory(factory.Factory):
    class Meta:
        model = Institution

    name = factory.Sequence(lambda n: f"Institution_{n}")
    acronym = factory.Sequence(lambda n: f"INST_{n}")
    description = factory.Faker('sentence')

class ResearcherFactory(factory.Factory):
    class Meta:
        model = Researcher

    name = factory.Sequence(lambda n: f"Researcher_{n}")
    lattes_id = factory.Sequence(lambda n: f"{n:016d}")
    lattes_10_id = factory.Sequence(lambda n: f"L{n:010d}")
    citations = factory.Sequence(lambda n: f"Researcher_{n}, R.")
    orcid = factory.Sequence(lambda n: f"0000-0002-1825-{n:04d}")
    abstract = factory.Faker('paragraph')
    graduation = "Doutorado"
    classification = "A1"
    stars = 5

class ResearcherProductionFactory(factory.Factory):
    class Meta:
        model = ResearcherProduction

    researcher_id = None
    city = factory.Faker('city')
    great_area = "CIENCIAS_EXATAS_E_DA_TERRA"
    great_area_ = factory.LazyAttribute(lambda o: [o.great_area])
    articles = 0
    book_chapters = 0
    book = 0
    patent = 0
    software = 0
    brand = 0
    work_in_event = 0

class OpenAlexResearcherFactory(factory.Factory):
    class Meta:
        model = OpenAlexResearcher

    researcher_id = None
    h_index = 0
    relevance_score = 0
    works_count = 0
    cited_by_count = 0
    i10_index = 0
    orcid = factory.Sequence(lambda n: f"0000-0002-1825-{n:04d}")
    scopus = factory.Sequence(lambda n: f"SCOPUS_{n}")
    openalex = factory.Sequence(lambda n: f"OPENALEX_{n}")
