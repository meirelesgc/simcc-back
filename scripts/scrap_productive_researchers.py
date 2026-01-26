from urllib.parse import parse_qs, urlparse

import scrapy


def get_query_param(url: str, key: str):
    if not url:
        return None
    return parse_qs(urlparse(url).query).get(key, [None])[0]


def norm(value):
    if value is None:
        return None
    value = str(value).strip()
    return value if value else None


class ProductiveSpider(scrapy.Spider):
    name = 'researchers'
    start_urls = ['http://memoria2.cnpq.br/web/guest/bolsistas-vigentes']

    custom_settings = {
        'RETRY_TIMES': 10,
        'DOWNLOAD_TIMEOUT': 60,
        'DOWNLOAD_DELAY': 1.0,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1.0,
        'AUTOTHROTTLE_MAX_DELAY': 10.0,
        'USER_AGENT': 'Mozilla/5.0',
        'DEFAULT_REQUEST_HEADERS': {
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8'
        },
        'COOKIES_ENABLED': True,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.row_n = 0

    def start_requests(self):
        yield scrapy.Request(
            self.start_urls[0], callback=self.parse_all, dont_filter=True
        )

    def parse_all(self, response):
        form = response.css('form#formBolsistas, form[name="formBolsistas"]')
        if form:
            yield scrapy.FormRequest.from_response(
                response,
                formid='formBolsistas',
                formdata={'codigoModalidade': '-'},
                callback=self.parse,
                dont_filter=True,
            )
        else:
            yield from self.parse(response)

    def parse(self, response):
        for researcher in response.css('div.dados-perfil'):
            self.row_n += 1

            nome = researcher.css('div.dados > h3::text').get()
            area = researcher.css('div.dados > h4::text').get()
            subarea = researcher.css('div.dados > h5::text').get()

            email_url = researcher.css(
                'div.btn-group a[title="e-mail"]::attr(href)'
            ).get()
            curriculo_url = researcher.css(
                'div.btn-group a[title="Currículo"]::attr(href)'
            ).get()

            id_lattes = get_query_param(email_url, 'nroIdCNPq')

            modalidade_txt = researcher.xpath(
                './/div[contains(@class,"info-perfil")]//li[label[contains(normalize-space(.),"Modalidade")]]/text()[normalize-space()]'
            ).get()

            instituicao = researcher.xpath(
                './/div[contains(@class,"info-perfil")]//li[label[contains(normalize-space(.),"Instituição")]]/text()[normalize-space()]'
            ).get()

            nivel = None
            cod_modalidade = None
            nome_modalidade = None

            if modalidade_txt:
                partes = [
                    p.strip() for p in modalidade_txt.split('-') if p.strip()
                ]
                if partes:
                    nivel = partes[-1]
                cod_modalidade = (
                    modalidade_txt.strip().split()[0]
                    if modalidade_txt.strip()
                    else None
                )
                nome_modalidade = modalidade_txt.strip()

            yield {
                '#': str(self.row_n),
                '# Id Lattes': norm(id_lattes),
                '# Nome Beneficiário': norm(nome),
                '# CPF Beneficiário': None,
                '# Nome País': None,
                '# Nome Região': None,
                '# Nome UF': None,
                '# Nome Cidade': None,
                '# Nome Grande Área': norm(area),
                '# Nome Área': norm(subarea),
                '# Nome Sub-Área': None,
                '# Cod Modalidade': norm(cod_modalidade),
                '# Nome Modalidade': norm(nome_modalidade),
                '# Título Chamada': None,
                '# Cod Categoria Nível': norm(nivel),
                '# Nome Programa Fomento': None,
                '# Nome Instituto': norm(instituicao),
                'QUANTAUXILIO': '0,00',
                'QUANTBOLSA': '1,00',
                'email_url': norm(email_url),
                'curriculo_url': norm(curriculo_url),
            }

        next_page = response.css(
            'ul.pager li a:contains("Próximo")::attr(href)'
        ).get()
        if next_page and not next_page.startswith('javascript'):
            yield response.follow(next_page, self.parse)


# Comando de execução
# # scrapy runspider scripts/scrap_productive_researchers.py -o tmp.csv -s FEED_EXPORT_DELIMITER=';' -s FEED_EXPORT_ENCODING='utf-8-sig'
