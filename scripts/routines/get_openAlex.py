import json
import time
from pathlib import Path
from uuid import uuid4

import requests
from requests.adapters import HTTPAdapter
from sqlalchemy import text
from urllib3.util.retry import Retry

from simcc.core.db.database import get_sync_session
from simcc.core.db.model import OpenAlexArticle, OpenAlexResearcher
from simcc.core.logging import logger
from simcc.core.logging.events import (
    routine_item_error,
    routine_progress,
    routine_step_finished,
    routine_step_started,
)

BASE_PATH = Path('storage/openalex')

ARTICLE_PATH = BASE_PATH / 'article'
RESEARCHER_PATH = BASE_PATH / 'researcher'

ARTICLE_PATH.mkdir(parents=True, exist_ok=True)
RESEARCHER_PATH.mkdir(parents=True, exist_ok=True)

OPENALEX_MAIL = 'seu-email@dominio.com'

WORK_URL = 'https://api.openalex.org/works/https://doi.org/'

AUTHOR_URL = 'https://api.openalex.org/authors/orcid:'


retry_strategy = Retry(
    total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504]
)

adapter = HTTPAdapter(max_retries=retry_strategy)

http = requests.Session()

http.mount('https://', adapter)


SQL_SELECT_ARTICLES = text("""
    SELECT
        bp.id,
        bp.doi
    FROM bibliographic_production bp
    LEFT JOIN openalex_article oa
        ON oa.article_id = bp.id
    WHERE
        bp.doi IS NOT NULL
        AND oa.article_id IS NULL;
""")


SQL_SELECT_RESEARCHERS = text("""
    SELECT
        r.id,
        r.orcid
    FROM researcher r
    LEFT JOIN openalex_researcher oar
        ON oar.researcher_id = r.id
    WHERE
        r.orcid IS NOT NULL
        AND oar.researcher_id IS NULL;
""")


def fetch_json(url: str):
    response = http.get(url, timeout=(5, 30))

    if response.status_code != 200:
        return None, response.status_code

    return response.json(), response.status_code


def safe_get(data, *keys):
    for key in keys:
        if not isinstance(data, dict):
            return None

        data = data.get(key)

    return data


def build_abstract(inverted_index):
    if not inverted_index:
        return None

    if not any(inverted_index.values()):
        return None

    length = max(max(value) for value in inverted_index.values())

    abstract = [''] * (length + 1)

    for word, positions in inverted_index.items():
        for position in positions:
            abstract[position] = word

    return ' '.join(abstract)


def save_json(path: Path, payload):
    with open(path, 'w', encoding='utf-8') as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def extract_article(data, article_id):
    authorships = data.get('authorships', [])

    authors = []

    institutions = []

    for item in authorships:
        author_name = safe_get(item, 'author', 'display_name')

        if author_name:
            authors.append(author_name)

        institution_names = [
            institution.get('display_name')
            for institution in item.get('institutions', [])
            if institution.get('display_name')
        ]

        if institution_names:
            institutions.append(', '.join(institution_names))

    return OpenAlexArticle(
        id=uuid4(),
        article_id=article_id,
        article_institution=safe_get(
            data, 'primary_location', 'source', 'display_name'
        ),
        issn=str(safe_get(data, 'primary_location', 'source', 'issn')),
        abstract=build_abstract(data.get('abstract_inverted_index')),
        authors='; '.join(authors),
        authors_institution='; '.join(institutions),
        language=data.get('language'),
        citations_count=data.get('cited_by_count', 0),
        pdf=safe_get(data, 'primary_location', 'pdf_url'),
        landing_page_url=safe_get(
            data, 'primary_location', 'landing_page_url'
        ),
        keywords='; '.join([
            keyword.get('display_name')
            for keyword in data.get('keywords', [])
            if keyword.get('display_name')
        ]),
    )


def extract_researcher(data, researcher_id):
    orcid = safe_get(data, 'ids', 'orcid')

    if orcid:
        orcid = orcid[-19:]

    return OpenAlexResearcher(
        researcher_id=researcher_id,
        h_index=safe_get(data, 'summary_stats', 'h_index'),
        relevance_score=0,
        works_count=data.get('works_count'),
        cited_by_count=data.get('cited_by_count'),
        i10_index=safe_get(data, 'summary_stats', 'i10_index'),
        scopus=safe_get(data, 'ids', 'scopus'),
        orcid=orcid,
        openalex=data.get('id'),
    )


def process_articles(session):
    results = session.execute(SQL_SELECT_ARTICLES).mappings().all()
    found = len(results)
    success = 0
    failed = 0

    routine_step_started("openalex_articles", total_items=found)

    for i, row in enumerate(results):
        article_id = row['id']
        doi = row['doi']

        try:
            url = f'{WORK_URL}{doi}?mailto={OPENALEX_MAIL}'

            payload, status = fetch_json(url)

            if not payload:
                failed += 1
                routine_item_error(article_id, f"OpenAlex returned status {status}", doi=doi)
                continue

            article = extract_article(payload, article_id)

            session.merge(article)

            save_json(ARTICLE_PATH / f'{article_id}.json', payload)

            session.commit()
            success += 1

            time.sleep(1)

        except Exception as e:
            session.rollback()
            failed += 1
            routine_item_error(article_id, str(e), doi=doi)

        if (i + 1) % 100 == 0 or (i + 1) == found:
            routine_progress("openalex_articles", i + 1, found, success, failed)

    routine_step_finished("openalex_articles", total_items=found)
    return found, success, failed


def process_researchers(session):
    results = session.execute(SQL_SELECT_RESEARCHERS).mappings().all()
    found = len(results)
    success = 0
    failed = 0

    routine_step_started("openalex_researchers", total_items=found)

    for i, row in enumerate(results):
        researcher_id = row['id']
        orcid = row['orcid']

        try:
            url = f'{AUTHOR_URL}{orcid}?mailto={OPENALEX_MAIL}'

            payload, status = fetch_json(url)

            if not payload:
                failed += 1
                routine_item_error(researcher_id, f"OpenAlex returned status {status}", orcid=orcid)
                continue

            researcher = extract_researcher(payload, researcher_id)

            session.merge(researcher)

            save_json(RESEARCHER_PATH / f'{researcher_id}.json', payload)

            session.commit()
            success += 1

            time.sleep(1)

        except Exception as e:
            session.rollback()
            failed += 1
            routine_item_error(researcher_id, str(e), orcid=orcid)

        if (i + 1) % 50 == 0 or (i + 1) == found:
            routine_progress("openalex_researchers", i + 1, found, success, failed)

    routine_step_finished("openalex_researchers", total_items=found)
    return found, success, failed



items_found = 0
items_succeeded = 0
items_failed = 0


def main():
    global items_found, items_succeeded, items_failed
    start = time.perf_counter()

    session = next(get_sync_session())

    try:
        found_res, success_res, failed_res = process_researchers(session)

        found_art, success_art, failed_art = process_articles(session)

        items_found = found_res + found_art
        items_succeeded = success_res + success_art
        items_failed = failed_res + failed_art

        duration = time.perf_counter() - start

    except Exception:
        items_succeeded = 0
        items_failed = items_found
        session.rollback()

        raise

    finally:
        session.close()


if __name__ == '__main__':
    main()
