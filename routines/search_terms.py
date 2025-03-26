from time import sleep, time

import firebase_admin
import pandas as pd
from firebase_admin import credentials, firestore

from routines.logger import logger_routine
from simcc.config import settings
from simcc.repositories import conn


def terms_dataframe() -> pd.DataFrame:
    SCRIPT_SQL = r"""
        SELECT
            regexp_replace(term, '[^a-zA-Z0-9À-ÿ\s]', '', 'g') AS term,
            frequency,
            type_,
            '0' AS great_area,
            unaccent(LOWER(regexp_replace(term, '[^a-zA-Z0-9À-ÿ\s]', '', 'g'))) AS term_normalize
        FROM public.research_dictionary d
        WHERE term ~ '^[^0-9]+$'
            AND CHAR_LENGTH(d.term) >= 4
            AND frequency >= 24
            AND type_ NOT IN ('BOOK', 'PATENT')

        UNION

        SELECT 
            regexp_replace(term, '[^a-zA-Z0-9À-ÿ\s]', '', 'g') AS term,
            frequency, 
            type_, 
            '0',
            unaccent(LOWER(regexp_replace(term, '[^a-zA-Z0-9À-ÿ\s]', '', 'g'))) AS term_normalize
        FROM public.research_dictionary d
        WHERE term ~ '^[^0-9]+$'
            AND CHAR_LENGTH(d.term) >= 4
            AND type_ IN ('BOOK', 'PATENT')

        UNION

        SELECT AREA, 1, 'AREA', great_area,
            unaccent(LOWER(great_area)) AS term_normalize
        FROM (SELECT
                LOWER(
                TRIM(
                STRING_TO_TABLE(
                SPLIT_PART(area_specialty, '|', 1), ';'))) AS AREA,

                LOWER(
                TRIM(
                STRING_TO_TABLE(
                SPLIT_PART(area_specialty, '|', 2), ';'))) AS great_area
            FROM public.researcher_production
            ORDER BY AREA) AS subquery
        UNION

        SELECT name, '1', 'NAME', '0', unaccent(LOWER(name)) AS term_normalize
        FROM researcher
        """
    result = conn.select(SCRIPT_SQL)
    terms = pd.DataFrame(result)
    return terms


if __name__ == '__main__':
    cred = credentials.Certificate('cert.json.json')

    firebase_admin.initialize_app(credential=cred)
    db = firestore.client()

    dictionary = db.collection(settings.FIREBASE_COLLECTION)
    docs = dictionary.stream()

    try:
        count = 0
        cooldown = time()
        for index, doc in enumerate(docs):
            count += 1
            if count >= 80 or time() - cooldown >= 90:
                sleep(10)
                count = 0
                cooldown = time()

            print(f'Deletando [{doc.get("type_")}]')
            doc.reference.delete()

        count = 0
        cooldown = time()
        for item in terms_dataframe().to_dict(orient='records'):
            count += 1
            if count >= 80 or time() - cooldown >= 90:
                sleep(10)
                count = 0
                cooldown = time()
            print(f'Adicionando [{item.get("type_")}]')
            dictionary.add(item)

        logger_routine('SEARCH_TERM', False)
    except Exception as e:
        logger_routine('SEARCH_TERM', True, str(e))
