import firebase_admin
import pandas as pd
from firebase_admin import credentials, firestore

from routines.logger import logger_routine
from simcc.config import settings
from simcc.repositories import conn


def delete_collection(coll_ref, batch_size):
    docs = coll_ref.limit(batch_size).stream()
    deleted = 0
    batch = db.batch()
    for doc in docs:
        batch.delete(doc.reference)
        deleted += 1
    if deleted > 0:
        batch.commit()
        return deleted
    return 0


def insert_data_batch(collection_ref, data_list, batch_size):
    total_items = len(data_list)
    for i in range(0, total_items, batch_size):
        print('INSERTING BATCH: ', i)

        batch = db.batch()
        for item in data_list[i : i + batch_size]:
            collection_ref.add(item)
        batch.commit()


def terms_dataframe() -> pd.DataFrame:
    SCRIPT_SQL = r"""
        SELECT
            INITCAP(TRANSLATE(term, $$-\".:[],;()'$$, ' ')) AS term,
            frequency, type_, '0' AS great_area,
            unaccent(LOWER(regexp_replace(term, '[^a-zA-Z0-9À-ÿ\s]', '', 'g')))
            AS term_normalize
        FROM public.research_dictionary d
        WHERE term ~ '^[^0-9]+$'
            AND CHAR_LENGTH(d.term) >= 4
            AND frequency >= 24
            AND type_ NOT IN ('BOOK', 'PATENT')

        UNION

        SELECT
            INITCAP(TRANSLATE(term, $$-\".:[],;()'$$, ' ')) AS term,
            frequency, type_, '0',
            unaccent(LOWER(regexp_replace(term, '[^a-zA-Z0-9À-ÿ\s]', '', 'g')))
            AS term_normalize
        FROM public.research_dictionary d
        WHERE term ~ '^[^0-9]+$'
            AND CHAR_LENGTH(d.term) >= 3
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

    try:
        collection_ref = db.collection(settings.FIREBASE_COLLECTION)
        batch_size = 500
        while delete_collection(collection_ref, batch_size) > 0:
            pass
        data_to_insert = terms_dataframe().to_dict(orient='records')
        batch_size = 1000
        insert_data_batch(collection_ref, data_to_insert, batch_size)

        logger_routine('SEARCH_TERM', False)
    except Exception as e:
        logger_routine('SEARCH_TERM', True, str(e))
