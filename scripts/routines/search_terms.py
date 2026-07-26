import hashlib
import time
from itertools import islice

import firebase_admin
from firebase_admin import credentials, firestore
from sqlalchemy import text

from simcc.core.db.database import get_sync_session
from simcc.core.settings import Settings

FIRESTORE_BATCH_LIMIT = 500


SETTINGS = Settings()


def get_db():
    try:
        firebase_admin.get_app()
    except ValueError:
        cred = credentials.Certificate(SETTINGS.FIREBASE_CERT_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def chunked(iterable, size):
    iterator = iter(iterable)
    while True:
        chunk = list(islice(iterator, size))
        if not chunk:
            break
        yield chunk


def normalize_value(value):
    if value is None:
        return None
    if hasattr(value, 'item'):
        try:
            return value.item()
        except Exception:
            return value
    return value


def list_to_records(records_list):
    for row in records_list:
        yield {
            column: normalize_value(value)
            for column, value in row.items()
        }


def build_document_id(item):
    payload = '|'.join(
        str(item.get(field)) if item.get(field) is not None else ''
        for field in (
            'term_normalize',
            'term',
            'type_',
            'great_area',
            'frequency',
        )
    )
    return hashlib.sha1(payload.encode('utf-8')).hexdigest()


def delete_collection(db, coll_ref, batch_size):
    docs = list(coll_ref.limit(batch_size).stream())
    if not docs:
        return 0

    batch = db.batch()
    for doc in docs:
        batch.delete(doc.reference)
    batch.commit()
    return len(docs)


def insert_data_batch(db, collection_ref, records, batch_size):
    total_inserted = 0

    for chunk in chunked(records, batch_size):
        batch = db.batch()
        for item in chunk:
            doc_id = build_document_id(item)
            batch.set(collection_ref.document(doc_id), item)
        batch.commit()
        total_inserted += len(chunk)

    return total_inserted


def terms_dataframe(session) -> list[dict]:
    script_sql = r"""
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
    result = session.execute(text(script_sql))
    return result.mappings().all()


items_found = 0
items_succeeded = 0
items_failed = 0


def main():
    global items_found, items_succeeded, items_failed
    session = next(get_sync_session())
    start_time = time.perf_counter()

    try:
        db = get_db()
        collection_ref = db.collection(SETTINGS.FIREBASE_COLLECTION)

        deleted_total = 0
        while True:
            deleted = delete_collection(
                db, collection_ref, FIRESTORE_BATCH_LIMIT
            )
            if deleted == 0:
                break
            deleted_total += deleted

        terms_list = terms_dataframe(session)
        items_found = len(terms_list)
        records = list_to_records(terms_list)
        inserted_total = insert_data_batch(
            db,
            collection_ref,
            records,
            FIRESTORE_BATCH_LIMIT,
        )

        session.commit()
        items_succeeded = inserted_total
        items_failed = items_found - items_succeeded
        duration = time.perf_counter() - start_time
    except Exception as e:
        items_succeeded = 0
        items_failed = items_found
        session.rollback()
        duration = time.perf_counter() - start_time


if __name__ == '__main__':
    main()

