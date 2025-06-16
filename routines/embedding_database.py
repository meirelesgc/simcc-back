from langchain_openai import OpenAIEmbeddings

from simcc.config import Settings
from simcc.repositories import conn


def get_embeddings(text: str) -> list[float]:
    embeddings_model = OpenAIEmbeddings(
        model='text-embedding-3-large', openai_api_key=Settings().OPENAI_API_KEY
    )
    return embeddings_model.embed_query(text)


def count_tokens(text: str) -> int:
    return 0


def get_cost(total_tokens: int) -> float:
    return 0.0


def process_researcher_abstracts():
    print('--- Stage 1: Processing Researcher Abstracts ---')
    select_query = """
        SELECT id, abstract AS text
        FROM researcher
        WHERE abstract IS NOT NULL AND id NOT IN
        (SELECT reference_id FROM embeddings.abstract);
    """
    records = conn.select(select_query)

    if not records:
        print('No new researcher abstracts to process.')
        return

    params_to_insert = []
    for record in records:
        if record.get('text'):
            print(f'Processing researcher ID: {record["id"]}')
            embeddings = get_embeddings(record['text'])
            price = get_cost(count_tokens(record['text']))
            params_to_insert.append((record['id'], embeddings, price))

    if params_to_insert:
        insert_query = """
            INSERT INTO embeddings.abstract (reference_id, embeddings, price)
            VALUES (%s, %s, %s)
        """
        row_count = conn.execmany(insert_query, params_to_insert)
        print(f'Inserted {row_count} new records into embeddings.abstract.')


def process_article_titles():
    """Fetches, embeds, and stores titles for articles."""
    print('\n--- Stage 2: Processing Article Titles ---')
    select_query = """
        SELECT id, title AS text
        FROM bibliographic_production
        WHERE type = 'ARTICLE' AND title IS NOT NULL AND id NOT IN
        (SELECT reference_id FROM embeddings.article);
    """
    records = conn.select(select_query)

    if not records:
        print('No new article titles to process.')
        return

    params_to_insert = []
    for record in records:
        if record.get('text'):
            print(f'Processing article ID: {record["id"]}')
            embeddings = get_embeddings(record['text'])
            price = get_cost(count_tokens(record['text']))
            params_to_insert.append((record['id'], embeddings, price))

    if params_to_insert:
        insert_query = """
            INSERT INTO embeddings.article (reference_id, embeddings, price)
            VALUES (%s, %s, %s)
        """
        row_count = conn.execmany(insert_query, params_to_insert)
        print(f'Inserted {row_count} new records into embeddings.article.')


def process_openalex_abstracts():
    """Fetches, embeds, and stores abstracts from OpenAlex articles."""
    print('\n--- Stage 3: Processing OpenAlex Article Abstracts ---')
    select_query = """
        SELECT article_id AS id, abstract AS text
        FROM openalex_article
        WHERE abstract IS NOT NULL AND article_id NOT IN
        (SELECT reference_id FROM embeddings.article_abstract);
    """
    records = conn.select(select_query)

    if not records:
        print('No new OpenAlex abstracts to process.')
        return

    params_to_insert = []
    for record in records:
        if record.get('text'):
            print(f'Processing OpenAlex article ID: {record["id"]}')
            embeddings = get_embeddings(record['text'])
            price = get_cost(count_tokens(record['text']))
            params_to_insert.append((record['id'], embeddings, price))

    if params_to_insert:
        insert_query = """
            INSERT INTO embeddings.article_abstract (reference_id, embeddings, price)
            VALUES (%s, %s, %s)
        """
        row_count = conn.execmany(insert_query, params_to_insert)
        print(
            f'Inserted {row_count} new records into embeddings.article_abstract.'
        )


def process_book_titles():
    print('\n--- Stage 4: Processing Book Titles ---')
    select_query = """
        SELECT id, title AS text
        FROM bibliographic_production
        WHERE type = 'BOOK' AND title IS NOT NULL AND id NOT IN
        (SELECT reference_id FROM embeddings.book);
    """
    records = conn.select(select_query)

    if not records:
        print('No new book titles to process.')
        return

    params_to_insert = []
    for record in records:
        if record.get('text'):
            print(f'Processing book ID: {record["id"]}')
            embeddings = get_embeddings(record['text'])
            price = get_cost(count_tokens(record['text']))
            params_to_insert.append((record['id'], embeddings, price))

    if params_to_insert:
        insert_query = """
            INSERT INTO embeddings.book (reference_id, embeddings, price)
            VALUES (%s, %s, %s)
        """
        row_count = conn.execmany(insert_query, params_to_insert)
        print(f'Inserted {row_count} new records into embeddings.book.')


def process_event_titles():
    """Fetches, embeds, and stores titles for works in events."""
    print('\n--- Stage 5: Processing Work in Event Titles ---')
    select_query = """
        SELECT id, title AS text
        FROM bibliographic_production
        WHERE type = 'WORK_IN_EVENT' AND title IS NOT NULL AND id NOT IN
        (SELECT reference_id FROM embeddings.event);
    """
    records = conn.select(select_query)

    if not records:
        print('No new event titles to process.')
        return

    params_to_insert = []
    for record in records:
        if record.get('text'):
            print(f'Processing event work ID: {record["id"]}')
            embeddings = get_embeddings(record['text'])
            price = get_cost(count_tokens(record['text']))
            params_to_insert.append((record['id'], embeddings, price))

    if params_to_insert:
        insert_query = """
            INSERT INTO embeddings.event (reference_id, embeddings, price)
            VALUES (%s, %s, %s)
        """
        row_count = conn.execmany(insert_query, params_to_insert)
        print(f'Inserted {row_count} new records into embeddings.event.')


def process_patent_titles():
    """Fetches, embeds, and stores titles for patents."""
    print('\n--- Stage 6: Processing Patent Titles ---')
    select_query = """
        SELECT id, title AS text 
        FROM patent
        WHERE title IS NOT NULL AND id NOT IN
        (SELECT reference_id FROM embeddings.patent);
    """
    records = conn.select(select_query)

    if not records:
        print('No new patent titles to process.')
        return

    params_to_insert = []
    for record in records:
        if record.get('text'):
            print(f'Processing patent ID: {record["id"]}')
            embeddings = get_embeddings(record['text'])
            price = get_cost(count_tokens(record['text']))
            params_to_insert.append((record['id'], embeddings, price))

    if params_to_insert:
        insert_query = """
            INSERT INTO embeddings.patent (reference_id, embeddings, price)
            VALUES (%s, %s, %s)
        """
        row_count = conn.execmany(insert_query, params_to_insert)
        print(f'Inserted {row_count} new records into embeddings.patent.')


if __name__ == '__main__':
    try:
        process_researcher_abstracts()
        process_article_titles()
        process_openalex_abstracts()
        process_book_titles()
        process_event_titles()
        process_patent_titles()
    finally:
        conn.close()
        print('\nProcess finished and database connection pool closed.')
