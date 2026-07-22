import time

import nltk
from sqlalchemy import text

from simcc.core.db.database import get_sync_session



def get_stopwords():
    stopwords = nltk.corpus.stopwords.words('english')
    stopwords.extend(nltk.corpus.stopwords.words('portuguese'))
    return stopwords


def get_query_article():
    return r"""
        INSERT INTO research_dictionary (term, frequency, type_)
        WITH words AS (
                SELECT regexp_split_to_table(translate(title,'-\.:,;''', ' '), '\s+') AS word
                FROM bibliographic_production
                WHERE type = 'ARTICLE'),
            words_count AS (
                SELECT COUNT(*) AS frequency, LOWER(word) AS word
                FROM words
                WHERE word ~ '\w+'
                GROUP BY LOWER(word))
        SELECT word, frequency, 'ARTICLE'
        FROM words_count
        WHERE 1 = 1
            AND CHAR_LENGTH(word) > 3
            AND TRIM(word) <> ALL(:stopwords)
            AND CHAR_LENGTH(word) < 255
        ORDER BY frequency;
    """


def get_query_generic(table, column, doc_type, extra_where=''):
    return rf"""
        INSERT INTO research_dictionary (term, frequency, type_)
        WITH _words AS (
            SELECT regexp_split_to_table(translate({column},'-\.:,;''', ' '), '\s+') AS word
            FROM {table}
            {extra_where}
        ),
        words AS (
            SELECT LOWER(word) AS word, unaccent(LOWER(regexp_replace(word, '[^a-zA-Z0-9À-ÿ\s]', '', 'g'))) AS normalized_word
            FROM _words
        ),
        words_count AS (
            SELECT COUNT(*) AS frequency, word
            FROM words
            WHERE word ~ '\w+'
            GROUP BY word
        ),
        words_sum AS (
            SELECT normalized_word, COUNT(*) AS total_frequency
            FROM words
            GROUP BY normalized_word
        ),
        biggest_frequency AS (
            SELECT DISTINCT ON (w.normalized_word)
                wc.word, wc.frequency, w.normalized_word
            FROM words_count wc
            JOIN words w ON wc.word = w.word
            ORDER BY w.normalized_word, wc.frequency DESC
        )
        SELECT bf.word AS term, ws.total_frequency AS frequency, '{doc_type}' AS type_
        FROM biggest_frequency bf
        JOIN words_sum ws ON bf.normalized_word = ws.normalized_word
        WHERE
            CHAR_LENGTH(bf.word) > 3
            AND CHAR_LENGTH(bf.word) < 255
            AND bf.frequency > 4
            AND TRIM(bf.word) <> ALL(:stopwords)
        ORDER BY frequency, word;
    """


def list_researchers(session):
    result = session.execute(
        text(
            'SELECT id AS researcher_id, name, lattes_id FROM public.researcher'
        )
    )
    return result.fetchall()


def main():
    session = next(get_sync_session())
    start_time = time.perf_counter()


    try:
        stopwords = get_stopwords()

        configurations = [
            ('ARTICLE', get_query_article()),
            (
                'BOOK_CHAPTER',
                get_query_generic(
                    'bibliographic_production',
                    'title',
                    'BOOK_CHAPTER',
                    "WHERE type = 'BOOK_CHAPTER'",
                ),
            ),
            ('PATENT', get_query_generic('patent', 'title', 'PATENT')),
            (
                'SPEAKER',
                get_query_generic('participation_events', 'title', 'SPEAKER'),
            ),
            (
                'ABSTRACT',
                get_query_generic('researcher', 'abstract', 'ABSTRACT'),
            ),
            (
                'BOOK',
                get_query_generic(
                    'bibliographic_production',
                    'title',
                    'BOOK',
                    "WHERE type = 'BOOK'",
                ),
            ),
        ]

        for doc_type, query in configurations:
            session.execute(
                text(
                    f"DELETE FROM research_dictionary WHERE type_ = '{doc_type}';"
                )
            )
            session.execute(text(query), {'stopwords': stopwords})

        session.commit()
        duration = time.perf_counter() - start_time
    except Exception as e:
        session.rollback()
        duration = time.perf_counter() - start_time


if __name__ == '__main__':
    main()
