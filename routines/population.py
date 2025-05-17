import os

import nltk

from routines.logger import logger_routine
from simcc.repositories import conn

LOG_PATH = 'logs'


def create_researcher_article_dictionary():
    SCRIPT_SQL = """
        DELETE FROM research_dictionary
        WHERE type_ = 'ARTICLE';
        """
    conn.exec(SCRIPT_SQL)

    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        INSERT INTO research_dictionary (term, frequency, type_)
        WITH words AS (
                SELECT regexp_split_to_table(translate(b.title,'-\.:,;''', ' '), '\s+') AS word
                FROM bibliographic_production b
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
            AND TRIM(word) <> ALL(%(stopwords)s)
        ORDER BY frequency;
        """  # noqa: E501

    conn.exec(SCRIPT_SQL, parameters)


def create_researcher_book_chapter_dictionary():
    SCRIPT_SQL = """
        DELETE FROM research_dictionary
        WHERE type_ = 'BOOK_CHAPTER';
        """
    conn.exec(SCRIPT_SQL)

    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        INSERT INTO research_dictionary (term, frequency, type_)
        WITH _words AS (
            SELECT regexp_split_to_table(translate(b.title,'-\.:,;''', ' '), '\s+') AS word
            FROM bibliographic_production b
            WHERE type = 'BOOK_CHAPTER'
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
        SELECT 	bf.word AS term, ws.total_frequency AS frequency, 'BOOK_CHAPTER' AS type_
        FROM biggest_frequency bf
        JOIN words_sum ws ON bf.normalized_word = ws.normalized_word
        WHERE
            CHAR_LENGTH(bf.word) > 3
            AND bf.frequency > 4
            AND TRIM(bf.word) <> ALL(%(stopwords)s)
        ORDER BY frequency, word;
        """  # noqa: E501

    conn.exec(SCRIPT_SQL, parameters)


def create_researcher_patent_dictionary():
    SCRIPT_SQL = """
        DELETE FROM research_dictionary
        WHERE type_ = 'PATENT';
        """
    conn.exec(SCRIPT_SQL)

    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        INSERT INTO research_dictionary (term, frequency, type_)
        WITH _words AS (
            SELECT regexp_split_to_table(translate(p.title,'-\.:,;''', ' '), '\s+') AS word
            FROM patent p
            ORDER BY word
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
        SELECT 	bf.word AS term, ws.total_frequency AS frequency, 'PATENT' AS type_
        FROM biggest_frequency bf
        JOIN words_sum ws ON bf.normalized_word = ws.normalized_word
        WHERE
            CHAR_LENGTH(bf.word) > 3
            AND bf.frequency > 4
            AND TRIM(bf.word) <> ALL(%(stopwords)s)
        ORDER BY frequency, word;
        """  # noqa: E501

    conn.exec(SCRIPT_SQL, parameters)


def create_researcher_event_dictionary():
    SCRIPT_SQL = """
        DELETE FROM research_dictionary
        WHERE type_ = 'SPEAKER';
        """
    conn.exec(SCRIPT_SQL)

    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        INSERT INTO research_dictionary (term, frequency, type_)
        WITH _words AS (
            SELECT regexp_split_to_table(translate(p.title,'-\.:,;''', ' '), '\s+') AS word
            FROM participation_events p
            ORDER BY word
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
        SELECT 	bf.word AS term, ws.total_frequency AS frequency, 'SPEAKER' AS type_
        FROM biggest_frequency bf
        JOIN words_sum ws ON bf.normalized_word = ws.normalized_word
        WHERE
            CHAR_LENGTH(bf.word) > 3
            AND bf.frequency > 4
            AND TRIM(bf.word) <> ALL(%(stopwords)s)
        ORDER BY frequency, word;
        """  # noqa: E501

    conn.exec(SCRIPT_SQL, parameters)


def create_researcher_abstract_dictionary():
    SCRIPT_SQL = """
        DELETE FROM research_dictionary
        WHERE type_ = 'ABSTRACT';
        """
    conn.exec(SCRIPT_SQL)

    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        INSERT INTO research_dictionary (term, frequency, type_)
        WITH _words AS (
            SELECT regexp_split_to_table(translate(r.abstract,'-\.:,;''', ' '), '\s+') AS word
            FROM researcher r
            ORDER BY word
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
        SELECT 	bf.word AS term, ws.total_frequency AS frequency, 'ABSTRACT' AS type_
        FROM biggest_frequency bf
        JOIN words_sum ws ON bf.normalized_word = ws.normalized_word
        WHERE
            CHAR_LENGTH(bf.word) > 3
            AND bf.frequency > 4
            AND TRIM(bf.word) <> ALL(%(stopwords)s)
        ORDER BY frequency, word;
        """  # noqa: E501

    conn.exec(SCRIPT_SQL, parameters)


def create_researcher_book_dictionary():
    SCRIPT_SQL = """
        DELETE FROM research_dictionary
        WHERE type_ = 'BOOK';
        """
    conn.exec(SCRIPT_SQL)

    stopwords = nltk.corpus.stopwords.words('english')
    stopwords += nltk.corpus.stopwords.words('portuguese')

    parameters = {}
    parameters['stopwords'] = stopwords

    SCRIPT_SQL = r"""
        INSERT INTO research_dictionary (term, frequency, type_)
        WITH _words AS (
            SELECT regexp_split_to_table(translate(b.title,'-\.:,;''', ' '), '\s+') AS word
            FROM bibliographic_production b
            WHERE type = 'BOOK'
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
        SELECT 	bf.word AS term, ws.total_frequency AS frequency, 'BOOK' AS type_
        FROM biggest_frequency bf
        JOIN words_sum ws ON bf.normalized_word = ws.normalized_word
        WHERE
            CHAR_LENGTH(bf.word) > 3
            AND bf.frequency > 4
            AND TRIM(bf.word) <> ALL(%(stopwords)s)
        ORDER BY frequency, word;
        """  # noqa: E501

    conn.exec(SCRIPT_SQL, parameters)


def list_researchers():
    SCRIPT_SQL = """
        SELECT id AS researcher_id, name, lattes_id
        FROM public.researcher
        """
    result = conn.select(SCRIPT_SQL)
    return result


if __name__ == '__main__':
    for directory in [LOG_PATH]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    create_researcher_article_dictionary()
    create_researcher_book_dictionary()
    create_researcher_book_chapter_dictionary()

    create_researcher_abstract_dictionary()  # OK
    create_researcher_patent_dictionary()  # OK
    create_researcher_event_dictionary()  # OK
    logger_routine('POPULATION', False)
