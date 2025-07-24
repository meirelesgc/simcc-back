import sys
from collections import Counter

import spacy

from simcc.repositories import conn

SOURCES = [
    {
        'type_name': 'ARTICLE',
        'column': 'title',
        'sql': "SELECT title FROM bibliographic_production WHERE type = 'ARTICLE' AND title IS NOT NULL",
    },
    {
        'type_name': 'BOOK',
        'column': 'title',
        'sql': "SELECT title FROM bibliographic_production WHERE type = 'BOOK' AND title IS NOT NULL",
    },
    {
        'type_name': 'BOOK_CHAPTER',
        'column': 'title',
        'sql': "SELECT title FROM bibliographic_production WHERE type = 'BOOK_CHAPTER' AND title IS NOT NULL",
    },
    {
        'type_name': 'PATENT',
        'column': 'title',
        'sql': 'SELECT title FROM patent WHERE title IS NOT NULL',
    },
    {
        'type_name': 'SPEAKER',
        'column': 'title',
        'sql': 'SELECT title FROM participation_events WHERE title IS NOT NULL',
    },
    {
        'type_name': 'ABSTRACT',
        'column': 'abstract',
        'sql': 'SELECT abstract FROM researcher WHERE abstract IS NOT NULL',
    },
]


def setup_database():
    print(
        "Verificando e configurando a tabela 'research_ngrams' no banco de dados..."
    )

    SCRIPT_SQL_TABLE = """
    CREATE TABLE IF NOT EXISTS research_ngrams (
        id SERIAL PRIMARY KEY,
        ngram TEXT NOT NULL,
        frequency INTEGER NOT NULL,
        n INTEGER NOT NULL,
        source_type VARCHAR(50) NOT NULL,
        stopwords_removed BOOLEAN NOT NULL
    );
    """
    SCRIPT_SQL_INDEX = """
    CREATE INDEX IF NOT EXISTS idx_research_ngrams_source
    ON research_ngrams(source_type, n, stopwords_removed);
    """

    conn.exec(SCRIPT_SQL_TABLE)
    conn.exec(SCRIPT_SQL_INDEX)
    print('Tabela pronta.')


def get_ngrams_from_texts(text_list, n, remove_stopwords, nlp_model):
    all_tokens = []
    for text in text_list:
        # Garante que o texto não seja nulo
        if not text:
            continue

        doc = nlp_model(str(text).lower())
        tokens = []
        for token in doc:
            if not token.is_punct and not token.is_space and token.is_alpha:
                if remove_stopwords and token.is_stop:
                    continue
                tokens.append(token.text)
        all_tokens.extend(tokens)

    ngrams = [
        tuple(all_tokens[i : i + n]) for i in range(len(all_tokens) - n + 1)
    ]

    return Counter(ngrams)


def process_and_insert_all_sources():
    try:
        print('Carregando o modelo de linguagem do Spacy (pt_core_news_lg)...')
        nlp = spacy.load('pt_core_news_lg')
        print('Modelo carregado.')
    except OSError:
        print("Erro: O modelo 'pt_core_news_lg' não foi encontrado.")
        print('Por favor, execute: python -m spacy download pt_core_news_lg')
        sys.exit(1)

    conn.exec('BEGIN;')
    try:  # noqa: PLR1702
        for source in SOURCES:
            source_type = source['type_name']
            print(f'\n--- Processando fonte: {source_type} ---')

            data = conn.select(source['sql'])
            if not data:
                print(
                    f"Nenhum dado encontrado para a fonte '{source_type}'. Pulando."
                )
                continue

            text_list = [item[source['column']] for item in data]

            for n in range(1, 6):
                for remove_stops in [False, True]:
                    stopword_status = (
                        'SEM stopwords' if remove_stops else 'COM stopwords'
                    )
                    print(f'  Calculando {n}-gramas {stopword_status}...')

                    ngram_counts = get_ngrams_from_texts(
                        text_list, n, remove_stops, nlp
                    )

                    if not ngram_counts:
                        print('    Nenhum n-grama gerado. Pulando inserção.')
                        continue

                    delete_sql = """
                        DELETE FROM research_ngrams
                        WHERE source_type = %(source_type)s
                          AND n = %(n)s
                          AND stopwords_removed = %(stopwords_removed)s
                    """
                    conn.exec(
                        delete_sql,
                        {
                            'source_type': source_type,
                            'n': n,
                            'stopwords_removed': remove_stops,
                        },
                    )

                    insert_sql = """
                        INSERT INTO research_ngrams (ngram, frequency, n, source_type, stopwords_removed)
                        VALUES (%(ngram)s, %(frequency)s, %(n)s, %(source_type)s, %(stopwords_removed)s);
                    """
                    records_to_insert = [
                        {
                            'ngram': ' '.join(ngram_tuple),
                            'frequency': count,
                            'n': n,
                            'source_type': source_type,
                            'stopwords_removed': remove_stops,
                        }
                        for ngram_tuple, count in ngram_counts.items()
                    ]

                    if hasattr(conn, 'executemany'):
                        conn.executemany(insert_sql, records_to_insert)
                    else:
                        for record in records_to_insert:
                            conn.exec(insert_sql, record)

                    print(
                        f'    {len(records_to_insert)} registros de {n}-gramas inseridos.'
                    )

        conn.exec('COMMIT;')
        print(
            '\nProcesso concluído com sucesso! Todas as alterações foram salvas.'
        )

    except Exception as e:
        print(f'\nOcorreu um erro: {e}')
        print('Revertendo todas as alterações (ROLLBACK)...')
        conn.exec('ROLLBACK;')
        raise


if __name__ == '__main__':
    setup_database()
    process_and_insert_all_sources()
