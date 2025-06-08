import sys
from collections import Counter

import spacy

from simcc.repositories import conn

# --- CONFIGURAÇÃO DAS FONTES DE DADOS ---
# Lista de dicionários para configurar as fontes de texto a serem processadas.
# 'type_name': O nome que será salvo na tabela.
# 'column': A coluna que contém o texto.
# 'sql': A query para extrair os dados.
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
    """
    Cria a tabela 'research_ngrams' se ela não existir.
    Esta tabela armazenará todos os n-gramas calculados.
    """
    print(
        "Verificando e configurando a tabela 'research_ngrams' no banco de dados..."
    )

    # Adicionado um índice para otimizar as operações de DELETE.
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
    """
    Extrai e conta n-gramas de uma lista de textos.

    Args:
        text_list (list): Lista de strings para processar.
        n (int): O tamanho do n-grama (ex: 2 para bigramas).
        remove_stopwords (bool): Se True, remove as stopwords.
        nlp_model: O modelo Spacy carregado.

    Returns:
        Counter: Um objeto Counter com os n-gramas e suas frequências.
    """
    all_tokens = []
    for text in text_list:
        # Garante que o texto não seja nulo
        if not text:
            continue

        doc = nlp_model(str(text).lower())
        tokens = []
        for token in doc:
            # Filtra pontuação, espaços e tokens não alfabéticos
            if not token.is_punct and not token.is_space and token.is_alpha:
                if remove_stopwords and token.is_stop:
                    continue
                tokens.append(token.text)
        all_tokens.extend(tokens)

    # Gera os n-gramas a partir da lista de tokens limpos
    ngrams = [
        tuple(all_tokens[i : i + n]) for i in range(len(all_tokens) - n + 1)
    ]

    return Counter(ngrams)


def process_and_insert_all_sources():
    """
    Função principal que orquestra todo o processo:
    1. Carrega o modelo Spacy.w
    2. Itera sobre todas as fontes de dados definidas em SOURCES.
    3. Para cada fonte, calcula n-gramas (1 a 5, com/sem stopwords).
    4. Insere os resultados no banco de dados.
    """
    try:
        print('Carregando o modelo de linguagem do Spacy (pt_core_news_lg)...')
        nlp = spacy.load('pt_core_news_lg')
        print('Modelo carregado.')
    except OSError:
        print("Erro: O modelo 'pt_core_news_lg' não foi encontrado.")
        print('Por favor, execute: python -m spacy download pt_core_news_lg')
        sys.exit(1)

    # Inicia uma transação para garantir a integridade dos dados
    conn.exec('BEGIN;')
    try:  # noqa: PLR1702
        for source in SOURCES:
            source_type = source['type_name']
            print(f'\n--- Processando fonte: {source_type} ---')

            # 1. Coleta os dados do banco
            data = conn.select(source['sql'])
            if not data:
                print(
                    f"Nenhum dado encontrado para a fonte '{source_type}'. Pulando."
                )
                continue

            text_list = [item[source['column']] for item in data]

            # 2. Itera para cada tamanho de n-grama (de 1 a 5)
            for n in range(1, 6):
                # 3. Itera para cada opção de stopwords (com e sem)
                for remove_stops in [False, True]:
                    stopword_status = (
                        'SEM stopwords' if remove_stops else 'COM stopwords'
                    )
                    print(f'  Calculando {n}-gramas {stopword_status}...')

                    # Calcula os n-gramas
                    ngram_counts = get_ngrams_from_texts(
                        text_list, n, remove_stops, nlp
                    )

                    if not ngram_counts:
                        print('    Nenhum n-grama gerado. Pulando inserção.')
                        continue

                    # 4. Deleta os dados antigos para esta combinação específica
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

                    # 5. Prepara os dados para inserção em lote
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

                    # A função conn.executemany é hipotética, mas representa
                    # uma inserção em lote. Se seu `conn` não tiver,
                    # um laço `for record in records_to_insert: conn.exec(insert_sql, record)` funcionaria,
                    # mas seria mais lento. A maioria dos conectores de DB (psycopg2, etc.) tem essa funcionalidade.
                    if hasattr(conn, 'executemany'):
                        conn.executemany(insert_sql, records_to_insert)
                    else:  # Fallback para inserção linha a linha
                        for record in records_to_insert:
                            conn.exec(insert_sql, record)

                    print(
                        f'    {len(records_to_insert)} registros de {n}-gramas inseridos.'
                    )

        # Se tudo correu bem, confirma as alterações
        conn.exec('COMMIT;')
        print(
            '\nProcesso concluído com sucesso! Todas as alterações foram salvas.'
        )

    except Exception as e:
        # Em caso de qualquer erro, desfaz todas as alterações
        print(f'\nOcorreu um erro: {e}')
        print('Revertendo todas as alterações (ROLLBACK)...')
        conn.exec('ROLLBACK;')
        # Re-levanta a exceção para que o erro completo seja visível
        raise


if __name__ == '__main__':
    # 1. Garante que a tabela de destino exista
    setup_database()

    # 2. Executa o processo principal de extração e inserção
    process_and_insert_all_sources()
