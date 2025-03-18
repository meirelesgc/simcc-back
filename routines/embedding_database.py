from simcc.repositories import conn


def embedding_abstract():
    SCRIPT_SQL = """
        SELECT id, abstract
        FROM researcher
        WHERE id NOT IN (SELECT reference_id FROM embeddings.abstract);
        """
    result = conn.select(SCRIPT_SQL)
    for abstract in result:
        print(f'Abstract: {abstract}')


def embedding_article(): ...
def embedding_article_abstract(): ...
def embedding_book(): ...
def embedding_event(): ...
def embedding_patent(): ...


if __name__ == '__main__':
    embedding_abstract()
