from langchain_ollama import ChatOllama, OllamaEmbeddings
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_postgres import PGVector

from simcc.config import Settings

MODEL_CONFIGS = {
    'internal': {
        'chat': lambda: ChatOllama(
            model='llama3.2:3b',
            temperature=0,
            base_url=Settings().OLLAMA_HOST,
        ),
        'embedding': lambda: OllamaEmbeddings(
            model='nomic-embed-text:latest',
            base_url=Settings().OLLAMA_HOST,
        ),
    },
    'external': {
        'chat': lambda: ChatOpenAI(
            model='gpt-4o',
            temperature=0,
            api_key=Settings().OPENAI_API_KEY,
        ),
        'embedding': lambda: OpenAIEmbeddings(
            model='text-embedding-3-large',
            api_key=Settings().OPENAI_API_KEY,
        ),
    },
}


def get_model(model_source: str = 'internal'):
    return MODEL_CONFIGS[model_source]['chat']()


def get_embedding_function(model_source: str = 'internal'):
    return MODEL_CONFIGS[model_source]['embedding']()


def get_vector_store(model_source: str = 'internal'):
    return PGVector(
        embeddings=get_embedding_function(model_source),
        collection_name='arrange',
        connection=Settings().get_connection_string_psycopg3(),
        use_jsonb=True,
    )
