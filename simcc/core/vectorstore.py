from langchain_openai import OpenAIEmbeddings
from langchain_postgres import PGVector

from simcc.config import Settings

vectorstore = PGVector(
    embeddings=OpenAIEmbeddings(
        model='text-embedding-3-small',
        api_key=Settings().OPENAI_API_KEY,
    ),
    connection='postgresql+' + Settings().DATABASE_URL,
    use_jsonb=True,
    async_mode=True,
)


async def get_vectorstore():
    yield vectorstore
