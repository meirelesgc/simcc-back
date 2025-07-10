from langchain_openai import ChatOpenAI

from simcc.config import Settings

model = ChatOpenAI(model='gpt-4o-mini', api_key=Settings().OPENAI_API_KEY)


async def get_model():
    return model
