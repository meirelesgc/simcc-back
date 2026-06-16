import pandas as pd
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

from simcc.core.config import Settings
from simcc.repositories import conn

SETTINGS = Settings()

BATCH_SIZE = 20

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS temp_researcher_city (
    researcher_name VARCHAR,
    institution_name VARCHAR,
    city VARCHAR
);
"""
conn.exec(CREATE_TABLE_SQL)

BASE_SQL = """
SELECT r.name AS researcher_name, i.name AS institution_name
FROM researcher r
LEFT JOIN institution i ON i.id = r.institution_id
WHERE i.acronym IS NOT NULL
ORDER BY r.name
LIMIT {limit} OFFSET {offset}
"""

MODEL = 'gpt-5.4-nano'
llm = ChatOpenAI(model=MODEL, temperature=0, api_key=SETTINGS.OPENAI_API_KEY)

prompt = ChatPromptTemplate.from_messages([
    (
        'system',
        'Você identifica a cidade de um pesquisador com base no seu nome e instituição.',
    ),
    (
        'human',
        'Pesquisador: {researcher_name}\nInstituição: {institution_name}\n\nRetorne apenas o nome da cidade. Caso não encontre a informação, retorne "Desconhecida".',
    ),
])

chain = prompt | llm | StrOutputParser()

offset = 0
all_results = []

while True:
    SQL = BASE_SQL.format(limit=BATCH_SIZE, offset=offset)
    result = conn.select(SQL)
    df = pd.DataFrame(result)

    if df.empty:
        break

    inputs = [
        {
            'researcher_name': row['researcher_name'],
            'institution_name': row['institution_name'],
        }
        for _, row in df.iterrows()
    ]

    outputs = chain.batch(inputs, {'max_concurrency': 5})

    df['city'] = [o.strip() for o in outputs]
    all_results.append(df)

    for _, row in df.iterrows():
        researcher_name = str(row['researcher_name']).replace("'", "''")
        institution_name = str(row['institution_name']).replace("'", "''")
        city = str(row['city']).replace("'", "''")

        sql_insert = f"""
        INSERT INTO temp_researcher_city (researcher_name, institution_name, city)
        VALUES ('{researcher_name}', '{institution_name}', '{city}');
        """
        conn.exec(sql_insert)

    offset += BATCH_SIZE

final_df = (
    pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()
)
final_df.to_csv('cidades_pesquisadores.csv', index=False)
