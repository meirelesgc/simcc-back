import pandas as pd
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from simcc.config import Settings
from simcc.repositories import conn

SETTINGS = Settings()

BATCH_SIZE = 20

ods = [
    'Erradicação da pobreza',
    'Fome zero e agricultura sustentável',
    'Saúde e bem-estar',
    'Educação de qualidade',
    'Igualdade de gênero',
    'Água potável e saneamento',
    'Energia limpa e acessível',
    'Trabalho decente e crescimento econômico',
    'Indústria, inovação e infraestrutura',
    'Redução das desigualdades',
    'Cidades e comunidades sustentáveis',
    'Consumo e produção responsáveis',
    'Ação contra a mudança global do clima',
    'Vida na água',
    'Vida terrestre',
    'Paz, justiça e instituições eficazes',
    'Parcerias e meios de implementação',
]

BASE_SQL = """
SELECT bp.id, bp.researcher_id, bp.title
FROM bibliographic_production bp
LEFT JOIN sdg_alignment ao
    ON ao.reference_id = bp.id
    AND ao.type = 'ARTICLE'
WHERE bp.type = 'ARTICLE'
AND bp.year::INT > 2017
AND ao.reference_id IS NULL
ORDER BY bp.id
LIMIT {limit} OFFSET {offset}
"""

MODEL = 'gpt-5.4-nano'

llm = ChatOpenAI(model=MODEL, temperature=0, api_key=SETTINGS.OPENAI_API_KEY)

prompt = ChatPromptTemplate.from_messages([
    (
        'system',
        'Você classifica artigos científicos de acordo com os Objetivos de Desenvolvimento Sustentável (ODS).',
    ),
    (
        'human',
        'ODS disponíveis:\n{ods}\n\nTítulo do artigo:\n{title}\n\nRetorne apenas 1 ODS mais aderente. Responda somente com o nome exato do ODS.',
    ),
])

chain = prompt | llm | StrOutputParser()

offset = 0
all_results = []

while True:
    SQL = BASE_SQL.format(limit=BATCH_SIZE, offset=offset)
    result = conn.select(SQL)
    articles = pd.DataFrame(result)

    if articles.empty:
        break

    inputs = [
        {'title': row['title'], 'ods': '\n'.join(ods)}
        for _, row in articles.iterrows()
    ]

    outputs = chain.batch(inputs, {'max_concurrency': 5})

    articles['ods_preditos'] = [o.strip() for o in outputs]
    all_results.append(articles)

    for _, row in articles.iterrows():
        reference_id = row['id']
        sdg_clean = row['ods_preditos']

        sql = f"""
        INSERT INTO sdg_alignment (reference_id, type, sdg)
        SELECT '{reference_id}', 'ARTICLE', '{sdg_clean}'
        WHERE NOT EXISTS (
            SELECT 1 FROM sdg_alignment
            WHERE reference_id = '{reference_id}'
            AND type = 'ARTICLE'
        );
        """

        conn.exec(sql)

    offset += BATCH_SIZE

final_df = (
    pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()
)
final_df.to_csv('resultado.csv', index=False)

print(final_df)
