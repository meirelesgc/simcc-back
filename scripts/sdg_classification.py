import pandas as pd
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

from simcc.core.config import Settings
from simcc.repositories import conn

SETTINGS = Settings()

BATCH_SIZE = 20

ods = {
    '1': '1 - Erradicação da pobreza',
    '2': '2 - Fome zero e agricultura sustentável',
    '3': '3 - Saúde e bem-estar',
    '4': '4 - Educação de qualidade',
    '5': '5 - Igualdade de gênero',
    '6': '6 - Água potável e saneamento',
    '7': '7 - Energia limpa e acessível',
    '8': '8 - Trabalho decente e crescimento econômico',
    '9': '9 - Indústria, inovação e infraestrutura',
    '10': '10 - Redução das desigualdades',
    '11': '11 - Cidades e comunidades sustentáveis',
    '12': '12 - Consumo e produção responsáveis',
    '13': '13 - Ação contra a mudança global do clima',
    '14': '14 - Vida na água',
    '15': '15 - Vida terrestre',
    '16': '16 - Paz, justiça e instituições eficazes',
    '17': '17 - Parcerias e meios de implementação',
}

ods_text = '\n'.join(ods.values())

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
        'ODS disponíveis:\n{ods}\n\nTítulo do artigo:\n{title}\n\nRetorne apenas 1 ODS. Responda somente com o número do ODS (1 a 17).',
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
        {'title': row['title'], 'ods': ods_text}
        for _, row in articles.iterrows()
    ]

    outputs = chain.batch(inputs, {'max_concurrency': 5})

    articles['ods_predito_id'] = [o.strip() for o in outputs]
    all_results.append(articles)

    for _, row in articles.iterrows():
        reference_id = row['id']
        sdg_id = row['ods_predito_id']

        if sdg_id.isdigit():
            sql = f"""
            INSERT INTO sdg_alignment (reference_id, type, sdg_id)
            SELECT '{reference_id}', 'ARTICLE', (SELECT id FROM public.sdg WHERE number = {sdg_id})
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
