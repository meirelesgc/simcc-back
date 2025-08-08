import pandas as pd
import unidecode

from simcc.repositories import conn_admin

researchers = pd.read_csv('storage/csv/simcc_program_researchers.csv')


for _, researcher in researchers.iterrows():
    normalized_name = (
        unidecode.unidecode(researcher['name']).replace(' ', '').lower()
    )
    researcher['name'] = normalized_name

    SCRIPT_SQL = """
        SELECT researcher_id AS id
        FROM researcher
        WHERE REPLACE(UNACCENT(name), ' ', '') ILIKE %(name)s
    """
    result = conn_admin.select(SCRIPT_SQL, researcher.to_dict(), one=True)

    if not result:
        print(f'Pesquisador não encontrado: {researcher["name"]}')
        continue
    researcher['id'] = result['id']
    researcher['year'] = [2025, 2024, 2023, 2022, 2021]
    SCRIPT_SQL = """
        INSERT INTO graduate_program_researcher
            (graduate_program_id, researcher_id, year, type_)
        SELECT gp.graduate_program_id, %(id)s, %(year)s, %(type_)s
        FROM graduate_program gp
        WHERE gp.code = %(code)s
        ON CONFLICT (graduate_program_id, researcher_id) DO UPDATE
        SET type_ = COALESCE(graduate_program_researcher.type_, EXCLUDED.type_);
    """
    conn_admin.exec(SCRIPT_SQL, researcher.to_dict())
    print(f'Registro processado: {result["id"]}')
