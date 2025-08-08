import pandas as pd
import unidecode

from simcc.repositories import conn_admin

researchers = pd.read_csv('storage/csv/simcc_program_researchers.csv')

for _, researcher in researchers.iterrows():
    normalized_name = (
        unidecode.unidecode(researcher['name']).replace(' ', '').lower()
    )
    researcher['name'] = normalized_name

    sql_find_researcher = """
        SELECT researcher_id AS id 
        FROM researcher
        WHERE REPLACE(UNACCENT(name), ' ', '') ILIKE %(name)s
    """
    researcher_data = researcher.to_dict()
    db_data = conn_admin.select(sql_find_researcher, researcher_data, one=True)

    if not db_data:
        print(f'Pesquisador não encontrado: {researcher["name"]}')
        continue

    researcher_data.update(db_data)

    sql_upsert = """
        INSERT INTO graduate_program_researcher
            (graduate_program_id, researcher_id, year, type_)
        SELECT gp.graduate_program_id, %(id)s, %(year)s, %(type_)s
        FROM graduate_program gp
        WHERE gp.code = %(code)s
        ON CONFLICT (graduate_program_id, researcher_id, year) DO UPDATE
        SET type_ = COALESCE(graduate_program_researcher.type_, EXCLUDED.type_);
    """
    conn_admin.exec(sql_upsert, researcher_data)
    print(
        f'Registro processado: {researcher_data["id"]} - Ano {researcher_data["year"]}'
    )
