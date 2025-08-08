import pandas as pd

from simcc.repositories import conn_admin

programs = pd.read_csv('storage/csv/simcc_programs.csv')


def get_institution_id(program):
    SCRIPT_SQL = """
        SELECT institution_id AS id
        FROM institution
        WHERE acronym = %(siglaIes)s;
    """
    params = program.to_dict()
    result = conn_admin.select(SCRIPT_SQL, params, one=True)
    return result['id'] if result else '89858fed-23e6-417c-b978-2acf2fb843cc'


programs['institution_id'] = programs.apply(get_institution_id, axis=1)
programs['visible'] = programs['situação'].eq('EM FUNCIONAMENTO')

for _, data in programs.iterrows():
    SCRIPT_SQL = """
        INSERT INTO graduate_program
        (code, name, area, modality, type, rating, institution_id, visible, city)
        VALUES
        (%(código)s, %(nome)s, %(nomeAreaAvaliacao)s, %(modalidade)s, %(grau)s,
         %(conceito)s, %(institution_id)s, %(visible)s, %(cidade)s)
        ON CONFLICT (name, institution_id) DO UPDATE
        SET name          = COALESCE(graduate_program.name, EXCLUDED.name),
            area          = COALESCE(graduate_program.area, EXCLUDED.area),
            modality      = COALESCE(graduate_program.modality, EXCLUDED.modality),
            type          = COALESCE(graduate_program.type, EXCLUDED.type),
            rating        = COALESCE(graduate_program.rating, EXCLUDED.rating),
            visible       = COALESCE(graduate_program.visible, EXCLUDED.visible),
            institution_id= COALESCE(graduate_program.institution_id, EXCLUDED.institution_id),
            city          = COALESCE(graduate_program.city, EXCLUDED.city);
    """
    conn_admin.exec(SCRIPT_SQL, data.to_dict())
    print(f'Registro processado: {_}')
