import pandas as pd
import unicodedata
from psycopg2.errors import UniqueViolation

from simcc.repositories import conn_admin


def get_researcher_id(researcher_name):
    SCRIPT_SQL = """
        SELECT researcher_id
        FROM researcher
        WHERE
            regexp_replace(unaccent(upper(name)),'[^A-Z0-9]','','g')
            =
            regexp_replace(unaccent(upper(%(name)s)),'[^A-Z0-9]','','g')
        """
    researcher = conn_admin.select(SCRIPT_SQL, {'name': researcher_name})
    if researcher:
        return researcher[0]['researcher_id']
    return None


def normalize_string(s):
    s = unicodedata.normalize('NFD', s).replace('\n', '')
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return s.lower()


def get_program_id(code):
    SCRIPT_SQL = """
        SELECT graduate_program_id
        FROM graduate_program
        WHERE code = %(code)s
        """
    program = conn_admin.select(SCRIPT_SQL, {'code': code})
    if program:
        return program[0]['graduate_program_id']
    return None


researchers = pd.read_csv('storage/csv/002_program_researchers.csv')
researchers.columns = [normalize_string(col) for col in researchers.columns]

SCRIPT_SQL = """
    INSERT INTO public.graduate_program_researcher
        (graduate_program_id, researcher_id, year, type_)
    VALUES
        (%(pg_id)s, %(r_id)s, ARRAY[2025, 2024, 2023, 2022, 2021], %(categoria)s);
    """

for _, researcher in researchers.iterrows():
    r_id = get_researcher_id(researcher['nome'])
    pg_id = get_program_id(researcher['id do programa'])
    if r_id and pg_id:
        if 'PERMANENTE' in researcher['categoria']:
            categoria = 'PERMANENTE'
        else:
            categoria = 'COLABORADOR'
        params = {'pg_id': pg_id, 'r_id': r_id, 'categoria': categoria}
        try:
            conn_admin.exec(SCRIPT_SQL, params)
        except UniqueViolation:
            print('Duplicado')
