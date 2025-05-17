import unicodedata

import pandas as pd
import psycopg
from numpy import nan

from simcc.repositories import conn


def normalize_string(s):
    s = unicodedata.normalize('NFD', s).replace('\n', '')
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return s.lower()


def get_graduate_program_id(code):
    SCRIPT_SQL = """
        SELECT graduate_program_id
        FROM graduate_program
        WHERE code = %(code)s
        """
    result = conn.select(SCRIPT_SQL, {'code': code}, one=True)
    if result:
        return result['graduate_program_id']
    return None


lines = pd.read_csv('storage/csv/002_program_research_lines.csv')
lines.columns = [normalize_string(col) for col in lines.columns]

lines['start_year'] = pd.to_datetime(
    lines['data de inicio'], format='%d/%m/%Y', errors='coerce'
).dt.year

lines['end_year'] = pd.to_datetime(
    lines['data de fim'], format='%d/%m/%Y', errors='coerce'
).dt.year

lines['start_year'] = pd.to_datetime(
    lines['data de inicio'], format='%d/%m/%Y', errors='coerce'
).dt.year
lines['end_year'] = pd.to_datetime(
    lines['data de fim'], format='%d/%m/%Y', errors='coerce'
).dt.year

lines['start_year'] = lines['start_year'].replace(nan, None)
lines['end_year'] = lines['end_year'].replace(nan, None)


SCRIPT_SQL = """
    INSERT INTO public.research_lines_programs
        (graduate_program_id, name, area, start_year, end_year)
    VALUES
        (%(graduate_program_id)s, %(nome)s, %(area de concentracao)s,
        %(start_year)s, %(end_year)s);
    """
for _, line in lines.iterrows():
    graduate_program_id = get_graduate_program_id(line['codigo do programa'])
    if graduate_program_id:
        params = line.to_dict()
        params['graduate_program_id'] = graduate_program_id
        try:
            conn.exec(SCRIPT_SQL, params)
        except psycopg.errors.UniqueViolation:
            print('UniqueViolation')

SCRIPT_SQL = """
    UPDATE research_lines_programs SET
        name = INITCAP(name),
        area = UPPER(name);
    """
conn.exec(SCRIPT_SQL)
