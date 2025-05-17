import ast
import unicodedata
import json
import pandas as pd

from simcc.repositories import conn_admin


def normalize_string(s):
    s = unicodedata.normalize('NFD', s).replace('\n', '')
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return s.lower()


def normalize_keys(d):
    def normalize(s):
        s = unicodedata.normalize('NFD', s)
        s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
        return s.lower()

    return {normalize(k): v for k, v in d.items()}


def get_institution_id(institution):
    SCRIPT_SQL = """
        SELECT institution_id FROM institution WHERE name ILIKE %(institution)s
        """
    institution_id = conn_admin.select(SCRIPT_SQL, {'institution': institution})
    if institution_id:
        return institution_id[0]['institution_id']
    return None


with open('storage/city.json', 'r', encoding='utf-8') as buffer:
    citys = json.load(buffer)

programs = pd.read_csv('storage/csv/002_programs.csv')
programs.columns = [normalize_string(col) for col in programs.columns]


programs['ies_nome'] = programs['ies_nome'].where(
    programs['ies_nome'].notnull(), None
)

SCRIPT_SQL = """
    INSERT INTO public.graduate_program (code, name, area, modality, type,
        institution_id, state, city, visible, site)
    VALUES (
        %(codigo)s, %(nome)s, %(area de avaliacao)s, %(modality)s, %(type)s,
        %(institution_id)s, %(state)s, %(city)s, %(visible)s, %(ies_url)s)
    ON CONFLICT (code) DO UPDATE SET
        name = EXCLUDED.name, area = EXCLUDED.area, modality = EXCLUDED.modality,
        type = EXCLUDED.type, institution_id = EXCLUDED.institution_id,
        state = EXCLUDED.state, city = EXCLUDED.city, site = EXCLUDED.site,
        visible = EXCLUDED.visible;
    """
erro = 0
for _, program in programs.iterrows():
    pg = program.to_dict()

    if pg['situacao'] == 'EM FUNCIONAMENTO':
        cs = str(pg['ies_municipio']).split(' - ')
        pg['city'], pg['state'] = cs if len(cs) == 2 else ('', '')
        if institution_id := get_institution_id(pg['ies_nome']):
            pg['institution_id'] = institution_id
        else:
            erro += 1
            continue

        _type = list()

        pg['visible'] = False
        for course in ast.literal_eval(pg['cursos']):
            _co = normalize_keys(course)

            if _co['situacao'] == 'EM FUNCIONAMENTO':
                pg['visible'] = True

            nota = normalize_string(_co['nota'])
            nivel = normalize_string(_co['nivel'])
            modality = f'{nota}|{nivel}'

            if 'profissional' in modality:
                pg['modality'] = 'PROFISSIONAL'
            else:
                pg['modality'] = 'ACADÊMICO'

            _type.append(_co['nivel'])

        pg['type'] = '/'.join(_type)
        conn_admin.exec(SCRIPT_SQL, pg)
    else:
        erro += 1

print(erro)
SCRIPT_SQL = """
    UPDATE graduate_program SET
        name = INITCAP(name),
        area = UPPER(area)
    """

conn_admin.exec(SCRIPT_SQL)
