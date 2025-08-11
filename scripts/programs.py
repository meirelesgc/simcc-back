import ast
import json
import unicodedata
from datetime import datetime

import pandas as pd

from simcc.repositories import conn_admin


def extract_data(text):
    if isinstance(text, list):
        text = text[0]
    parts = text.split('|')
    key_values = [p.strip() for p in parts if p.strip()]
    return {
        key_values[i]: key_values[i + 1] for i in range(0, len(key_values), 2)
    }


def normalize_string(s):
    s = unicodedata.normalize('NFD', s).replace('\n', '')
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return s.lower()


def normalize_keys(d):
    return {normalize_string(k): v for k, v in d.items()}


def get_institution_id(name):
    sql = (
        'SELECT institution_id FROM institution WHERE name ILIKE %(institution)s'
    )
    result = conn_admin.select(sql, {'institution': name})
    return result[0]['institution_id'] if result else None


def load_city_data(path='storage/city.json'):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_programs_csv(path='storage/csv/002_programs.csv'):
    df = pd.read_csv(path)
    df.columns = [normalize_string(col) for col in df.columns]
    df['ies_nome'] = df['ies_nome'].where(df['ies_nome'].notnull(), None)
    return df


def process_program_row(pg):
    if pg['situacao'] != 'EM FUNCIONAMENTO':
        return None

    cs = str(pg['ies_municipio']).split(' - ')
    pg['city'], pg['state'] = cs if len(cs) == 2 else ('', '')

    institution_id = get_institution_id(pg['ies_nome'])
    if not institution_id:
        return None

    pg['institution_id'] = institution_id
    pg['visible'] = False
    types = []

    for course in ast.literal_eval(pg['cursos']):
        course_data = normalize_keys(course)
        if course_data.get('situacao') == 'em funcionamento':
            pg['visible'] = True

        modality = f'{normalize_string(course_data.get("nota", ""))}|{normalize_string(course_data.get("nivel", ""))}'
        pg['modality'] = (
            'PROFISSIONAL' if 'profissional' in modality else 'ACADÊMICO'
        )
        types.append(course_data.get('nivel', ''))

    pg['type'] = '/'.join(types)
    pg['name_en'] = pg.get('nome ingles')
    pg['basic_area'] = pg.get('area basica')
    pg['cooperation_project'] = pg.get('projetos_coop')
    pg['coordinator'] = pg.get('coordenador')
    pg['email'] = pg.get('ies_email')

    try:
        pg['start'] = datetime.strptime(
            pg.get('ies_inicio', ''), '%d/%m/%Y'
        ).date()
    except Exception:
        pg['start'] = None

    try:
        pg['phone'] = ''.join(
            c for c in pg.get('ies_telefones', '') if c.isdigit()
        )
    except Exception:
        pg['phone'] = None

    try:
        regimes = ast.literal_eval(pg.get('regime_letivo', '[]'))
        pg['periodicity'] = '/'.join(
            set(item.get('Nome', '') for item in regimes)
        )
    except Exception:
        pg['periodicity'] = ''

    return pg


def insert_or_update_program(pg):
    SCRIPT_SQL = """
        INSERT INTO public.graduate_program (code, name, name_en, basic_area,
            cooperation_project, area, modality, type, institution_id, state,
            city, visible, site, coordinator, email, "start", phone, periodicity)
        VALUES (%(codigo)s, %(nome)s, %(name_en)s, %(basic_area)s,
            %(cooperation_project)s, %(area de avaliacao)s, %(modality)s,
            %(type)s, %(institution_id)s, %(state)s, %(city)s, %(visible)s, %(ies_url)s,
            %(coordinator)s, %(email)s, %(start)s, %(phone)s, %(periodicity)s)
        ON CONFLICT (code) DO UPDATE SET
            name = COALESCE(public.graduate_program.name, EXCLUDED.name),
            name_en = COALESCE(public.graduate_program.name_en, EXCLUDED.name_en),
            basic_area = COALESCE(public.graduate_program.basic_area, EXCLUDED.basic_area),
            cooperation_project = COALESCE(public.graduate_program.cooperation_project, EXCLUDED.cooperation_project),
            area = COALESCE(public.graduate_program.area, EXCLUDED.area),
            modality = COALESCE(public.graduate_program.modality, EXCLUDED.modality),
            type = COALESCE(public.graduate_program.type, EXCLUDED.type),
            institution_id = COALESCE(public.graduate_program.institution_id, EXCLUDED.institution_id),
            state = COALESCE(public.graduate_program.state, EXCLUDED.state),
            city = COALESCE(public.graduate_program.city, EXCLUDED.city),
            visible = COALESCE(public.graduate_program.visible, EXCLUDED.visible),
            site = COALESCE(public.graduate_program.site, EXCLUDED.site),
            coordinator = COALESCE(public.graduate_program.coordinator, EXCLUDED.coordinator),
            email = COALESCE(public.graduate_program.email, EXCLUDED.email),
            "start" = COALESCE(public.graduate_program."start", EXCLUDED."start"),
            phone = COALESCE(public.graduate_program.phone, EXCLUDED.phone),
            periodicity = COALESCE(public.graduate_program.periodicity, EXCLUDED.periodicity);
            """
    conn_admin.exec(SCRIPT_SQL, pg)


def format_program_names():
    sql = """
        UPDATE graduate_program SET
            name = INITCAP(name),
            area = UPPER(area)
    """
    conn_admin.exec(sql)


def main():
    erro = 0
    programs = load_programs_csv()

    for _, row in programs.iterrows():
        program = process_program_row(row.to_dict())
        if program:
            insert_or_update_program(program)
        else:
            erro += 1

    print(f'Erros: {erro}')
    format_program_names()


if __name__ == '__main__':
    main()
