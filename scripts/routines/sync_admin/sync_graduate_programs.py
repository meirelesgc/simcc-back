import ast
import time
import unicodedata
from collections import Counter
from datetime import datetime

import polars as pl
from sqlalchemy import text

from simcc.core.db.database import get_admin_sync_session


def normalize_string(s):
    if not isinstance(s, str):
        return str(s) if s is not None else ''
    s = unicodedata.normalize('NFD', s).replace('\n', '')
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return s.lower()


def normalize_keys(d):
    return {normalize_string(k): v for k, v in d.items()}


def get_institutions_mapping(session):
    sql = text(
        'SELECT institution_id::TEXT, name FROM institution WHERE name IS NOT NULL'
    )
    result = session.execute(sql).mappings().all()
    return {
        normalize_string(row['name']): row['institution_id'] for row in result
    }


def load_programs_csv(path='storage/seed/programs.csv'):
    df = pl.read_csv(path)
    df = df.rename({col: normalize_string(col) for col in df.columns})
    return df


def process_program_row(pg, institutions_map):
    if pg.get('situacao') != 'EM FUNCIONAMENTO':
        return None, "Ignorado: Situação não é 'EM FUNCIONAMENTO'"

    try:
        municipio_raw = str(pg.get('ies_municipio', ''))
        cs = municipio_raw.split(' - ')
        city, state = cs if len(cs) == 2 else ('', '')
    except Exception:
        city, state = ('', '')

    ies_nome_norm = normalize_string(pg.get('ies_nome'))
    institution_id = institutions_map.get(ies_nome_norm)

    if not institution_id:
        return (
            None,
            f'Erro: Instituição não encontrada no banco ({pg.get("ies_nome")})',
        )

    visible = False
    types = []

    try:
        cursos_raw = pg.get('cursos')
        if cursos_raw is None:
            cursos_list = []
        else:
            cursos_list = ast.literal_eval(str(cursos_raw))

        for course in cursos_list:
            course_data = normalize_keys(course)
            if course_data.get('situacao') == 'em funcionamento':
                visible = True

            modality = f'{normalize_string(course_data.get("nota", ""))}|{normalize_string(course_data.get("nivel", ""))}'
            program_modality = (
                'PROFISSIONAL' if 'profissional' in modality else 'ACADÊMICO'
            )
            types.append(course_data.get('nivel', ''))
    except (ValueError, SyntaxError) as e:
        return None, f"Erro: Falha ao fazer parse da coluna 'cursos': {str(e)}"

    program_type = '/'.join(types)

    try:
        raw_start = pg.get('ies_inicio', '')
        if raw_start and isinstance(raw_start, str):
            start_date = datetime.strptime(raw_start, '%d/%m/%Y').date()
        else:
            start_date = None
    except ValueError:
        start_date = None

    try:
        phone = ''.join(
            c for c in str(pg.get('ies_telefones', '')) if c.isdigit()
        )
    except Exception:
        phone = None

    try:
        regime_raw = pg.get('regime_letivo')
        if regime_raw is None:
            regimes = []
        else:
            regimes = ast.literal_eval(str(regime_raw))

        periodicity = '/'.join(
            set(
                item.get('Nome', '')
                for item in regimes
                if isinstance(item, dict)
            )
        )
    except Exception:
        periodicity = ''

    return {
        'code': pg.get('codigo'),
        'name': pg.get('nome'),
        'name_en': pg.get('nome ingles'),
        'basic_area': pg.get('area basica'),
        'cooperation_project': pg.get('projetos_coop'),
        'area': pg.get('area de avaliacao'),
        'modality': program_modality,
        'type': program_type,
        'institution_id': institution_id,
        'state': state,
        'city': city,
        'visible': visible,
        'site': pg.get('ies_url'),
        'coordinator': pg.get('coordenador'),
        'email': pg.get('ies_email'),
        'start': start_date,
        'phone': phone,
        'periodicity': periodicity,
    }, None


def format_program_names(session):
    sql = text("""
        UPDATE graduate_program SET
            name = INITCAP(name),
            area = UPPER(area)
    """)
    session.execute(sql)


items_found = 0
items_succeeded = 0
items_failed = 0


def main():
    global items_found, items_succeeded, items_failed
    session = next(get_admin_sync_session())
    start_time = time.perf_counter()

    try:
        programs_df = load_programs_csv()
        institutions_map = get_institutions_mapping(session)

        stats = Counter()
        total_rows = len(programs_df)
        items_found = total_rows
        valid_programs = []

        for row_dict in programs_df.to_dicts():
            program_data, error_reason = process_program_row(
                row_dict, institutions_map
            )

            if program_data:
                valid_programs.append(program_data)
                stats['Sucesso'] += 1
            else:
                stats[error_reason] += 1
                
        items_succeeded = stats['Sucesso']
        items_failed = total_rows - items_succeeded

        if valid_programs:
            query_upsert = text("""
                INSERT INTO public.graduate_program (
                    code, name, name_en, basic_area, cooperation_project, area, modality, 
                    type, institution_id, state, city, visible, site, coordinator, email, "start", phone, periodicity
                ) VALUES (
                    :code, :name, :name_en, :basic_area, :cooperation_project, :area, :modality,
                    :type, :institution_id, :state, :city, :visible, :site, :coordinator, :email, :start, :phone, :periodicity
                ) ON CONFLICT (code) DO UPDATE SET
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
            """)

            session.execute(query_upsert, valid_programs)
            format_program_names(session)

        session.commit()
        duration = time.perf_counter() - start_time
    except Exception as E:
        print(E)
        session.rollback()
        duration = time.perf_counter() - start_time


if __name__ == '__main__':
    main()
