import ast
import json
import unicodedata
import logging
from datetime import datetime
from collections import Counter
from functools import lru_cache

import pandas as pd
from simcc.repositories import conn_admin

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_data(text):
    if isinstance(text, list):
        text = text[0]
    parts = text.split('|')
    key_values = [p.strip() for p in parts if p.strip()]
    return {key_values[i]: key_values[i + 1] for i in range(0, len(key_values), 2)}

def normalize_string(s):
    if not isinstance(s, str):
        return str(s) if s is not None else ""
    s = unicodedata.normalize('NFD', s).replace('\n', '')
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return s.lower()

def normalize_keys(d):
    return {normalize_string(k): v for k, v in d.items()}

@lru_cache(maxsize=1024)
def get_institution_id(name):
    try:
        if not name:
            return None
        sql = 'SELECT institution_id FROM institution WHERE name ILIKE %(institution)s'
        result = conn_admin.select(sql, {'institution': name})
        return result[0]['institution_id'] if result else None
    except Exception as e:
        logger.error(f"Erro ao buscar instituição '{name}': {e}")
        return None

def load_city_data(path='storage/city.json'):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Arquivo de cidades não encontrado: {path}")
        return {}

def load_programs_csv(path='storage/csv/002_programs.csv'):
    logger.info(f"Carregando CSV: {path}")
    df = pd.read_csv(path)
    df.columns = [normalize_string(col) for col in df.columns]
    df['ies_nome'] = df['ies_nome'].where(df['ies_nome'].notnull(), None)
    return df

def _program_ident(pg):
    codigo = pg.get('codigo')
    nome = pg.get('nome')
    if codigo and nome:
        return f"codigo={codigo} nome={nome}"
    if codigo:
        return f"codigo={codigo}"
    if nome:
        return f"nome={nome}"
    return "codigo=<sem_codigo> nome=<sem_nome>"

def process_program_row(pg):
    if pg.get('situacao') != 'EM FUNCIONAMENTO':
        return None, "Ignorado: Situação não é 'EM FUNCIONAMENTO'"

    try:
        municipio_raw = str(pg.get('ies_municipio', ''))
        cs = municipio_raw.split(' - ')
        pg['city'], pg['state'] = cs if len(cs) == 2 else ('', '')
    except Exception:
        pg['city'], pg['state'] = ('', '')

    institution_id = get_institution_id(pg['ies_nome'])
    if not institution_id:
        return None, f"Erro: Instituição não encontrada no banco ({pg.get('ies_nome')})"

    pg['institution_id'] = institution_id
    pg['visible'] = False
    types = []

    try:
        cursos_raw = pg.get('cursos', '[]')
        if pd.isna(cursos_raw):
            cursos_list = []
        else:
            cursos_list = ast.literal_eval(str(cursos_raw))

        for course in cursos_list:
            course_data = normalize_keys(course)
            if course_data.get('situacao') == 'em funcionamento':
                pg['visible'] = True

            modality = f'{normalize_string(course_data.get("nota", ""))}|{normalize_string(course_data.get("nivel", ""))}'
            pg['modality'] = 'PROFISSIONAL' if 'profissional' in modality else 'ACADÊMICO'
            types.append(course_data.get('nivel', ''))
    except (ValueError, SyntaxError) as e:
        return None, f"Erro: Falha ao fazer parse da coluna 'cursos': {str(e)}"

    pg['type'] = '/'.join(types)
    pg['name_en'] = pg.get('nome ingles')
    pg['basic_area'] = pg.get('area basica')
    pg['cooperation_project'] = pg.get('projetos_coop')
    pg['coordinator'] = pg.get('coordenador')
    pg['email'] = pg.get('ies_email')

    try:
        raw_start = pg.get('ies_inicio', '')
        if raw_start and isinstance(raw_start, str):
            pg['start'] = datetime.strptime(raw_start, '%d/%m/%Y').date()
        else:
            pg['start'] = None
    except ValueError:
        pg['start'] = None

    try:
        pg['phone'] = ''.join(c for c in str(pg.get('ies_telefones', '')) if c.isdigit())
    except Exception:
        pg['phone'] = None

    try:
        regime_raw = pg.get('regime_letivo', '[]')
        if pd.isna(regime_raw):
            regimes = []
        else:
            regimes = ast.literal_eval(str(regime_raw))

        pg['periodicity'] = '/'.join(set(item.get('Nome', '') for item in regimes if isinstance(item, dict)))
    except Exception:
        pg['periodicity'] = ''

    return pg, None

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
    try:
        conn_admin.exec(SCRIPT_SQL, pg)
    except Exception as e:
        logger.error(f"Erro de SQL ao inserir programa codigo={pg.get('codigo')} nome={pg.get('nome')}: {e}")
        raise e

def format_program_names():
    logger.info("Formatando nomes e áreas no banco de dados...")
    sql = """
        UPDATE graduate_program SET
            name = INITCAP(name),
            area = UPPER(area)
    """
    conn_admin.exec(sql)

def main():
    logger.info("Iniciando processamento...")

    try:
        programs = load_programs_csv()
    except Exception as e:
        logger.critical(f"Falha fatal ao carregar CSV: {e}")
        return

    stats = Counter()
    total_rows = len(programs)

    logger.info(f"Total de registros a processar: {total_rows}")

    for index, row in programs.iterrows():
        if index > 0 and index % 100 == 0:
            print(f"Processando linha {index}/{total_rows}...", end='\r')

        row_dict = row.to_dict()
        program, error_reason = process_program_row(row_dict)

        if program:
            try:
                insert_or_update_program(program)
                stats['Sucesso'] += 1
            except Exception:
                stats['Erro de Banco de Dados'] += 1
        else:
            stats[error_reason] += 1
            ident = _program_ident(row_dict)
            logger.warning(f"PG_BARRADO {ident} motivo={error_reason}")

    print("\n" + "=" * 40)
    print("RELATÓRIO FINAL DE PROCESSAMENTO")
    print("=" * 40)
    print(f"Total Processado: {total_rows}")
    print(f"Sucessos: {stats['Sucesso']}")
    print("-" * 40)
    print("DETALHE DAS ANORMALIDADES (Agrupado):")

    detalhes = {k: v for k, v in stats.items() if k != 'Sucesso'}

    if not detalhes:
        print("Nenhuma anormalidade detectada.")
    else:
        for motivo, count in detalhes.items():
            print(f" -> {count:5d} ocorrências: {motivo}")

    print("=" * 40)

    format_program_names()
    logger.info("Processamento concluído.")

if __name__ == '__main__':
    main()
