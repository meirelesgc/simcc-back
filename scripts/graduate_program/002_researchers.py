import re
import unicodedata
from collections import Counter

import pandas as pd

from simcc.repositories import conn_admin


def normalize_string(s):
    if not isinstance(s, str):
        return str(s) if s is not None else ''
    s = unicodedata.normalize('NFD', s).replace('\n', '')
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return s.lower()


def normalize_name_match(name):
    if not isinstance(name, str):
        return ''
    name = unicodedata.normalize('NFD', name)
    name = name.encode('ascii', 'ignore').decode('utf-8').upper()
    return re.sub(r'[^A-Z0-9]', '', name)


def normalize_categoria(value):
    v = str(value) if value is not None else ''
    return 'PERMANENTE' if 'PERMANENTE' in v.upper() else 'COLABORADOR'


def main():
    path = 'storage/csv/002_program_researchers.csv'
    print(f'Carregando CSV: {path}')

    try:
        df = pd.read_csv(path)
    except Exception as e:
        print(f'Falha ao ler CSV: {e}')
        return

    df.columns = [normalize_string(col) for col in df.columns]
    total = len(df)
    print(f'Total de registros no CSV: {total}')

    required_cols = {'nome', 'id do programa', 'categoria'}
    missing = required_cols - set(df.columns)
    if missing:
        print(f'CSV sem colunas obrigatorias: {sorted(missing)}')
        return

    print('Carregando dados do banco para merge...')

    res_db = conn_admin.select('SELECT researcher_id, name FROM researcher', {})
    df_res = pd.DataFrame(res_db)
    if not df_res.empty:
        df_res['match_name'] = df_res['name'].apply(normalize_name_match)
        df_res = df_res.drop_duplicates(subset=['match_name'])

    prog_db = conn_admin.select(
        'SELECT graduate_program_id, code FROM graduate_program', {}
    )
    df_prog = pd.DataFrame(prog_db)

    print('Processando colunas do CSV e realizando merges...')

    df['match_name'] = df['nome'].apply(normalize_name_match)
    df['categoria'] = df['categoria'].apply(normalize_categoria)

    if not df_res.empty:
        df = df.merge(
            df_res[['researcher_id', 'match_name']], on='match_name', how='left'
        )
    else:
        df['researcher_id'] = None

    if not df_prog.empty:
        df = df.merge(
            df_prog, left_on='id do programa', right_on='code', how='left'
        )
    else:
        df['graduate_program_id'] = None

    SCRIPT_SQL = """
        INSERT INTO public.graduate_program_researcher
            (graduate_program_id, researcher_id, year, type_)
        VALUES
            (%(pg_id)s, %(r_id)s, %(year)s, %(categoria)s)
        ON CONFLICT DO NOTHING;
    """

    years = (2026, 2025, 2024, 2023)
    stats = Counter()

    print('Iniciando insercoes...')

    for i, row in df.iterrows():
        if (i + 1) % 250 == 0 or (i + 1) == total:
            print(f'Progresso: {i + 1}/{total}')

        r_id = row.get('researcher_id')
        pg_id = row.get('graduate_program_id')

        if pd.isna(r_id):
            stats['Barrado: pesquisador nao encontrado'] += 1
            continue

        if pd.isna(pg_id):
            stats['Barrado: programa nao encontrado'] += 1
            continue

        for year in years:
            params = {
                'pg_id': str(pg_id),
                'r_id': str(r_id),
                'categoria': row['categoria'],
                'year': year,
            }
            try:
                conn_admin.exec(SCRIPT_SQL, params)
                stats['Processado'] += 1
            except Exception as e:
                stats['Erro: insert'] += 1
                print(f'Erro no insert: {e}')

    print('=' * 50)
    print('RELATORIO FINAL')
    print('=' * 50)
    print(f'Total lido: {total}')
    for k, v in stats.most_common():
        print(f'{k}: {v}')


if __name__ == '__main__':
    main()
