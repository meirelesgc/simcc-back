import os
import re

import pandas as pd
from numpy import nan
from unidecode import unidecode

from simcc.repositories import conn, conn_admin


def sanitize(s):
    try:
        s = unidecode(s)
    except ImportError:
        pass
    s = s.lower()
    s = re.sub(r'[^a-z0-9]+', '_', s)
    s = re.sub(r'(^_|_$)', '', s)
    return s


def insert_groups():
    print('Lendo arquivo de grupos de pesquisa...')
    groups = pd.read_csv('storage/research_groups/research_groups.csv')
    groups = groups.replace(nan, None)
    groups['Instituição'] = 'ICTITE'

    SCRIPT_SQL = """
        INSERT INTO public.research_group (name, institution, first_leader, second_leader, area)
        VALUES (%(Grupo de Pesquisa)s, %(Instituição)s, %(Líder 1)s, %(Líder 2)s, %(Área)s)
        ON CONFLICT (name, institution) DO UPDATE
        SET first_leader = COALESCE(NULLIF(EXCLUDED.first_leader, ''), research_group.first_leader),
            second_leader = COALESCE(NULLIF(EXCLUDED.second_leader, ''), research_group.second_leader),
            area = COALESCE(NULLIF(EXCLUDED.area, ''), research_group.area);
        """

    for idx, group in groups.iterrows():
        data = group.to_dict()
        print(
            f'Inserindo grupo {data.get("Grupo de Pesquisa")} ({idx + 1}/{len(groups)})'
        )
        conn.exec(SCRIPT_SQL, data)
    print('Inserção de grupos concluída.\n')


def insert_researchers():
    print('Buscando dados de grupos e instituições...')
    SCRIPT_SQL = """
        SELECT research_group.name, institution
        FROM research_group
        INNER JOIN institution
            ON institution.acronym = research_group.institution
        WHERE institution IS NOT NULL
        """
    i = conn.select(SCRIPT_SQL)

    SCRIPT_SQL = """
        SELECT institution_id, acronym
        FROM institution
        """
    institutions = conn_admin.select(SCRIPT_SQL)

    SCRIPT_SQL = """
        INSERT INTO researcher (name, lattes_id, institution_id)
        VALUES (%(Nome)s, %(Lattes)s, %(institution_id)s)
        ON CONFLICT (lattes_id) DO NOTHING;
        """

    files = [
        f
        for f in os.listdir('storage/research_groups/')
        if f.startswith('researchers_')
    ]
    print(f'{len(files)} arquivos de pesquisadores encontrados.')

    for file in files:
        group = sanitize(file[11:-4])
        print(f'\nProcessando arquivo: {file} (grupo: {group})')
        csv = pd.read_csv(f'storage/research_groups/{file}')

        acronym = [n for n in i if sanitize(n['name']) == group]
        if not acronym:
            print(f'Aviso: grupo {group} não encontrado nos dados do banco.')
            continue

        acronym = acronym[0]['institution']
        institution_id = [i for i in institutions if i['acronym'] == acronym]
        if not institution_id:
            print(f'Aviso: instituição {acronym} não encontrada.')
            continue

        institution_id = institution_id[0]['institution_id']
        csv['institution_id'] = institution_id

        for idx, researcher in csv.iterrows():
            params = researcher.to_dict()
            string = f'Inserindo pesquisador {params.get("Nome")} ({idx + 1}/{len(csv)})'
            print(string)
            # WIP - O Certo é validar esse lattes_id
            conn_admin.exec(SCRIPT_SQL, params)
    print('Inserção de pesquisadores concluída.')


def sync_groups():
    SCRIPT_SQL = """
        SELECT research_group.name, research_group.id
        FROM research_group
        INNER JOIN institution
            ON institution.acronym = research_group.institution
        WHERE institution IS NOT NULL
        """
    g = conn.select(SCRIPT_SQL)

    SCRIPT_SQL = """
        INSERT INTO research_group_researcher (research_group_id, researcher_id)
        SELECT %(group_id)s, r.id
        FROM researcher r
        WHERE r.lattes_id = %(Lattes)s
        ON CONFLICT  (research_group_id, researcher_id) DO NOTHING;
        """
    for f in os.listdir('storage/research_groups/'):
        if f.startswith('researchers_'):
            group = sanitize(f[11:-4])
            print(f'\nProcessando arquivo: {f} (grupo: {group})')
            group_id = [n for n in g if sanitize(n['name']) == group]
            if not group_id:
                continue
            researchers = pd.read_csv(f'storage/research_groups/{f}')
            researchers['group_id'] = group_id[0]['id']
            researchers['Lattes'] = (
                researchers['Lattes'].astype(str).apply(lambda x: x.zfill(16))
            )
            conn.execmany(SCRIPT_SQL, researchers.to_dict(orient='records'))


if __name__ == '__main__':
    print('Iniciando inserção de dados...\n')
    insert_groups()
    insert_researchers()
    sync_groups()
    print('\nProcesso finalizado.')
