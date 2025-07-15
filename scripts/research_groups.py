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
    groups = pd.read_csv('storage/research_groups/research_groups.csv')
    groups = groups.replace(nan, None)

    SCRIPT_SQL = """
        INSERT INTO public.research_group (name, institution, first_leader, second_leader, area)
        VALUES (%(Grupo de Pesquisa)s, %(Instituição)s, %(Líder 1)s, %(Líder 2)s, %(Área)s)
        ON CONFLICT (name, institution) DO UPDATE
        SET first_leader = COALESCE(NULLIF(EXCLUDED.first_leader, ''), research_group.first_leader),
            second_leader = COALESCE(NULLIF(EXCLUDED.second_leader, ''), research_group.second_leader),
            area = COALESCE(NULLIF(EXCLUDED.area, ''), research_group.area);
        """

    for _, group in groups.iterrows():
        data = group.to_dict()
        print(data['Grupo de Pesquisa'])
        conn.exec(SCRIPT_SQL, data)


def insert_researchers():
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
    # [{institution_id: UUID, acronym: str}]
    institutions = conn_admin.select(SCRIPT_SQL)
    SCRIPT_SQL = """
        INSERT INTO researcher (name, lattes_id, institution_id)
        VALUES (%(Nome)s, %(Lattes)s, %(institution_id)s);
        """
    for file in os.listdir('storage/research_groups/'):
        if file.startswith('researchers_'):
            group = sanitize(file[11:-4])
            # Nome,Titulação,Data de inclusão,Lattes
            csv = pd.read_csv(f'storage/research_groups/{file}')
            acronym = [n for n in i if sanitize(n['name']) == group]
            acronym = acronym[0]['institution']
            institution_id = [i for i in institutions if i['acronym'] == acronym]
            csv['institution_id'] = institution_id[0]['institution_id']
            for _, researcher in csv.iterrows():
                params = researcher.to_dict()
                conn_admin.exec(SCRIPT_SQL, params)


if __name__ == '__main__':
    insert_groups()
    insert_researchers()
