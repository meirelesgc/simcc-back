import httpx

from simcc.repositories import conn

labs = httpx.get('https://vip.uesc.br/list_labs_json').json()

for _, lab in enumerate(labs):
    lab['id_lattes'] = str(lab.get('id_lattes', '')).zfill(16)
    SCRIPT_SQL = """
        SELECT id FROM researcher WHERE LPAD(lattes_id, 16, '0') = %(id_lattes)s
    """
    lattes_id = conn.select(SCRIPT_SQL, lab, True)
    if lattes_id:
        lab['researcher_id'] = lattes_id.get('id')
        SCRIPT_SQL = """
                SELECT id FROM institution WHERE acronym = %(instituicao)s
                """
        institution_id = conn.select(SCRIPT_SQL, lab, True)
        if institution_id:
            lab['institution_id'] = institution_id.get('id')
            SCRIPT_SQL = """
                INSERT INTO public.labs(hashed_id, type, location, name,
                    description, website, activities, areas, campus,
                    institution_id, researcher_id, responsible)
                VALUES (%(HashedID)s, %(tipo)s, %(local)s, %(nome)s,
                    %(descricao)s, %(site)s, %(atividades)s, %(areas)s, %(campus)s,
                    %(institution_id)s, %(researcher_id)s,
                    %(responsavel)s);
                    """
            conn.exec(SCRIPT_SQL, lab)
            print(_, 'XPTO')
