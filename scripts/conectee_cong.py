import pandas as pd
from simcc.repositories import conn

columns = ['MEMBRO', 'DEPARTAMENTO', 'MANDATO', 'EMAIL', 'TELEFONE']

mandates = pd.read_excel('storage/conectee/congregacao.xls', skiprows=2)


# Função para remover "PROF." e converter para maiúsculas
def clean_member_name(name):
    if pd.notna(name) and isinstance(name, str):
        if (
            'PROF.' in name.upper()
        ):  # Garantindo que a verificação seja em maiúsculas
            name = name.split('.', 1)[
                -1
            ].strip()  # Pegando apenas a segunda parte
        return name.upper()
    return name


mandates['MEMBRO'] = mandates['MEMBRO'].apply(clean_member_name)

mandates = mandates.where(pd.notna(mandates), None)
mandates = mandates.dropna(how='all')

for _, mandate in mandates.iterrows():
    SCRIPT_SQL = """
        INSERT INTO ufmg.mandate(member, departament, mandate, email, phone)
            VALUES (%(MEMBRO)s, %(DEPARTAMENTO)s, %(MANDATO)s, %(E-MAIL)s,
            %(TELEFONE)s);
        """
    conn.exec(SCRIPT_SQL, mandate.to_dict())
