import unicodedata
import logging
from collections import Counter

import pandas as pd
from psycopg.errors import UniqueViolation

from simcc.repositories import conn_admin

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def normalize_string(s):
    if not isinstance(s, str):
        return str(s) if s is not None else ""
    s = unicodedata.normalize('NFD', s).replace('\n', '')
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return s.lower()

def get_researcher_id(researcher_name):
    if not researcher_name or (isinstance(researcher_name, float) and pd.isna(researcher_name)):
        return None
    SCRIPT_SQL = """
        SELECT researcher_id
        FROM researcher
        WHERE
            regexp_replace(unaccent(upper(name)),'[^A-Z0-9]','','g')
            =
            regexp_replace(unaccent(upper(%(name)s)),'[^A-Z0-9]','','g')
    """
    researcher = conn_admin.select(SCRIPT_SQL, {'name': str(researcher_name)})
    if researcher:
        return researcher[0]['researcher_id']
    return None

def get_program_id(code):
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return None
    SCRIPT_SQL = """
        SELECT graduate_program_id
        FROM graduate_program
        WHERE code = %(code)s
    """
    program = conn_admin.select(SCRIPT_SQL, {'code': code})
    if program:
        return program[0]['graduate_program_id']
    return None

def ident_row(row):
    nome = row.get('nome')
    pg_code = row.get('id do programa')
    cat = row.get('categoria')
    parts = []
    parts.append(f"nome={nome}" if nome is not None else "nome=<vazio>")
    parts.append(f"pg_code={pg_code}" if pg_code is not None else "pg_code=<vazio>")
    parts.append(f"categoria_raw={cat}" if cat is not None else "categoria_raw=<vazio>")
    return " ".join(parts)

def normalize_categoria(value):
    v = str(value) if value is not None else ""
    return "PERMANENTE" if "PERMANENTE" in v.upper() else "COLABORADOR"

def main():
    path = "storage/csv/002_program_researchers.csv"
    logger.info(f"Carregando CSV: {path}")

    try:
        researchers = pd.read_csv(path)
    except Exception as e:
        logger.critical(f"Falha ao ler CSV: {e}")
        return

    researchers.columns = [normalize_string(col) for col in researchers.columns]
    total = len(researchers)
    logger.info(f"Total de registros no CSV: {total}")

    required_cols = {"nome", "id do programa", "categoria"}
    missing = required_cols - set(researchers.columns)
    if missing:
        logger.critical(f"CSV sem colunas obrigatorias: {sorted(missing)}")
        return

    SCRIPT_SQL = """
        INSERT INTO public.graduate_program_researcher
            (graduate_program_id, researcher_id, year, type_)
        VALUES
            (%(pg_id)s, %(r_id)s, %(year)s, %(categoria)s);
    """

    years = (2026, 2025, 2024, 2023)
    stats = Counter()

    for i, (_, row) in enumerate(researchers.iterrows(), start=1):
        row_dict = row.to_dict()

        if i % 250 == 0 or i == total:
            logger.info(f"Progresso: {i}/{total}")

        nome = row_dict.get("nome")
        pg_code = row_dict.get("id do programa")
        categoria = normalize_categoria(row_dict.get("categoria"))

        try:
            r_id = get_researcher_id(nome)
        except Exception as e:
            stats["Erro: lookup pesquisador"] += 1
            logger.error(f"PGP_LOOKUP_PESQUISADOR_FALHOU {ident_row(row_dict)} erro={e}")
            continue

        if not r_id:
            stats["Barrado: pesquisador nao encontrado"] += 1
            logger.warning(f"PGP_BARRADO_PESQUISADOR_NAO_ENCONTRADO {ident_row(row_dict)}")
            continue

        try:
            pg_id = get_program_id(pg_code)
        except Exception as e:
            stats["Erro: lookup programa"] += 1
            logger.error(f"PGP_LOOKUP_PROGRAMA_FALHOU {ident_row(row_dict)} erro={e}")
            continue

        if not pg_id:
            stats["Barrado: programa nao encontrado"] += 1
            logger.warning(f"PGP_BARRADO_PROGRAMA_NAO_ENCONTRADO {ident_row(row_dict)}")
            continue

        for year in years:
            params = {
                "pg_id": pg_id,
                "r_id": r_id,
                "categoria": categoria,
                "year": year
            }
            try:
                conn_admin.exec(SCRIPT_SQL, params)
                stats["Inserido"] += 1
                logger.info(f"PGP_INSERIDO nome={nome} pg_code={pg_code} r_id={r_id} pg_id={pg_id} categoria={categoria} year={year}")
            except UniqueViolation:
                stats["Duplicado"] += 1
                logger.warning(f"PGP_DUPLICADO nome={nome} pg_code={pg_code} r_id={r_id} pg_id={pg_id} categoria={categoria} year={year}")
            except Exception as e:
                stats["Erro: insert"] += 1
                logger.error(f"PGP_ERRO_INSERT nome={nome} pg_code={pg_code} r_id={r_id} pg_id={pg_id} categoria={categoria} year={year} erro={e}")

    logger.info("=" * 50)
    logger.info("RELATORIO FINAL")
    logger.info("=" * 50)
    logger.info(f"Total lido: {total}")
    for k, v in stats.most_common():
        logger.info(f"{k}: {v}")

if __name__ == "__main__":
    main()
