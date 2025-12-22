import psycopg2

from config import settings


def conecta_db(database_url=None):
    dsn = database_url if database_url is not None else settings.DATABASE_URL
    return psycopg2.connect(dsn)


def execScript_db(sql, params=None, database_url=None):
    try:
        with conecta_db(database_url) as con, con.cursor() as cur:
            cur.execute(sql, params)
            con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        con.rollback()
        return 1


def consultar_db(sql, params=None, database_url=None):
    try:
        with conecta_db(database_url) as con, con.cursor() as cur:
            cur.execute(sql, params)
            registros = cur.fetchall()
            return registros
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        return 1
