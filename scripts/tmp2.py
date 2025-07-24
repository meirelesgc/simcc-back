from asyncio import run

import pandas as pd

from simcc.core.database import admin_conn, conn


async def main():
    await admin_conn.connect()
    await conn.connect()
    SCRIPT_SQL = """
        INSERT INTO researcher (name, lattes_id, institution_id)
        SELECT %(name)s, %(lattes_id)s, institution_id
        FROM institution WHERE acronym = %(acronym)s
        ON CONFLICT (lattes_id) DO NOTHING;
        """
    researchers = pd.read_csv('researchers_with_validation.csv')
    await conn.execmany(SCRIPT_SQL, researchers.to_dict(orient='records'))
    await admin_conn.disconnect()
    await conn.disconnect()


if __name__ == '__main__':
    run(main())
