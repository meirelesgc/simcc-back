from asyncio import run

import httpx
import pandas as pd

from simcc.core.database import admin_conn


def validate_lattes(lattes_id):
    try:
        PROXY_URL = f'https://simcc.uesc.br/v3/api/getDataAtualizacaoCV?lattes_id={lattes_id}'
        response = httpx.get(PROXY_URL, verify=False, timeout=None).json()
        return bool(response)
    except Exception as e:
        print(e)
        return False


async def main():
    await admin_conn.connect()

    SQL_QUERY = """
        SELECT r.name, r.lattes_id, i.acronym
        FROM researcher r
        LEFT JOIN institution i 
            ON r.institution_id = i.institution_id
        """

    researchers = await admin_conn.select(SQL_QUERY)
    researchers = pd.DataFrame(researchers)

    validation_results = []
    for _, researcher in researchers.iterrows():
        is_valid = validate_lattes(researcher['lattes_id'])
        print(f'Researcher: {researcher["name"]} - Validated: {is_valid}')
        validation_results.append(is_valid)

    researchers['lattes_validated'] = validation_results
    researchers.to_csv('researchers_with_validation.csv', index=False)

    await admin_conn.disconnect()


if __name__ == '__main__':
    run(main())
