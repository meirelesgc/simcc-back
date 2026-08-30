import tempfile
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text

from scripts.ingest.ingest_institution_researchers import ingest_csv
from simcc.core.db.models.researcher import Researcher


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ingest_csv_with_real_session(session):
    # 1. Cria pesquisador no banco com lattes_id único
    unique_lattes = str(uuid4().int)[:16]
    researcher = Researcher(
        name='Pesquisador Teste Ingestao',
        lattes_id=unique_lattes,
    )
    session.add(researcher)
    await session.commit()
    await session.refresh(researcher)

    # 2. Cria CSV de teste temporário
    csv_content = (
        'siape,name,department,work_regime,city,lattes_id,zip_code\n'
        '1673892,ADRIANO ANUNCIACAO OLIVEIRA,CAHL,DE,CACHOEIRA,'
        f'{unique_lattes},44300-000\n'
        '9999999,DESCONHECIDO,DEPX,40,SALVADOR,0000000000000000,40000-000\n'
    )
    with tempfile.NamedTemporaryFile(
        'w', suffix='.csv', encoding='utf-8', delete=False
    ) as f:
        f.write(csv_content)
        temp_csv_path = Path(f.name)

    try:
        # 3. Executa a ingestão
        metrics = await ingest_csv(temp_csv_path, session)

        expected_total = 2
        expected_upserted = 1
        expected_ignored = 1

        assert metrics['total'] == expected_total
        assert metrics['upserted'] == expected_upserted
        assert metrics['ignored'] == expected_ignored

        # 4. Verifica dados no banco
        sql_select = (
            'SELECT * FROM researcher_institution_data '
            'WHERE researcher_id = :rid'
        )
        result = await session.execute(
            text(sql_select),
            {'rid': str(researcher.id)},
        )
        row = result.mappings().one()
        assert row['zip_code'] == '44300-000'
        assert row['work_regime'] == 'DE'
        attrs = row['custom_attributes']
        assert attrs['siape'] == '1673892'
        assert attrs['department'] == 'CAHL'
        assert attrs['city'] == 'CACHOEIRA'
        assert 'name' not in attrs

        # 5. Testa idempotência (reexecução com o mesmo CSV)
        metrics2 = await ingest_csv(temp_csv_path, session)
        assert metrics2['upserted'] == expected_upserted
        assert metrics2['ignored'] == expected_ignored

        # Garante que continua havendo exatamente 1 registro
        sql_count = (
            'SELECT COUNT(*) FROM researcher_institution_data '
            'WHERE researcher_id = :rid'
        )
        count_res = await session.execute(
            text(sql_count),
            {'rid': str(researcher.id)},
        )
        assert count_res.scalar() == 1

    finally:
        temp_csv_path.unlink(missing_ok=True)
