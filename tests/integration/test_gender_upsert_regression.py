from uuid import uuid4

import pytest
from sqlalchemy import text

from simcc.core.db.models.researcher import Researcher
from simcc.core.db.models.researcher_institution import (
    ResearcherInstitutionData,
)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_gender_upsert_non_destructive(session):
    # 1. Cria pesquisador
    unique_lattes = str(uuid4().int)[:16]
    researcher = Researcher(
        name='Pesquisador Teste Upsert',
        lattes_id=unique_lattes,
    )
    session.add(researcher)
    await session.commit()
    await session.refresh(researcher)

    # 2. Cria dados institucionais iniciais com atributos customizados
    inst_data = ResearcherInstitutionData(
        researcher_id=researcher.id,
        zip_code='44300-000',
        work_regime='DE',
        custom_attributes={'siape': '1234567', 'department': 'DCET'},
    )
    session.add(inst_data)
    await session.commit()

    # 3. Executa a query de UPSERT da rotina gender_ai
    upsert_sql = text("""
        INSERT INTO researcher_institution_data (
            researcher_id, custom_attributes
        )
        VALUES (
            :researcher_id,
            jsonb_build_object('genero', CAST(:gender AS TEXT))
        )
        ON CONFLICT (researcher_id) DO UPDATE SET
            custom_attributes = COALESCE(
                researcher_institution_data.custom_attributes, '{}'::jsonb
            ) || jsonb_build_object('genero', CAST(EXCLUDED.custom_attributes->>'genero' AS TEXT));
    """)

    await session.execute(
        upsert_sql,
        {'researcher_id': researcher.id, 'gender': 'Homem Cis'},
    )
    await session.commit()

    # 4. Verifica que siape, department, zip_code e work_regime foram preservados
    res = await session.execute(
        text("""
            SELECT zip_code, work_regime, custom_attributes
            FROM researcher_institution_data
            WHERE researcher_id = :researcher_id
        """),
        {'researcher_id': researcher.id},
    )
    row = res.mappings().one()

    assert row['zip_code'] == '44300-000'
    assert row['work_regime'] == 'DE'
    assert row['custom_attributes']['genero'] == 'Homem Cis'
    assert row['custom_attributes']['siape'] == '1234567'
    assert row['custom_attributes']['department'] == 'DCET'


@pytest.mark.integration
@pytest.mark.asyncio
async def test_gender_upsert_when_record_does_not_exist(session):
    # 1. Cria pesquisador sem dados institucionais
    unique_lattes = str(uuid4().int)[:16]
    researcher = Researcher(
        name='Pesquisador Novo Sem Instituicao',
        lattes_id=unique_lattes,
    )
    session.add(researcher)
    await session.commit()
    await session.refresh(researcher)

    # 2. Executa a query de UPSERT
    upsert_sql = text("""
        INSERT INTO researcher_institution_data (
            researcher_id, custom_attributes
        )
        VALUES (
            :researcher_id,
            jsonb_build_object('genero', CAST(:gender AS TEXT))
        )
        ON CONFLICT (researcher_id) DO UPDATE SET
            custom_attributes = COALESCE(
                researcher_institution_data.custom_attributes, '{}'::jsonb
            ) || jsonb_build_object('genero', CAST(EXCLUDED.custom_attributes->>'genero' AS TEXT));
    """)

    await session.execute(
        upsert_sql,
        {'researcher_id': researcher.id, 'gender': 'Mulher Cis'},
    )
    await session.commit()

    # 3. Verifica que a linha foi inserida
    res = await session.execute(
        text("""
            SELECT zip_code, work_regime, custom_attributes
            FROM researcher_institution_data
            WHERE researcher_id = :researcher_id
        """),
        {'researcher_id': researcher.id},
    )
    row = res.mappings().one()

    assert row['zip_code'] is None
    assert row['work_regime'] is None
    assert row['custom_attributes']['genero'] == 'Mulher Cis'
