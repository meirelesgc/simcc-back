from uuid import uuid4

import pytest
from sqlalchemy import text

from simcc.core.db.models.researcher import Researcher
from simcc.core.db.models.researcher_custom_attributes import (
    ResearcherCustomAttributes,
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

    # 2. Cria dados customizados com atributos extras
    inst_data = ResearcherCustomAttributes(
        researcher_id=researcher.id,
        zip_code='44300-000',
        work_regime='DE',
        custom_attributes={'siape': '1234567', 'department': 'DCET'},
    )
    session.add(inst_data)
    await session.commit()

    # 3. Executa a query de upsert de gênero da rotina gender_ai
    upsert_sql = text("""
        INSERT INTO researcher_custom_attributes (
            researcher_id, gender
        )
        VALUES (
            :researcher_id, :gender
        )
        ON CONFLICT (researcher_id) DO UPDATE SET
            gender = EXCLUDED.gender;
    """)

    await session.execute(
        upsert_sql,
        {'researcher_id': researcher.id, 'gender': 'Homem Cis'},
    )
    await session.commit()

    # 4. Verifica que gender foi atualizado e que zip_code, work_regime e custom_attributes foram preservados
    res_attrs = await session.execute(
        text("""
            SELECT gender, zip_code, work_regime, custom_attributes
            FROM researcher_custom_attributes
            WHERE researcher_id = :researcher_id
        """),
        {'researcher_id': researcher.id},
    )
    row = res_attrs.mappings().one()

    assert row['gender'] == 'Homem Cis'
    assert row['zip_code'] == '44300-000'
    assert row['work_regime'] == 'DE'
    assert row['custom_attributes']['siape'] == '1234567'
    assert row['custom_attributes']['department'] == 'DCET'
    assert 'genero' not in row['custom_attributes']


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

    # 2. Executa a query de upsert de gênero
    upsert_sql = text("""
        INSERT INTO researcher_custom_attributes (
            researcher_id, gender
        )
        VALUES (
            :researcher_id, :gender
        )
        ON CONFLICT (researcher_id) DO UPDATE SET
            gender = EXCLUDED.gender;
    """)

    await session.execute(
        upsert_sql,
        {'researcher_id': researcher.id, 'gender': 'Mulher Cis'},
    )
    await session.commit()

    # 3. Verifica que a linha foi inserida em researcher_custom_attributes com gender preenchido
    res = await session.execute(
        text("""
            SELECT gender, zip_code, work_regime, custom_attributes
            FROM researcher_custom_attributes
            WHERE researcher_id = :researcher_id
        """),
        {'researcher_id': researcher.id},
    )
    row = res.mappings().one()

    assert row['gender'] == 'Mulher Cis'
    assert row['zip_code'] is None
    assert row['work_regime'] is None
    assert row['custom_attributes'] is None
