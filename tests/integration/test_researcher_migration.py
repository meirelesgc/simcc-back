import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from simcc.core.db.models.researcher import Researcher
from simcc.core.db.models.researcher_custom_attributes import (
    ResearcherCustomAttributes,
)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_researcher_lattes_id_not_null_constraint(session):
    researcher_invalid = Researcher(
        name='Pesquisador Sem Lattes',
        lattes_id=None,  # type: ignore
    )
    session.add(researcher_invalid)

    # Inserção sem lattes_id deve falhar no commit
    with pytest.raises(IntegrityError):
        await session.commit()

    await session.rollback()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_researcher_institution_data_crud_and_cascade(session):
    # 1. Cria pesquisador válido com lattes_id único
    researcher = Researcher(
        name='Pesquisador Teste Instituicao',
        lattes_id='9999888877776666',
    )
    session.add(researcher)
    await session.commit()
    await session.refresh(researcher)

    # 2. Cria dados institucionais vinculados
    inst_data = ResearcherCustomAttributes(
        researcher_id=researcher.id,
        zip_code='44300-000',
        work_regime='DE',
        custom_attributes={'siape': '1234567', 'department': 'CAHL'},
    )
    session.add(inst_data)
    await session.commit()
    await session.refresh(inst_data)

    assert inst_data.researcher_id == researcher.id
    assert inst_data.custom_attributes['siape'] == '1234567'

    # 3. Testa deleção em cascata
    await session.delete(researcher)
    await session.commit()

    # Verifica remoção automática via CASCADE
    sql = (
        'SELECT id FROM researcher_custom_attributes '
        'WHERE researcher_id = :rid'
    )
    result = await session.execute(text(sql), {'rid': str(researcher.id)})
    assert result.scalar() is None
