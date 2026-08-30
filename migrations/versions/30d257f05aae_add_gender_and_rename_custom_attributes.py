"""add_gender_and_rename_custom_attributes

Revision ID: 30d257f05aae
Revises: c1a9f02b3d4e
Create Date: 2026-08-30 10:03:50.723122

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '30d257f05aae'
down_revision: Union[str, Sequence[str], None] = 'c1a9f02b3d4e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Renomeia tabela researcher_institution_data para researcher_custom_attributes
    op.rename_table('researcher_institution_data', 'researcher_custom_attributes')

    # 2. Renomeia índices e constraints correspondentes
    op.execute(
        'ALTER INDEX IF EXISTS ix_researcher_institution_data_researcher_id '
        'RENAME TO ix_researcher_custom_attributes_researcher_id;'
    )
    op.execute(
        'ALTER TABLE researcher_custom_attributes '
        'RENAME CONSTRAINT uq_researcher_institution_data_researcher_id '
        'TO uq_researcher_custom_attributes_researcher_id;'
    )

    # 3. Adiciona coluna fixa gender na tabela researcher_custom_attributes
    op.add_column(
        'researcher_custom_attributes',
        sa.Column('gender', sa.String(), nullable=True),
    )

    # 4. Migração de dados: move gênero do JSONB custom_attributes para a coluna fixa gender
    op.execute("""
        UPDATE researcher_custom_attributes
        SET gender = (custom_attributes->>'genero')
        WHERE custom_attributes ? 'genero'
          AND (gender IS NULL OR TRIM(gender) = '');
    """)
    op.execute("""
        UPDATE researcher_custom_attributes
        SET custom_attributes = custom_attributes - 'genero'
        WHERE custom_attributes ? 'genero';
    """)


def downgrade() -> None:
    # 1. Restaura gênero para JSONB caso exista valor na coluna gender
    op.execute("""
        UPDATE researcher_custom_attributes
        SET custom_attributes = COALESCE(custom_attributes, '{}'::jsonb) || jsonb_build_object('genero', gender)
        WHERE gender IS NOT NULL;
    """)

    # 2. Remove coluna gender da tabela researcher_custom_attributes
    op.drop_column('researcher_custom_attributes', 'gender')

    # 3. Renomeia índices e constraints de volta
    op.execute(
        'ALTER INDEX IF EXISTS ix_researcher_custom_attributes_researcher_id '
        'RENAME TO ix_researcher_institution_data_researcher_id;'
    )
    op.execute(
        'ALTER TABLE researcher_custom_attributes '
        'RENAME CONSTRAINT uq_researcher_custom_attributes_researcher_id '
        'TO uq_researcher_institution_data_researcher_id;'
    )

    # 4. Renomeia tabela de volta
    op.rename_table('researcher_custom_attributes', 'researcher_institution_data')
