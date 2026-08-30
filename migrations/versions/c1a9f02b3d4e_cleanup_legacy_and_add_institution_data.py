"""cleanup_legacy_and_add_institution_data

Revision ID: c1a9f02b3d4e
Revises: ff534e705a6f
Create Date: 2026-08-29 19:15:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'c1a9f02b3d4e'
down_revision: Union[str, Sequence[str], None] = 'ff534e705a6f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Limpeza segura de registros sem lattes_id antes de aplicar NOT NULL
    op.execute("""
        DO $$
        DECLARE
            deleted_count INTEGER;
        BEGIN
            DELETE FROM researcher WHERE lattes_id IS NULL;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            RAISE NOTICE 'Deleted % orphan researchers with NULL lattes_id', deleted_count;
        END $$;
    """)

    # 2. Aplicar constraint NOT NULL na coluna lattes_id da tabela researcher
    op.alter_column(
        'researcher',
        'lattes_id',
        existing_type=sa.String(),
        nullable=False,
    )

    # 3. Criar nova tabela researcher_institution_data
    op.create_table(
        'researcher_institution_data',
        sa.Column(
            'id',
            postgresql.UUID(as_uuid=True),
            server_default=sa.text('gen_random_uuid()'),
            nullable=False,
            primary_key=True,
        ),
        sa.Column(
            'researcher_id',
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey('researcher.id', ondelete='CASCADE'),
            nullable=False,
        ),
        sa.Column('zip_code', sa.String(), nullable=True),
        sa.Column('work_regime', sa.String(), nullable=True),
        sa.Column(
            'custom_attributes',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.UniqueConstraint('researcher_id', name='uq_researcher_institution_data_researcher_id'),
    )
    op.create_index(
        'ix_researcher_institution_data_researcher_id',
        'researcher_institution_data',
        ['researcher_id'],
        unique=True,
    )


def downgrade() -> None:
    # 1. Remover tabela researcher_institution_data
    op.drop_index(
        'ix_researcher_institution_data_researcher_id',
        table_name='researcher_institution_data',
    )
    op.drop_table('researcher_institution_data')

    # 2. Reverter constraint NOT NULL na coluna lattes_id
    op.alter_column(
        'researcher',
        'lattes_id',
        existing_type=sa.String(),
        nullable=True,
    )
