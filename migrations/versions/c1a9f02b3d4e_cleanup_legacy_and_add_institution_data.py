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
            -- Limpar dependências de bibliographic_production
            DELETE FROM openalex_article WHERE article_id IN (
                SELECT id FROM bibliographic_production WHERE researcher_id IN (
                    SELECT id FROM researcher WHERE lattes_id IS NULL
                )
            );
            DELETE FROM bibliographic_production_article WHERE bibliographic_production_id IN (
                SELECT id FROM bibliographic_production WHERE researcher_id IN (
                    SELECT id FROM researcher WHERE lattes_id IS NULL
                )
            );
            DELETE FROM bibliographic_production_book_chapter WHERE bibliographic_production_id IN (
                SELECT id FROM bibliographic_production WHERE researcher_id IN (
                    SELECT id FROM researcher WHERE lattes_id IS NULL
                )
            );
            DELETE FROM bibliographic_production_book WHERE bibliographic_production_id IN (
                SELECT id FROM bibliographic_production WHERE researcher_id IN (
                    SELECT id FROM researcher WHERE lattes_id IS NULL
                )
            );
            DELETE FROM bibliographic_production_work_in_event WHERE bibliographic_production_id IN (
                SELECT id FROM bibliographic_production WHERE researcher_id IN (
                    SELECT id FROM researcher WHERE lattes_id IS NULL
                )
            );

            -- Limpar dependências de process_or_technique
            DELETE FROM process_author WHERE process_id IN (
                SELECT id FROM process_or_technique WHERE researcher_id IN (
                    SELECT id FROM researcher WHERE lattes_id IS NULL
                )
            );
            DELETE FROM process_keyword WHERE process_id IN (
                SELECT id FROM process_or_technique WHERE researcher_id IN (
                    SELECT id FROM researcher WHERE lattes_id IS NULL
                )
            );
            DELETE FROM process_knowledge_area WHERE process_id IN (
                SELECT id FROM process_or_technique WHERE researcher_id IN (
                    SELECT id FROM researcher WHERE lattes_id IS NULL
                )
            );

            -- Limpar dependências de research_project
            DELETE FROM research_project_components WHERE project_id IN (
                SELECT id FROM research_project WHERE researcher_id IN (
                    SELECT id FROM researcher WHERE lattes_id IS NULL
                )
            );
            DELETE FROM research_project_foment WHERE project_id IN (
                SELECT id FROM research_project WHERE researcher_id IN (
                    SELECT id FROM researcher WHERE lattes_id IS NULL
                )
            );
            DELETE FROM research_project_production WHERE project_id IN (
                SELECT id FROM research_project WHERE researcher_id IN (
                    SELECT id FROM researcher WHERE lattes_id IS NULL
                )
            );

            -- Excluir pesquisadores órfãos
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

    # 4. Excluir schemas legados fisicamente do banco de dados
    op.execute('DROP SCHEMA IF EXISTS admin CASCADE;')
    op.execute('DROP SCHEMA IF EXISTS admin_simcc CASCADE;')
    op.execute('DROP SCHEMA IF EXISTS logs CASCADE;')
    op.execute('DROP SCHEMA IF EXISTS ufmg CASCADE;')


def downgrade() -> None:
    # 1. Recriar schemas legados
    op.execute('CREATE SCHEMA IF NOT EXISTS admin;')
    op.execute('CREATE SCHEMA IF NOT EXISTS admin_simcc;')
    op.execute('CREATE SCHEMA IF NOT EXISTS logs;')
    op.execute('CREATE SCHEMA IF NOT EXISTS ufmg;')

    # 2. Remover tabela researcher_institution_data
    op.drop_index(
        'ix_researcher_institution_data_researcher_id',
        table_name='researcher_institution_data',
    )
    op.drop_table('researcher_institution_data')

    # 3. Reverter constraint NOT NULL na coluna lattes_id
    op.alter_column(
        'researcher',
        'lattes_id',
        existing_type=sa.String(),
        nullable=True,
    )
