"""add_f_unaccent_wrapper_and_trgm_indexes

Revision ID: 9e12486e01a7
Revises: 30d257f05aae
Create Date: 2026-09-09 05:25:52.778306

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9e12486e01a7'
down_revision: Union[str, Sequence[str], None] = '30d257f05aae'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute('CREATE EXTENSION IF NOT EXISTS unaccent;')
    op.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm;')

    # 1. Cria a função wrapper IMMUTABLE para o unaccent no schema public
    # Funções de usuário são exportadas com IMMUTABLE pelo pg_dump, garantindo integridade no pg_restore
    op.execute("""
        CREATE OR REPLACE FUNCTION public.f_unaccent(text)
        RETURNS text
        LANGUAGE sql
        IMMUTABLE
        PARALLEL SAFE
        STRICT
        AS $$
            SELECT public.unaccent('public.unaccent', $1);
        $$;
    """)

    # 2. Recria os 14 índices trigram utilizando public.f_unaccent
    trgm_indexes = [
        ('idx_researcher_name_trgm', 'researcher', 'name'),
        ('idx_researcher_abstract_trgm', 'researcher', 'abstract'),
        ('idx_bp_title_trgm', 'bibliographic_production', 'title'),
        ('idx_bpbc_book_title_trgm', 'bibliographic_production_book_chapter', 'book_title'),
        ('idx_patent_title_trgm', 'patent', 'title'),
        ('idx_pe_event_name_trgm', 'participation_events', 'event_name'),
        ('idx_bpew_event_name_trgm', 'bibliographic_production_work_in_event', 'event_name'),
        ('idx_event_org_title_trgm', 'event_organization', 'title'),
        ('idx_rp_area_specialty_trgm', 'researcher_production', 'area_specialty'),
        ('idx_area_specialty_name_trgm', 'area_specialty', 'name'),
        ('idx_area_expertise_name_trgm', 'area_expertise', 'name'),
        ('idx_sub_area_expertise_name_trgm', 'sub_area_expertise', 'name'),
        ('idx_great_area_expertise_name_trgm', 'great_area_expertise', 'name'),
        ('idx_research_dictionary_term_trgm', 'research_dictionary', 'term'),
    ]

    for idx_name, table, column in trgm_indexes:
        op.execute(f'DROP INDEX IF EXISTS {idx_name};')
        op.execute(
            f'CREATE INDEX IF NOT EXISTS {idx_name} ON {table} '
            f'USING gin (public.f_unaccent(lower({column})) gin_trgm_ops);'
        )


def downgrade() -> None:
    """Downgrade schema."""
    trgm_indexes = [
        'idx_researcher_name_trgm',
        'idx_researcher_abstract_trgm',
        'idx_bp_title_trgm',
        'idx_bpbc_book_title_trgm',
        'idx_patent_title_trgm',
        'idx_pe_event_name_trgm',
        'idx_bpew_event_name_trgm',
        'idx_event_org_title_trgm',
        'idx_rp_area_specialty_trgm',
        'idx_area_specialty_name_trgm',
        'idx_area_expertise_name_trgm',
        'idx_sub_area_expertise_name_trgm',
        'idx_great_area_expertise_name_trgm',
        'idx_research_dictionary_term_trgm',
    ]

    for idx_name in trgm_indexes:
        op.execute(f'DROP INDEX IF EXISTS {idx_name};')

    op.execute('DROP FUNCTION IF EXISTS public.f_unaccent(text);')
