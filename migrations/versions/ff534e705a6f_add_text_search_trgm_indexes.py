"""add_text_search_trgm_indexes

Revision ID: ff534e705a6f
Revises: e128ba4e90ec
Create Date: 2026-08-24 17:44:40.236002

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ff534e705a6f'
down_revision: Union[str, Sequence[str], None] = 'e128ba4e90ec'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute('CREATE EXTENSION IF NOT EXISTS unaccent;')
    op.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm;')
    op.execute('ALTER FUNCTION unaccent(text) IMMUTABLE;')

    # 1. GIN Trigram Indexes para Busca Textual Rápida e Autocomplete
    op.execute('CREATE INDEX IF NOT EXISTS idx_researcher_name_trgm ON researcher USING gin (unaccent(lower(name)) gin_trgm_ops);')
    op.execute('CREATE INDEX IF NOT EXISTS idx_researcher_abstract_trgm ON researcher USING gin (unaccent(lower(abstract)) gin_trgm_ops);')
    op.execute('CREATE INDEX IF NOT EXISTS idx_bp_title_trgm ON bibliographic_production USING gin (unaccent(lower(title)) gin_trgm_ops);')
    op.execute('CREATE INDEX IF NOT EXISTS idx_bpbc_book_title_trgm ON bibliographic_production_book_chapter USING gin (unaccent(lower(book_title)) gin_trgm_ops);')
    op.execute('CREATE INDEX IF NOT EXISTS idx_patent_title_trgm ON patent USING gin (unaccent(lower(title)) gin_trgm_ops);')
    op.execute('CREATE INDEX IF NOT EXISTS idx_pe_event_name_trgm ON participation_events USING gin (unaccent(lower(event_name)) gin_trgm_ops);')
    op.execute('CREATE INDEX IF NOT EXISTS idx_bpew_event_name_trgm ON bibliographic_production_work_in_event USING gin (unaccent(lower(event_name)) gin_trgm_ops);')
    op.execute('CREATE INDEX IF NOT EXISTS idx_event_org_title_trgm ON event_organization USING gin (unaccent(lower(title)) gin_trgm_ops);')
    op.execute('CREATE INDEX IF NOT EXISTS idx_rp_area_specialty_trgm ON researcher_production USING gin (unaccent(lower(area_specialty)) gin_trgm_ops);')
    op.execute('CREATE INDEX IF NOT EXISTS idx_area_specialty_name_trgm ON area_specialty USING gin (unaccent(lower(name)) gin_trgm_ops);')
    op.execute('CREATE INDEX IF NOT EXISTS idx_area_expertise_name_trgm ON area_expertise USING gin (unaccent(lower(name)) gin_trgm_ops);')
    op.execute('CREATE INDEX IF NOT EXISTS idx_sub_area_expertise_name_trgm ON sub_area_expertise USING gin (unaccent(lower(name)) gin_trgm_ops);')
    op.execute('CREATE INDEX IF NOT EXISTS idx_great_area_expertise_name_trgm ON great_area_expertise USING gin (unaccent(lower(name)) gin_trgm_ops);')
    op.execute('CREATE INDEX IF NOT EXISTS idx_research_dictionary_term_trgm ON research_dictionary USING gin (unaccent(lower(term)) gin_trgm_ops);')

    # 2. B-Tree Indexes para filtros frequentes e relacionamentos
    op.execute('CREATE INDEX IF NOT EXISTS idx_bp_type_year_researcher ON bibliographic_production (type, year_, researcher_id);')
    op.execute('CREATE INDEX IF NOT EXISTS idx_pe_researcher_type_year ON participation_events (researcher_id, type_participation, year);')
    op.execute('CREATE INDEX IF NOT EXISTS idx_patent_researcher_year ON patent (researcher_id, development_year);')
    op.execute('CREATE INDEX IF NOT EXISTS idx_researcher_inst_status ON researcher (institution_id, status);')


def downgrade() -> None:
    """Downgrade schema."""
    op.execute('DROP INDEX IF EXISTS idx_researcher_inst_status;')
    op.execute('DROP INDEX IF EXISTS idx_patent_researcher_year;')
    op.execute('DROP INDEX IF EXISTS idx_pe_researcher_type_year;')
    op.execute('DROP INDEX IF EXISTS idx_bp_type_year_researcher;')
    op.execute('DROP INDEX IF EXISTS idx_research_dictionary_term_trgm;')
    op.execute('DROP INDEX IF EXISTS idx_great_area_expertise_name_trgm;')
    op.execute('DROP INDEX IF EXISTS idx_sub_area_expertise_name_trgm;')
    op.execute('DROP INDEX IF EXISTS idx_area_expertise_name_trgm;')
    op.execute('DROP INDEX IF EXISTS idx_area_specialty_name_trgm;')
    op.execute('DROP INDEX IF EXISTS idx_rp_area_specialty_trgm;')
    op.execute('DROP INDEX IF EXISTS idx_event_org_title_trgm;')
    op.execute('DROP INDEX IF EXISTS idx_bpew_event_name_trgm;')
    op.execute('DROP INDEX IF EXISTS idx_pe_event_name_trgm;')
    op.execute('DROP INDEX IF EXISTS idx_patent_title_trgm;')
    op.execute('DROP INDEX IF EXISTS idx_bpbc_book_title_trgm;')
    op.execute('DROP INDEX IF EXISTS idx_bp_title_trgm;')
    op.execute('DROP INDEX IF EXISTS idx_researcher_abstract_trgm;')
    op.execute('DROP INDEX IF EXISTS idx_researcher_name_trgm;')

