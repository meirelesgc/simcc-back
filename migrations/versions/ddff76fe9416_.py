"""empty message

Revision ID: ddff76fe9416
Revises: af7a03c3482f
Create Date: 2026-05-26 12:11:40.342222

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ddff76fe9416'
down_revision: Union[str, Sequence[str], None] = 'af7a03c3482f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("""
        ALTER TABLE graduate_program_researcher
        ALTER COLUMN year TYPE INT USING year[1];
    """)


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("""
        ALTER TABLE graduate_program_researcher
        ALTER COLUMN year TYPE INT[] USING ARRAY[year];
    """)