"""add product type

Revision ID: 71fa84ae162a
Revises: 4104e507f4ce
Create Date: 2025-09-23 19:24:54.230126
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = "71fa84ae162a"
down_revision: Union[str, Sequence[str], None] = "4104e507f4ce"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # Only the product type change
    op.add_column("products", sa.Column("type", sa.String(length=32), nullable=True))
    op.create_index(op.f("ix_products_type"), "products", ["type"], unique=False)
    # Optional hardening:
    # op.execute("UPDATE products SET type = 'generic' WHERE type IS NULL")
    # op.alter_column("products", "type", nullable=False)

def downgrade() -> None:
    op.drop_index(op.f("ix_products_type"), table_name="products")
    op.drop_column("products", "type")
