"""store enum values instead of member names

Revision ID: b7d3e9042af1
Revises: 9c2f1a7b4e10
Create Date: 2025-10-14 11:40:12.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'b7d3e9042af1'
down_revision: Union[str, Sequence[str], None] = '9c2f1a7b4e10'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# метки enum'ов исходно создавались из имён членов; приводим к их значениям
_RENAMES = [
    ("specversion", "V1_0", "1.0"),
    ("eventtype", "WAREHOUSE_MOVEMENT", "ru.retail.warehouses.movement"),
    ("datacontenttype", "JSON", "application/json"),
]


def upgrade() -> None:
    """Upgrade schema."""
    for type_name, old, new in _RENAMES:
        op.execute(f"ALTER TYPE {type_name} RENAME VALUE '{old}' TO '{new}'")


def downgrade() -> None:
    """Downgrade schema."""
    for type_name, old, new in _RENAMES:
        op.execute(f"ALTER TYPE {type_name} RENAME VALUE '{new}' TO '{old}'")
