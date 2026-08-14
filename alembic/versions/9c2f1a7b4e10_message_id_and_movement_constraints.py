"""message_id column, unique movement event, non-negative stock

Revision ID: 9c2f1a7b4e10
Revises: 82f711fbd1bd
Create Date: 2025-10-14 11:20:05.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9c2f1a7b4e10'
down_revision: Union[str, Sequence[str], None] = '82f711fbd1bd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # id сообщения переехал из первичного ключа в отдельную колонку
    op.add_column('movements', sa.Column('message_id', sa.UUID(), nullable=False))
    # событие уникально в рамках перемещения — на этом строится идемпотентность консьюмера
    op.create_unique_constraint('uq_movement_event', 'movements', ['movement_id', 'event'])
    # остаток на складе не может быть отрицательным даже при гонках
    op.create_check_constraint(
        'ck_warehouse_state_quantity_non_negative',
        'warehouse_states',
        'quantity >= 0',
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint('ck_warehouse_state_quantity_non_negative', 'warehouse_states', type_='check')
    op.drop_constraint('uq_movement_event', 'movements', type_='unique')
    op.drop_column('movements', 'message_id')
