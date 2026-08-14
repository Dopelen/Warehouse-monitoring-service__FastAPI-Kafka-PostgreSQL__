from sqlalchemy import Column, Integer, CheckConstraint
from sqlalchemy.dialects.postgresql import UUID
from app.db.base import Base

class WarehouseState(Base):
    __tablename__ = "warehouse_states"
    # подстраховка на уровне БД: остаток не должен уходить в минус ни при каких гонках
    __table_args__ = (
        CheckConstraint("quantity >= 0", name="ck_warehouse_state_quantity_non_negative"),
    )

    warehouse_id = Column(UUID(as_uuid=True), primary_key=True)
    product_id = Column(UUID(as_uuid=True), primary_key=True)
    quantity = Column(Integer, nullable=False, default=0)