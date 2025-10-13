import uuid
from sqlalchemy import Column, DateTime, Enum, Integer, String, BigInteger
from sqlalchemy.dialects.postgresql import UUID
from app.db.base import Base
from app.models.enums import MovementEvent, EventType, SpecVersion, DataContentType


class Movement(Base):
    __tablename__ = "movements"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    source = Column(String, nullable=False)
    specversion = Column(Enum(SpecVersion), nullable=False)
    type = Column(Enum(EventType), nullable=False)
    datacontenttype = Column(Enum(DataContentType), nullable=False)
    dataschema = Column(String, nullable=False)
    time = Column(BigInteger, nullable=False)
    subject = Column(String, nullable=False)
    destination = Column(String, nullable=False)

    # данные из вложенного `data` блока
    movement_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    warehouse_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    product_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    quantity = Column(Integer, nullable=False)
    event = Column(Enum(MovementEvent), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)