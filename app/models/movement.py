import uuid
from sqlalchemy import Column, DateTime, Enum, Integer, String, BigInteger, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from app.db.base import Base
from app.models.enums import MovementEvent, EventType, SpecVersion, DataContentType


class Movement(Base):
    __tablename__ = "movements"
    # id перемещения одинаков для отправки и приёмки, поэтому идентичность события —
    # это пара (movement_id, event); на неё же опираемся при идемпотентной вставке
    __table_args__ = (
        UniqueConstraint("movement_id", "event", name="uq_movement_event"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    # id самого Kafka-сообщения; раньше он затирал первичный ключ, теперь хранится отдельно
    message_id = Column(UUID(as_uuid=True), nullable=False)
    source = Column(String, nullable=False)
    # values_callable — чтобы в БД лежали значения ("1.0"), а не имена членов ("V1_0")
    specversion = Column(Enum(SpecVersion, values_callable=lambda e: [m.value for m in e]), nullable=False)
    type = Column(Enum(EventType, values_callable=lambda e: [m.value for m in e]), nullable=False)
    datacontenttype = Column(Enum(DataContentType, values_callable=lambda e: [m.value for m in e]), nullable=False)
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