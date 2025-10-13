from pydantic import BaseModel, Field, field_validator
from uuid import UUID
from datetime import datetime
from app.models.enums import EventType, DataContentType, SpecVersion, MovementEvent
import re


class KafkaMovementData(BaseModel):
    movement_id: UUID = Field(..., description="ID перемещения")
    warehouse_id: UUID = Field(..., description="ID склада")
    product_id: UUID = Field(..., description="ID товара")
    quantity: int = Field(..., description="Количество товара")
    event: MovementEvent = Field(..., description="Событие: arrival или departure")
    timestamp: datetime = Field(..., description="Временная метка события")


class KafkaMovementMessage(BaseModel):
    id: UUID = Field(..., description="ID сообщения")
    source: str = Field(..., description="Источник сообщения (WH-****)")
    specversion: SpecVersion = Field(..., description="Версия спецификации")
    type: EventType = Field(..., description="Тип события")
    datacontenttype: DataContentType = Field(..., description="Тип содержимого")
    dataschema: str = Field(..., description="Ссылка на схему данных")
    time: int = Field(..., description="Время события в миллисекундах UNIX")
    subject: str = Field(..., description="Тема события")
    destination: str = Field(..., description="Получатель события")
    data: KafkaMovementData = Field(..., description="Данные перемещения")

    @field_validator("source")
    def validate_source(cls, v):
        if not re.match(r"^WH-\d{4}$", v):
            raise ValueError("source должно быть в формате WH-****")
        return v

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": "b3b53031-e83a-4654-87f5-b6b6fb09fd99",
                "source": "WH-3423",
                "specversion": "1.0",
                "type": "ru.retail.warehouses.movement",
                "datacontenttype": "application/json",
                "dataschema": "ru.retail.warehouses.movement.v1.0",
                "time": 1737439421623,
                "subject": "WH-3423:ARRIVAL",
                "destination": "ru.retail.warehouses",
                "data": {
                    "movement_id": "c6290746-790e-43fa-8270-014dc90e02e0",
                    "warehouse_id": "c1d70455-7e14-11e9-812a-70106f431230",
                    "product_id": "4705204f-498f-4f96-b4ba-df17fb56bf55",
                    "quantity": 100,
                    "event": "arrival",
                    "timestamp": "2025-02-18T14:34:56Z"
                }
            }
        }
    }


class FlatMovementMessage(BaseModel):
    id: UUID = Field(..., description="ID сообщения")
    source: str = Field(..., description="Источник сообщения (WH-****)")
    specversion: SpecVersion = Field(..., description="Версия спецификации")
    type: EventType = Field(..., description="Тип события")
    datacontenttype: DataContentType = Field(..., description="Тип содержимого")
    dataschema: str = Field(..., description="Ссылка на схему данных")
    time: int = Field(..., description="Время события в миллисекундах UNIX")
    subject: str = Field(..., description="Тема события")
    destination: str = Field(..., description="Получатель события")

    movement_id: UUID = Field(..., description="ID перемещения")
    warehouse_id: UUID = Field(..., description="ID склада")
    product_id: UUID = Field(..., description="ID товара")
    quantity: int = Field(..., description="Количество")
    event: MovementEvent = Field(..., description="Тип события")
    timestamp: datetime = Field(..., description="Время события")

    @field_validator("source")
    def validate_source(cls, v):
        if not re.match(r"^WH-\d{4}$", v):
            raise ValueError("source должно быть в формате WH-****")
        return v

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": "b3b53031-e83a-4654-87f5-b6b6fb09fd99",
                "source": "WH-3423",
                "specversion": "1.0",
                "type": "ru.retail.warehouses.movement",
                "datacontenttype": "application/json",
                "dataschema": "ru.retail.warehouses.movement.v1.0",
                "time": 1737439421623,
                "subject": "WH-3423:ARRIVAL",
                "destination": "ru.retail.warehouses",
                "movement_id": "c6290746-790e-43fa-8270-014dc90e02e0",
                "warehouse_id": "c1d70455-7e14-11e9-812a-70106f431230",
                "product_id": "4705204f-498f-4f96-b4ba-df17fb56bf55",
                "quantity": 100,
                "event": "arrival",
                "timestamp": "2025-02-18T14:34:56Z"
            }
        }
    }


class MovementCreate(FlatMovementMessage):
    """Пока просто наследуется от FlatMovementMessage"""
    pass


class SendMovementResponse(BaseModel):
    status: str = Field(..., example="message sent", description="Статус отправки сообщения в Kafka")


class MovementInfoResponse(BaseModel):
    movement_id: UUID = Field(..., description="ID перемещения")
    from_warehouse: UUID = Field(..., description="ID отправителя")
    to_warehouse: UUID = Field(..., description="ID получателя")
    time_diff_seconds: float = Field(..., description="Разница во времени между отправкой и приемкой (в секундах)")
    quantity_difference: int = Field(..., description="Разница в количестве между отправкой и приемкой")

    model_config = {
        "from_attributes": True,
        "json_schema_extra": {
            "example": {
                "movement_id": "29a1adda-8c55-46f2-a2a5-644bd03d6db9",
                "from_warehouse": "ce6e83c5-f734-4981-bfe5-d3c1dc45350d",
                "to_warehouse": "b77151af-7b0c-42da-858a-ba758c9ada0e",
                "time_diff_seconds": 6.5,
                "quantity_difference": 0
            }
        }
    }