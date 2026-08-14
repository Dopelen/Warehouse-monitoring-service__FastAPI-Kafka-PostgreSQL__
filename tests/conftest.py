import uuid
from datetime import datetime, timezone, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.config import settings
from app.schemas.movement import KafkaMovementData, KafkaMovementMessage

# Тесты гоняем против той же мигрированной БД, что поднимает docker-compose,
# поэтому проверяются и Postgres-специфика (ON CONFLICT, FOR UPDATE), и сама миграция.
# NullPool обязателен: pytest-asyncio даёт свой event loop на каждый тест, а
# пулированные asyncpg-соединения нельзя переиспользоватьду меж циклами.
engine = create_async_engine(settings.DATABASE_URL, echo=False, poolclass=NullPool)
TestSession = async_sessionmaker(engine, expire_on_commit=False)


@pytest_asyncio.fixture
async def db():
    # чистим таблицы перед каждым тестом, чтобы они не влияли друг на друга
    async with TestSession() as session:
        await session.execute(text("TRUNCATE movements, warehouse_states"))
        await session.commit()
        yield session


def make_movement(
    *,
    event: str,
    movement_id: uuid.UUID | None = None,
    warehouse_id: uuid.UUID | None = None,
    product_id: uuid.UUID | None = None,
    quantity: int = 100,
    timestamp: datetime | None = None,
    message_id: uuid.UUID | None = None,
) -> KafkaMovementMessage:
    return KafkaMovementMessage(
        id=message_id or uuid.uuid4(),
        source="WH-3423",
        specversion="1.0",
        type="ru.retail.warehouses.movement",
        datacontenttype="application/json",
        dataschema="ru.retail.warehouses.movement.v1.0",
        time=1737439421623,
        subject="WH-3423:MOVEMENT",
        destination="ru.retail.warehouses",
        data=KafkaMovementData(
            movement_id=movement_id or uuid.uuid4(),
            warehouse_id=warehouse_id or uuid.uuid4(),
            product_id=product_id or uuid.uuid4(),
            quantity=quantity,
            event=event,
            timestamp=timestamp or datetime.now(timezone.utc),
        ),
    )
