from uuid import UUID
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.models.movement import Movement, MovementEvent
from app.models.warehouse_state import WarehouseState
from app.schemas.movement import KafkaMovementMessage, MovementInfoResponse
from app.services.exceptions import InventoryError


async def create_movement(db: AsyncSession, movement: KafkaMovementMessage) -> Movement | None:
    data = movement.data
    # строка movements = поля конверта + поля data; id сообщения кладём в message_id,
    # а PK генерируется отдельно (default uuid4)
    payload = movement.model_dump(exclude={"data", "id"})
    payload["message_id"] = movement.id
    payload.update(data.model_dump())

    # идемпотентность: повторная доставка того же события (movement_id, event)
    # ничего не вставляет и, что важнее, не трогает остаток второй раз
    insert_stmt = (
        pg_insert(Movement)
        .values(**payload)
        .on_conflict_do_nothing(index_elements=["movement_id", "event"])
        .returning(Movement.id)
    )
    result = await db.execute(insert_stmt)
    inserted = result.scalar_one_or_none()

    if inserted is None:
        # такое событие уже обработано ранее — тихо выходим
        await db.commit()
        return None

    if data.event == MovementEvent.arrival:
        # атомарный upsert-инкремент, чтобы параллельные приёмки не перетирали друг друга
        stock_stmt = (
            pg_insert(WarehouseState)
            .values(
                warehouse_id=data.warehouse_id,
                product_id=data.product_id,
                quantity=data.quantity,
            )
            .on_conflict_do_update(
                index_elements=["warehouse_id", "product_id"],
                set_={"quantity": WarehouseState.quantity + data.quantity},
            )
        )
        await db.execute(stock_stmt)

    elif data.event == MovementEvent.departure:
        # блокируем строку остатка на время чтения-изменения, иначе два departure
        # могут списать с одного и того же значения (double spend)
        query = (
            select(WarehouseState)
            .where(
                WarehouseState.warehouse_id == data.warehouse_id,
                WarehouseState.product_id == data.product_id,
            )
            .with_for_update()
        )
        state = (await db.execute(query)).scalar_one_or_none()
        if state is None:
            raise InventoryError("No stock record found for departure")
        if state.quantity < data.quantity:
            raise InventoryError("Insufficient stock for departure")
        state.quantity -= data.quantity

    await db.commit()
    new_movement = await db.get(Movement, inserted)
    return new_movement


async def get_movement_info(movement_id: UUID, db: AsyncSession) -> MovementInfoResponse:
    query = select(Movement).where(Movement.movement_id == movement_id)
    result = await db.execute(query)
    movements = result.scalars().all()

    if not movements:
        raise HTTPException(status_code=404, detail="Movement not found")

    # благодаря UNIQUE(movement_id, event) в паре не больше одной отправки и одной приёмки
    movement_map = {m.event: m for m in movements}
    departure = movement_map.get(MovementEvent.departure)
    arrival = movement_map.get(MovementEvent.arrival)

    # изменение времени и изменение кол-ва считаем только когда известны обе стороны пары;
    # пока пришло лишь одно событие, перемещение ещё «в пути» — отдаём частичные данные
    time_diff_seconds = None
    quantity_difference = None
    if departure is not None and arrival is not None:
        time_diff_seconds = (arrival.timestamp - departure.timestamp).total_seconds()
        quantity_difference = departure.quantity - arrival.quantity

    return MovementInfoResponse(
        movement_id=movement_id,
        from_warehouse=departure.warehouse_id if departure else None,
        to_warehouse=arrival.warehouse_id if arrival else None,
        time_diff_seconds=time_diff_seconds,
        quantity_difference=quantity_difference,
    )