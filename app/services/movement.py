from uuid import UUID
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models.movement import Movement, MovementEvent
from app.models.warehouse_state import WarehouseState
from app.schemas.movement import MovementCreate, MovementInfoResponse
from app.services.exceptions import InventoryError
from datetime import timedelta


async def create_movement(db: AsyncSession, movement: MovementCreate) -> Movement:
    new_movement = Movement(**movement.dict())
    db.add(new_movement)
    query = select(WarehouseState).where(
        WarehouseState.warehouse_id == movement.warehouse_id,
        WarehouseState.product_id == movement.product_id,
    )
    result = await db.execute(query)
    state = result.scalar_one_or_none()

    if movement.event == MovementEvent.arrival:
        if state is None:
            state = WarehouseState(
                warehouse_id=movement.warehouse_id,
                product_id=movement.product_id,
                quantity=0,
            )
            db.add(state)
        state.quantity += movement.quantity

    elif movement.event == MovementEvent.departure:
        if state is None:
            raise InventoryError("No stock record found for departure")
        if state.quantity < movement.quantity:
            raise InventoryError("Insufficient stock for departure")
        state.quantity -= movement.quantity

    await db.commit()
    await db.refresh(new_movement)
    return new_movement


async def get_movement_info(movement_id: UUID, db: AsyncSession) -> MovementInfoResponse:
    query = select(Movement).where(Movement.movement_id == movement_id)
    result = await db.execute(query)
    movements = result.scalars().all()

    if len(movements) != 2:
        raise HTTPException(status_code=404, detail="Movement pair not found")

    movement_map = {m.event.value: m for m in movements}  # используем .value для Enum
    if "departure" not in movement_map or "arrival" not in movement_map:
        raise HTTPException(status_code=400, detail="Invalid movement pair")

    departure = movement_map["departure"]
    arrival = movement_map["arrival"]

    time_diff = arrival.timestamp - departure.timestamp
    quantity_diff = departure.quantity - arrival.quantity

    return MovementInfoResponse.model_validate({
        "movement_id": movement_id,
        "from_warehouse": departure.warehouse_id,
        "to_warehouse": arrival.warehouse_id,
        "time_diff_seconds": time_diff.total_seconds(),
        "quantity_difference": quantity_diff,
    })