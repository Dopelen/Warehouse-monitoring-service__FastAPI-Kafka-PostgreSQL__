from uuid import UUID
from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.warehouse_state import WarehouseState
from app.schemas.warehouse import WarehouseStockResponse

async def get_warehouse_stock(
    db: AsyncSession,
    warehouse_id: UUID,
    product_id: UUID,
) -> WarehouseStockResponse:
    query = select(WarehouseState).where(
        WarehouseState.warehouse_id == warehouse_id,
        WarehouseState.product_id == product_id,
    )
    result = await db.execute(query)
    state = result.scalar_one_or_none()
    if state is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Склад или товар не найдены"
        )

    return WarehouseStockResponse.model_validate(state)