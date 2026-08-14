import uuid
from datetime import datetime, timezone, timedelta

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

from app.main import app
from app.db.database import get_async_session
from app.models.warehouse_state import WarehouseState
from app.services.exceptions import InventoryError
from app.services.movement import create_movement

from tests.conftest import TestSession, make_movement

pytestmark = pytest.mark.asyncio


async def _stock(db, warehouse_id, product_id) -> int | None:
    row = await db.execute(
        select(WarehouseState.quantity).where(
            WarehouseState.warehouse_id == warehouse_id,
            WarehouseState.product_id == product_id,
        )
    )
    return row.scalar_one_or_none()


# --- сервис: остатки -------------------------------------------------------

async def test_arrival_creates_and_increments_stock(db):
    wh, prod = uuid.uuid4(), uuid.uuid4()
    await create_movement(db, make_movement(event="arrival", warehouse_id=wh, product_id=prod, quantity=100))
    await create_movement(db, make_movement(event="arrival", warehouse_id=wh, product_id=prod, quantity=40))
    assert await _stock(db, wh, prod) == 140


async def test_departure_decrements_stock(db):
    wh, prod = uuid.uuid4(), uuid.uuid4()
    await create_movement(db, make_movement(event="arrival", warehouse_id=wh, product_id=prod, quantity=100))
    await create_movement(db, make_movement(event="departure", warehouse_id=wh, product_id=prod, quantity=30))
    assert await _stock(db, wh, prod) == 70


async def test_departure_without_stock_raises(db):
    with pytest.raises(InventoryError):
        await create_movement(db, make_movement(event="departure", quantity=10))


async def test_departure_insufficient_stock_raises(db):
    wh, prod = uuid.uuid4(), uuid.uuid4()
    await create_movement(db, make_movement(event="arrival", warehouse_id=wh, product_id=prod, quantity=5))
    with pytest.raises(InventoryError):
        await create_movement(db, make_movement(event="departure", warehouse_id=wh, product_id=prod, quantity=10))
    assert await _stock(db, wh, prod) == 5


# --- сервис: идемпотентность ----------------------------------------------

async def test_duplicate_event_is_idempotent(db):
    wh, prod, mid = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    first = await create_movement(
        db, make_movement(event="arrival", movement_id=mid, warehouse_id=wh, product_id=prod, quantity=100)
    )
    second = await create_movement(
        db, make_movement(event="arrival", movement_id=mid, warehouse_id=wh, product_id=prod, quantity=100)
    )
    assert first is not None
    assert second is None  # повтор того же события ничего не вставил
    assert await _stock(db, wh, prod) == 100  # и не удвоил остаток


# --- API: информация о перемещении ----------------------------------------

def _client():
    async def _override():
        async with TestSession() as session:
            yield session

    app.dependency_overrides[get_async_session] = _override
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def test_movement_info_full_pair(db):
    mid, prod = uuid.uuid4(), uuid.uuid4()
    wh_from, wh_to = uuid.uuid4(), uuid.uuid4()
    t0 = datetime(2025, 2, 18, 12, 0, 0, tzinfo=timezone.utc)
    # склад-отправитель должен иметь запас, иначе departure отклонится
    await create_movement(db, make_movement(event="arrival", movement_id=uuid.uuid4(),
                                            warehouse_id=wh_from, product_id=prod, quantity=100))
    await create_movement(db, make_movement(event="departure", movement_id=mid,
                                            warehouse_id=wh_from, product_id=prod, quantity=100, timestamp=t0))
    await create_movement(db, make_movement(event="arrival", movement_id=mid,
                                            warehouse_id=wh_to, product_id=prod, quantity=90,
                                            timestamp=t0 + timedelta(seconds=30)))

    async with _client() as client:
        resp = await client.get(f"/api/movements/{mid}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["from_warehouse"] == str(wh_from)
    assert body["to_warehouse"] == str(wh_to)
    assert body["time_diff_seconds"] == 30.0
    assert body["quantity_difference"] == 10


async def test_movement_info_in_transit_is_partial(db):
    mid, wh, prod = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    await create_movement(db, make_movement(event="arrival", warehouse_id=wh, product_id=prod, quantity=100))
    await create_movement(db, make_movement(event="departure", movement_id=mid,
                                            warehouse_id=wh, product_id=prod, quantity=100))
    async with _client() as client:
        resp = await client.get(f"/api/movements/{mid}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["from_warehouse"] == str(wh)
    assert body["to_warehouse"] is None
    assert body["time_diff_seconds"] is None
    assert body["quantity_difference"] is None


async def test_movement_info_not_found(db):
    async with _client() as client:
        resp = await client.get(f"/api/movements/{uuid.uuid4()}")
    assert resp.status_code == 404


# --- API: остаток на складе -----------------------------------------------

async def test_warehouse_stock_endpoint(db):
    wh, prod = uuid.uuid4(), uuid.uuid4()
    await create_movement(db, make_movement(event="arrival", warehouse_id=wh, product_id=prod, quantity=77))
    async with _client() as client:
        resp = await client.get(f"/api/warehouses/{wh}/products/{prod}")
    assert resp.status_code == 200
    assert resp.json()["quantity"] == 77


async def test_warehouse_stock_not_found(db):
    async with _client() as client:
        resp = await client.get(f"/api/warehouses/{uuid.uuid4()}/products/{uuid.uuid4()}")
    assert resp.status_code == 404
