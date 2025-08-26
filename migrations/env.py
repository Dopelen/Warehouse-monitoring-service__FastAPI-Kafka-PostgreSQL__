import asyncio
from logging.config import fileConfig

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from alembic import context
from app.db.base import Base
from app.core.config import settings

# --- Импорт моделей, чтобы Alembic их увидел ---
from app.models.movement import Movement
from app.models.warehouse_state import WarehouseState


# --- Логгирование Alembic ---
config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)


# --- Metadata для Alembic ---
target_metadata = Base.metadata

# --- Режим offline ---
def run_migrations_offline() -> None:
    url = settings.DATABASE_URL
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


# --- Функция синхронного запуска миграций ---
def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


# --- Онлайн режим с async engine ---
async def run_migrations_online() -> None:
    connectable = create_async_engine(
        settings.DATABASE_URL,
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


# --- Точка входа ---
if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())