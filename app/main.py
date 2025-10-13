from app.logging_config import setup_logging
import logging
import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text
from app.core.config import settings
from app.db.database import get_session_context
from app.kafka.consumer import KafkaConsumer
from app.kafka.producer import KafkaProducer
from app.kafka.utils import wait_for_kafka_ready, create_topic_if_not_exists
from app.api.routes import api_router
from app.db.base import Base
from app.db.database import engine
from app.services.exceptions import InventoryError

setup_logging()
logger = logging.getLogger(__name__)


async def lifespan(app: FastAPI):
    logger.info("🔄 Application startup in progress...")
    logger.info("🔄 Opening DB session")
    try:
        async with get_session_context() as session:
            await session.execute(text("SELECT 1"))
        logger.info("✅ DB is available")
    except Exception as e:
        logger.error(f"❌ DB connection failed: {str(e)}", exc_info=True)
        raise

    logger.info("🔄 Waiting for Kafka to become available...")
    await wait_for_kafka_ready()
    logger.info(f"🔄 Ensuring Kafka topic '{settings.KAFKA_TOPIC}' exists...")
    await create_topic_if_not_exists()

    kafka_producer = KafkaProducer()
    await kafka_producer.start()
    app.state.kafka_producer = kafka_producer

    kafka_consumer = KafkaConsumer()
    await kafka_consumer.start()
    app.state.kafka_consumer = kafka_consumer

    consumer_task = asyncio.create_task(kafka_consumer.consume())
    app.state.kafka_consumer_task = consumer_task

    yield

    logger.info("🔄 Application shutdown in progress...")

    if hasattr(app.state, 'kafka_consumer_task'):
        app.state.kafka_consumer_task.cancel()
        try:
            await app.state.kafka_consumer_task
        except asyncio.CancelledError:
            pass

    if hasattr(app.state, 'kafka_consumer'):
        await app.state.kafka_consumer.stop()

    if hasattr(app.state, 'kafka_producer'):
        await app.state.kafka_producer.stop()


# Создаем приложение с lifespan менеджером
app = FastAPI(
    title=settings.PROJECT_NAME,
    description="Микросервис для мониторинга состояния складов и товаров на них",
    version="1.0.0",
    lifespan=lifespan
)

@app.exception_handler(InventoryError)
async def inventory_error_handler(request: Request, exc: InventoryError):
    return JSONResponse(
        status_code=400,
        content={"detail": exc.message},
    )

app.include_router(api_router, prefix="/api")