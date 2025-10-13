import json
import logging

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import settings
from app.schemas.movement import MovementCreate
from app.db.database import get_session_context
from app.services.exceptions import InventoryError
from app.services.movement import create_movement

logger = logging.getLogger(__name__)


class KafkaConsumer:
    def __init__(self):
        self._consumer: AIOKafkaConsumer | None = None
        self._running = False

    async def start(self):
        if self._consumer is None:
            self._consumer = AIOKafkaConsumer(
                settings.KAFKA_TOPIC,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                group_id="movement-consumer-group"
            )
            await self._consumer.start()
            logger.info("✅ Kafka consumer started")
            self._running = True

    async def stop(self):
        if self._consumer is not None:
            await self._consumer.stop()
            logger.info("⛔ Kafka consumer stopped")
            self._consumer = None
            self._running = False

    async def consume(self):
        if not self._consumer:
            raise RuntimeError("❌ Kafka consumer is not started")

        logger.info("✅ Kafka consumer listening for messages...")

        try:
            async for msg in self._consumer:
                raw_data = msg.value

                if not isinstance(raw_data, dict):
                    logger.error(f"❌ Invalid JSON structure for message {msg.offset}, expected dict")
                    continue

                data = raw_data.pop("data", {})
                flat_data = {**raw_data, **data}

                try:
                    movement = MovementCreate(**flat_data)
                except Exception as e:
                    logger.error(f"❌ Validation error for message {raw_data.get('id', msg.offset)}: {e}")
                    continue

                try:
                    async with get_session_context() as session:
                        await create_movement(session, movement)
                except InventoryError as e:
                    logger.error(f"❌ Inventory error for message {raw_data.get('id', msg.offset)}: {e}")
                    continue
                except SQLAlchemyError as e:
                    logger.exception(f"❌ Database error for message {raw_data.get('id', msg.offset)}: {e}")
                    continue
                except Exception as e:
                    logger.exception(f"❌ Unexpected error for message {raw_data.get('id', msg.offset)}: {e}")
                    continue

                await self._consumer.commit()
                logger.info(f"✅ Successfully processed message {raw_data.get('id', msg.offset)}")

        except KafkaError as e:
            logger.exception(f"❌ Kafka error during consumer operation: {e}")
        except Exception as e:
            logger.exception(f"❌ Unexpected error in Kafka consumer: {e}")