import json
import logging
from aiokafka import AIOKafkaProducer
from app.core.config import settings
from app.schemas.movement import KafkaMovementMessage

logger = logging.getLogger(__name__)

class KafkaProducer:
    def __init__(self):
        self._producer: AIOKafkaProducer | None = None

    async def start(self):
        if self._producer is None:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
            )
            await self._producer.start()
            logger.info("✅ Kafka producer started")

    async def stop(self):
        if self._producer is not None:
            await self._producer.stop()
            logger.info("⛔ Kafka producer stopped")
            self._producer = None

    async def send_movement(self, movement: KafkaMovementMessage):
        if self._producer is None:
            raise RuntimeError("Kafka producer is not started")
        message = json.dumps(movement.model_dump(mode="json")).encode("utf-8")
        await self._producer.send_and_wait(settings.KAFKA_TOPIC, message)
        logger.info(f"📤 Kafka message with ID {movement.id} was sent")