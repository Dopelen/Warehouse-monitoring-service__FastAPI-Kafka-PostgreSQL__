import asyncio
import json
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import settings
from app.schemas.movement import KafkaMovementMessage
from app.db.database import get_session_context
from app.services.exceptions import InventoryError
from app.services.movement import create_movement

logger = logging.getLogger(__name__)

# сколько раз повторить обработку при транзиентной ошибке БД, прежде чем уронить в DLQ
MAX_DB_RETRIES = 3
RETRY_BACKOFF_BASE = 1  # секунды; растёт экспоненциально: 1, 2, 4...


class KafkaConsumer:
    def __init__(self):
        self._consumer: AIOKafkaConsumer | None = None
        self._dlq_producer: AIOKafkaProducer | None = None
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
            # отдельный продьюсер, чтобы отправлять неудачные сообщения в DLQ
            self._dlq_producer = AIOKafkaProducer(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
            await self._dlq_producer.start()
            logger.info("✅ Kafka consumer started")
            self._running = True

    async def stop(self):
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
        if self._dlq_producer is not None:
            await self._dlq_producer.stop()
            self._dlq_producer = None
        if self._running:
            logger.info("⛔ Kafka consumer stopped")
            self._running = False

    async def _send_to_dlq(self, msg, category: str, reason: str):
        # оборачиваем исходное сообщение метаданными об ошибке и кладём в DLQ-топик
        payload = {
            "original": msg.value,
            "error": {"category": category, "reason": reason},
            "source_topic": msg.topic,
            "source_offset": msg.offset,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }
        await self._dlq_producer.send_and_wait(
            settings.KAFKA_DLQ_TOPIC,
            json.dumps(payload, default=str).encode("utf-8"),
        )
        logger.info(f"📮 Message {msg.offset} parked to DLQ ({category})")

    async def _process(self, msg):
        raw_data = msg.value
        mid = raw_data.get("id", msg.offset) if isinstance(raw_data, dict) else msg.offset

        # --- перманентные ошибки: ретраить бесполезно, сразу в DLQ ---
        if not isinstance(raw_data, dict):
            logger.error(f"❌ Malformed message {msg.offset}: expected JSON object")
            await self._send_to_dlq(msg, "malformed", "expected JSON object")
            return

        try:
            movement = KafkaMovementMessage(**raw_data)
        except Exception as e:
            logger.error(f"❌ Validation error for message {mid}: {e}")
            await self._send_to_dlq(msg, "validation", str(e))
            return

        # --- обработка с ретраем транзиентных ошибок БД ---
        for attempt in range(1, MAX_DB_RETRIES + 1):
            try:
                async with get_session_context() as session:
                    await create_movement(session, movement)
                logger.info(f"✅ Successfully processed message {mid}")
                return
            except InventoryError as e:
                # бизнес-отказ: в DLQ, чтобы можно было переиграть (например при нарушенном порядке событий)
                logger.error(f"❌ Inventory error for message {mid}: {e}")
                await self._send_to_dlq(msg, "inventory", str(e))
                return
            except SQLAlchemyError as e:
                if attempt < MAX_DB_RETRIES:
                    delay = RETRY_BACKOFF_BASE * 2 ** (attempt - 1)
                    logger.warning(
                        f"⚠️ DB error for message {mid} (attempt {attempt}/{MAX_DB_RETRIES}), "
                        f"retry in {delay}s: {e}"
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.exception(f"❌ DB error for message {mid} after {MAX_DB_RETRIES} attempts: {e}")
                await self._send_to_dlq(msg, "db", str(e))
                return
            except Exception as e:
                logger.exception(f"❌ Unexpected error for message {mid}: {e}")
                await self._send_to_dlq(msg, "unexpected", str(e))
                return

    async def consume(self):
        if not self._consumer:
            raise RuntimeError("❌ Kafka consumer is not started")

        logger.info("✅ Kafka consumer listening for messages...")

        try:
            async for msg in self._consumer:
                try:
                    await self._process(msg)
                except Exception as e:
                    # сюда попадаем, только если не удалось даже записать в DLQ (например Kafka недоступна);
                    # offset НЕ коммитим — сообщение переиграется после перезапуска/ребаланса
                    logger.exception(
                        f"❌ Could not park message {msg.offset} to DLQ, offset not committed: {e}"
                    )
                    continue

                # успех или сообщение уехало в DLQ — в обоих случаях данные не потеряны, двигаем offset
                await self._consumer.commit()

        except KafkaError as e:
            logger.exception(f"❌ Kafka error during consumer operation: {e}")
        except Exception as e:
            logger.exception(f"❌ Unexpected error in Kafka consumer: {e}")
