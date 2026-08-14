import asyncio
import logging

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaConnectionError

from app.core.config import settings

logger = logging.getLogger(__name__)

MAX_RETRIES = 10
RETRY_DELAY = 5


async def create_topic_if_not_exists():
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        existing_topics = await admin.list_topics()
        # основной топик и DLQ создаём одинаково, отдельным циклом чтобы не дублировать код
        for topic_name in (settings.KAFKA_TOPIC, settings.KAFKA_DLQ_TOPIC):
            if topic_name not in existing_topics:
                await admin.create_topics([
                    NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
                ])
                logger.info(f"✅ Kafka topic '{topic_name}' created.")
            else:
                logger.info(f"ℹ️ Kafka topic '{topic_name}' already exists.")
    finally:
        await admin.close()


async def wait_for_kafka_ready(max_retries: int = MAX_RETRIES, delay: int = RETRY_DELAY):
    for attempt in range(1, max_retries + 1):
        try:
            admin = AIOKafkaAdminClient(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS)
            await admin.start()
            await admin.list_topics()
            await admin.close()
            logger.info("✅ Kafka is available")
            return
        except KafkaConnectionError as e:
            logger.warning(f"⚠️ Kafka not ready (attempt {attempt}/{max_retries}): {e}")
            await asyncio.sleep(delay)
        except Exception as e:
            logger.exception(f"❌ Unexpected error while waiting for Kafka: {e}")
            await asyncio.sleep(delay)
    raise RuntimeError("❌ Kafka did not become ready in time")