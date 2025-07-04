import os
import json
import logging
from aiokafka import AIOKafkaProducer

logger = logging.getLogger(__name__)


class BaseKafkaProducer:
    def __init__(self, bootstrap_servers: str = None):
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    async def start(self):
        await self.producer.start()
        logger.info("🚀 Kafka producer started")

    async def stop(self):
        await self.producer.stop()
        logger.info("🛑 Kafka producer stopped")

    async def send(self, topic: str, message: dict):
        try:
            await self.producer.send_and_wait(topic, message)
            logger.info(f"📤 Message sent to topic {topic}: {message}")
        except Exception as e:
            logger.exception(f"💥 Failed to send message to {topic}: {e}")
