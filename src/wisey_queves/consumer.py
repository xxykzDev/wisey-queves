import os
import json
import logging
from aiokafka import AIOKafkaConsumer

logger = logging.getLogger(__name__)


class BaseKafkaConsumer:
    def __init__(self, topic: str, bootstrap_servers: str = None, group_id: str = "default-group"):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self.group_id = group_id
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

    async def start(self):
        await self.consumer.start()
        logger.info(f"📡 Kafka consumer started on topic: {self.topic}")

    async def stop(self):
        await self.consumer.stop()
        logger.info("🛑 Kafka consumer stopped")

    async def get_messages(self):
        async for msg in self.consumer:
            yield msg.value
