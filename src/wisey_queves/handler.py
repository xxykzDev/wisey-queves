import logging
import asyncio
from wisey_queves.consumer import BaseKafkaConsumer

logger = logging.getLogger(__name__)


class KafkaEventHandler:
    def __init__(self, topic: str, bootstrap_servers: str = None, group_id: str = "default-handler-group"):
        self.consumer = BaseKafkaConsumer(topic, bootstrap_servers, group_id)

    async def start(self):
        await self.consumer.start()
        try:
            async for message in self.consumer.get_messages():
                if await self.can_handle(message):
                    await self.attempt(message)
        except Exception as e:
            logger.exception(f"🔥 Kafka handler crashed: {e}")
        finally:
            await self.consumer.stop()

    async def can_handle(self, message) -> bool:
        raise NotImplementedError("⚠️ Must implement can_handle()")

    async def attempt(self, message) -> None:
        raise NotImplementedError("⚠️ Must implement attempt()")
