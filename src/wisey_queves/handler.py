import logging
import asyncio
from wisey_queves.consumer import BaseKafkaConsumer
from wisey_telemetry.telemetry import get_tracer, start_trace_span

logger = logging.getLogger(__name__)
tracer = get_tracer("kafka-event-handler")


class KafkaEventHandler:
    def __init__(self, topic: str, bootstrap_servers: str = None, group_id: str = "default-handler-group"):
        self.consumer = BaseKafkaConsumer(topic, bootstrap_servers, group_id)

    async def start(self):
        await self.consumer.start()
        try:
            async for message in self.consumer.get_messages():
                with start_trace_span("kafka.handler.process", {
                    "messaging.destination": self.consumer.topic,
                    "handler.class": self.__class__.__name__,
                }) as span:
                    try:
                        if await self.can_handle(message):
                            span.add_event("can_handle returned True")
                            await self.attempt(message)
                            span.add_event("attempt completed")
                        else:
                            span.add_event("can_handle returned False")
                    except Exception as e:
                        logger.exception(f"❌ Error while handling message: {e}")
                        span.record_exception(e)
        except Exception as e:
            logger.exception(f"🔥 Kafka handler crashed: {e}")
        finally:
            await self.consumer.stop()

    async def can_handle(self, message) -> bool:
        raise NotImplementedError("⚠️ Must implement can_handle()")

    async def attempt(self, message) -> None:
        raise NotImplementedError("⚠️ Must implement attempt()")
