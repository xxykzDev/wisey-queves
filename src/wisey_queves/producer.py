import os
import json
import logging
from aiokafka import AIOKafkaProducer
from wisey_telemetry.telemetry import get_tracer, start_trace_span

logger = logging.getLogger(__name__)
tracer = get_tracer("base-kafka-producer")


class BaseKafkaProducer:
    def __init__(self, bootstrap_servers: str = None):
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

    async def start(self):
        await self.producer.start()
        logger.info("🚀 Kafka producer started")

    async def stop(self):
        await self.producer.stop()
        logger.info("🛑 Kafka producer stopped")

    async def send(self, topic: str, value: dict, key: str | None = None):
        with start_trace_span("kafka.produce", {
            "messaging.system": "kafka",
            "messaging.destination": topic,
            "messaging.kafka.message_key": key,
        }) as span:
            try:
                await self.producer.send_and_wait(topic, value, key=key)
                logger.info(f"📤 Message sent to topic {topic} with key={key}: {value}")
                span.add_event("Message sent", attributes={
                    "topic": topic,
                    "key": key,
                    "value": value,
                })
            except Exception as e:
                logger.exception(f"💥 Failed to send message to {topic}: {e}")
                span.record_exception(e)
