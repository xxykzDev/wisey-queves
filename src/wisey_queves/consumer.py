import os
import json
import logging
from aiokafka import AIOKafkaConsumer
from wisey_telemetry.telemetry import get_tracer, start_trace_span

logger = logging.getLogger(__name__)
tracer = get_tracer("kafka-consumer")


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
            value_deserializer=self._safe_deserializer,
        )

    def _safe_deserializer(self, message_value):
        """
        Safe deserializer that handles None/null values from Kafka
        
        Args:
            message_value: Raw bytes from Kafka message (can be None for tombstone messages)
            
        Returns:
            Parsed JSON dict or None for null messages
        """
        if message_value is None:
            logger.debug(f"📭 Received null/tombstone message from Kafka topic: {self.topic}")
            return None
            
        try:
            return json.loads(message_value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            logger.warning(f"⚠️ Failed to deserialize Kafka message on topic {self.topic}: {e}")
            return None
        except Exception as e:
            logger.error(f"💥 Unexpected error deserializing message on topic {self.topic}: {e}")
            return None

    async def start(self):
        await self.consumer.start()
        logger.info(f"📡 Kafka consumer started on topic: {self.topic}")

    async def stop(self):
        await self.consumer.stop()
        logger.info("🛑 Kafka consumer stopped")

    async def get_messages(self):
        async for msg in self.consumer:
            with start_trace_span("kafka.consume", {
                "messaging.system": "kafka",
                "messaging.destination": self.topic,
                "messaging.kafka.message_key": msg.key.decode("utf-8") if msg.key else None,
                "messaging.kafka.partition": msg.partition,
                "messaging.kafka.offset": msg.offset,
            }) as span:
                span.add_event("Kafka message received", attributes={
                    "topic": self.topic,
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "key": msg.key.decode("utf-8") if msg.key else None,
                })
                yield msg.value
