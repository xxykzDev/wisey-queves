import os
import json
import logging
import asyncio
from typing import Optional, Dict, Any
from aiokafka import AIOKafkaProducer
from aiokafka.errors import ProducerClosed, KafkaConnectionError

logger = logging.getLogger(__name__)

# Try to import telemetry, but don't fail if not available
try:
    from wisey_telemetry.telemetry import get_tracer, start_trace_span
    tracer = get_tracer("base-kafka-producer")
    telemetry_available = True
except ImportError:
    telemetry_available = False
    tracer = None
    # Create a dummy context manager for when telemetry is not available
    from contextlib import contextmanager
    @contextmanager
    def start_trace_span(name, attrs=None):
        class DummySpan:
            def add_event(self, *args, **kwargs): pass
            def record_exception(self, *args, **kwargs): pass
        yield DummySpan()

# Try to import metrics, but don't fail if not available
try:
    from wisey_telemetry.kafka_metrics import get_kafka_metrics
    metrics = get_kafka_metrics()
except ImportError:
    metrics = None
    logger.warning("Kafka metrics not available - install wisey-telemetry with metrics support")


class BaseKafkaProducer:
    def __init__(self, bootstrap_servers: str = None):
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self.producer: Optional[AIOKafkaProducer] = None
        self._lock = asyncio.Lock()
        self._is_started = False
        self._retry_count = 0
        self._max_retries = 3
        self._retry_delay = 1.0  # seconds
        
    async def _create_producer(self) -> AIOKafkaProducer:
        """Create a new Kafka producer instance"""
        return AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            enable_idempotence=True,  # Ensure exactly-once delivery
            request_timeout_ms=30000,
            retry_backoff_ms=100,
            metadata_max_age_ms=300000,
        )

    async def start(self):
        """Start the Kafka producer with connection management"""
        async with self._lock:
            if self._is_started and self.producer:
                try:
                    # Check if producer is still healthy
                    await self.producer._sender.sender_task
                except Exception:
                    # Producer is not healthy, recreate it
                    logger.warning("🔄 Producer unhealthy, recreating...")
                    await self._stop_producer()
                else:
                    # Producer is healthy, no need to restart
                    return
            
            try:
                self.producer = await self._create_producer()
                await self.producer.start()
                self._is_started = True
                self._retry_count = 0
                logger.info("🚀 Kafka producer started successfully")
                if metrics:
                    metrics.set_producer_health(True)
            except Exception as e:
                logger.error(f"💥 Failed to start Kafka producer: {e}")
                self._is_started = False
                if metrics:
                    metrics.set_producer_health(False)
                raise

    async def _stop_producer(self):
        """Internal method to stop the producer"""
        if self.producer:
            try:
                await self.producer.stop()
            except Exception as e:
                logger.warning(f"⚠️ Error stopping producer: {e}")
            finally:
                self.producer = None
                self._is_started = False

    async def stop(self):
        """Stop the Kafka producer"""
        async with self._lock:
            await self._stop_producer()
            logger.info("🛑 Kafka producer stopped")

    async def _ensure_connected(self):
        """Ensure the producer is connected and healthy"""
        if not self._is_started or not self.producer:
            await self.start()
            return
        
        # Check if producer is still healthy
        try:
            # Quick health check - this will fail if producer is closed
            if self.producer._closed:
                raise ProducerClosed()
        except (AttributeError, ProducerClosed):
            logger.warning("🔄 Producer closed, reconnecting...")
            if metrics:
                metrics.record_reconnection()
            await self.start()

    async def send(self, topic: str, value: dict, key: str | None = None):
        """Send a message with automatic reconnection and retry logic"""
        with start_trace_span("kafka.produce", {
            "messaging.system": "kafka",
            "messaging.destination": topic,
            "messaging.kafka.message_key": key,
        }) as span:
            
            for attempt in range(self._max_retries):
                try:
                    # Ensure we're connected
                    await self._ensure_connected()
                    
                    # Send the message
                    await self.producer.send_and_wait(topic, value, key=key)
                    logger.info(f"📤 Message sent to topic {topic} with key={key}: {value}")
                    span.add_event("Message sent", attributes={
                        "topic": topic,
                        "key": key,
                        "value": str(value),  # Convert to string for telemetry
                    })
                    if metrics and attempt > 0:
                        metrics.record_retry_attempts(attempt + 1, topic)
                    return  # Success, exit the retry loop
                    
                except ProducerClosed as e:
                    logger.warning(f"⚠️ Producer closed on attempt {attempt + 1}/{self._max_retries}: {e}")
                    if attempt < self._max_retries - 1:
                        # Try to reconnect
                        await asyncio.sleep(self._retry_delay * (attempt + 1))
                        try:
                            await self.start()
                        except Exception as start_error:
                            logger.error(f"💥 Failed to restart producer: {start_error}")
                    else:
                        logger.error(f"💥 Failed to send message after {self._max_retries} attempts")
                        span.record_exception(e)
                        raise
                        
                except KafkaConnectionError as e:
                    logger.warning(f"⚠️ Kafka connection error on attempt {attempt + 1}/{self._max_retries}: {e}")
                    if attempt < self._max_retries - 1:
                        await asyncio.sleep(self._retry_delay * (attempt + 1))
                        try:
                            await self.start()
                        except Exception as start_error:
                            logger.error(f"💥 Failed to restart producer: {start_error}")
                    else:
                        logger.error(f"💥 Kafka connection failed after {self._max_retries} attempts")
                        span.record_exception(e)
                        raise
                        
                except Exception as e:
                    logger.error(f"💥 Unexpected error sending message to {topic}: {e}")
                    span.record_exception(e)
                    if attempt < self._max_retries - 1:
                        await asyncio.sleep(self._retry_delay * (attempt + 1))
                    else:
                        raise

    async def send_batch(self, messages: list[tuple[str, dict, Optional[str]]]):
        """Send multiple messages in a batch
        
        Args:
            messages: List of tuples (topic, value, key)
        """
        await self._ensure_connected()
        
        batch = self.producer.create_batch()
        
        for topic, value, key in messages:
            metadata = batch.append(
                key=key.encode("utf-8") if key else None,
                value=json.dumps(value).encode("utf-8"),
                timestamp=None
            )
            if metadata is None:
                # Batch is full, send it and create a new one
                await self.producer.send_batch(batch, topic)
                batch = self.producer.create_batch()
                batch.append(
                    key=key.encode("utf-8") if key else None,
                    value=json.dumps(value).encode("utf-8"),
                    timestamp=None
                )
        
        # Send remaining messages
        if batch.record_count() > 0:
            await self.producer.send_batch(batch, topic)
        
        logger.info(f"📤 Batch of {len(messages)} messages sent")

    async def health_check(self) -> bool:
        """Check if the producer is healthy"""
        try:
            await self._ensure_connected()
            return True
        except Exception as e:
            logger.error(f"❌ Health check failed: {e}")
            return False