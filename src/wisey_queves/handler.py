import logging
import asyncio
import sys
from wisey_queves.consumer import BaseKafkaConsumer
from wisey_telemetry.telemetry import get_tracer, start_trace_span

logger = logging.getLogger(__name__)
tracer = get_tracer("kafka-event-handler")


class KafkaEventHandler:
    def __init__(self, topic: str, bootstrap_servers: str = None, group_id: str = "default-handler-group"):
        self.consumer = BaseKafkaConsumer(topic, bootstrap_servers, group_id)

    async def start(self):
        await self.consumer.start()
        logger.info(f"📡 Handler started for topic: {self.consumer.topic}")
        print(f"📡 Handler started for topic: {self.consumer.topic}", flush=True)
        sys.stdout.flush()
        
        try:
            logger.info("🔄 Entering message consumption loop...")
            print("🔄 Entering message consumption loop...", flush=True)
            sys.stdout.flush()
            
            message_count = 0
            async for message in self.consumer.get_messages():
                message_count += 1
                logger.info(f"📨 Received message #{message_count}: {type(message)}")
                print(f"📨 Received message #{message_count}: {type(message)}")
                
                # Skip None messages (tombstones)
                if message is None:
                    logger.info("📭 Skipping None/tombstone message")
                    print("📭 Skipping None/tombstone message")
                    continue
                
                with start_trace_span("kafka.handler.process", {
                    "messaging.destination": self.consumer.topic,
                    "handler.class": self.__class__.__name__,
                }) as span:
                    try:
                        logger.info(f"🎯 Calling can_handle for message #{message_count}")
                        print(f"🎯 Calling can_handle for message #{message_count}")
                        
                        if await self.can_handle(message):
                            span.add_event("can_handle returned True")
                            logger.info(f"✅ can_handle returned True for message #{message_count}")
                            print(f"✅ can_handle returned True for message #{message_count}")
                            
                            await self.attempt(message)
                            span.add_event("attempt completed")
                            logger.info(f"✅ attempt completed for message #{message_count}")
                            print(f"✅ attempt completed for message #{message_count}")
                        else:
                            span.add_event("can_handle returned False")
                            logger.info(f"❌ can_handle returned False for message #{message_count}")
                            print(f"❌ can_handle returned False for message #{message_count}")
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
