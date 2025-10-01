"""
Kafka utilities for the realtime user producer
"""

import json
from typing import Dict, Any, Optional
import time

import structlog
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions


logger = structlog.get_logger()


class KafkaManager:
    """Manages Kafka producer for direct data streaming"""
    
    def __init__(self, bootstrap_servers: str, poll_interval_ms: int = 50):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self._admin: Optional[AdminClient] = None
        self._ensured_topics: set = set()
        # periodic polling controls
        self._poll_interval_ms = max(0, int(poll_interval_ms))
        self._last_poll_ts = time.monotonic()
        self._initialize_producer()
    
    def _initialize_producer(self):
        """Initialize Confluent Kafka producer (librdkafka)"""
        try:
            conf = {
                'bootstrap.servers': self.bootstrap_servers,
                'client.id': 'fintech-python-producer',
                # batching
                'linger.ms': 200,
                'batch.size': 2048*1024,
                # compression
                'compression.type': 'lz4',
                # reliability / retries
                'acks': '0',
                'retries': 3,
                'retry.backoff.ms': 100,
                # buffering
                'queue.buffering.max.kbytes': 655360,
                'queue.buffering.max.messages': 1000000,
                # timeouts
                'request.timeout.ms': 30000,
            }
            self.producer = Producer(conf)
            logger.info("Confluent Kafka producer initialized", bootstrap_servers=self.bootstrap_servers)
        except Exception as e:
            logger.error("Failed to initialize Confluent Kafka producer", error=str(e))
            raise
    
    def _get_admin(self) -> AdminClient:
        if self._admin is None:
            self._admin = AdminClient({'bootstrap.servers': self.bootstrap_servers})
        return self._admin

    def ensure_topic(self, topic: str, num_partitions: int = 4, replication_factor: int = 1, timeout: float = 10.0):
        """Ensure topic exists with at least num_partitions. Create or increase partitions if needed."""
        try:
            if topic in self._ensured_topics:
                return
            admin = self._get_admin()
            md = admin.list_topics(timeout=timeout)
            if topic not in md.topics:
                new_topic = NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)
                futures = admin.create_topics([new_topic])
                f = futures.get(topic)
                try:
                    f.result()
                    logger.info("Kafka topic created", topic=topic, partitions=num_partitions)
                except Exception as e:
                    if 'already exists' not in str(e).lower():
                        raise
            else:
                current_partitions = len(md.topics[topic].partitions)
                if current_partitions < num_partitions:
                    futures = admin.create_partitions({topic: NewPartitions(num_partitions)})
                    try:
                        futures[topic].result()
                        logger.info("Kafka topic partitions increased", topic=topic, from_partitions=current_partitions, to_partitions=num_partitions)
                    except Exception as e:
                        if 'is already' not in str(e).lower():
                            raise
            self._ensured_topics.add(topic)
        except Exception as e:
            logger.error("Failed to ensure Kafka topic", topic=topic, error=str(e))
            raise
    
    def _delivery_report(self, err, msg):
        if err is not None:
            logger.error("Failed to send message to Kafka", error=str(err))
        else:
            logger.debug("Message sent successfully", topic=msg.topic(), partition=msg.partition(), offset=msg.offset())
    
    def _maybe_poll(self, force: bool = False):
        if not self.producer:
            return
        # Called after produce attempts to advance delivery callbacks periodically
        now = time.monotonic()
      
        should_poll_by_time = (now - self._last_poll_ts) * 1000.0 >= self._poll_interval_ms
       
        if force or should_poll_by_time:
            self.producer.poll(0)
            self._last_poll_ts = now
     
    def send_message(self, payload: Dict[str, Any], topic: str):
        """Send a JSON message to Kafka topic"""
        try:
            if not self.producer:
                self._initialize_producer()
            key = (payload.get('uid') or "").encode('utf-8')
            value_bytes = json.dumps(payload, default=str).encode('utf-8')
            self.producer.produce(topic, key=key, value=value_bytes, on_delivery=self._delivery_report)
            self._maybe_poll()
        except BufferError as e:
            logger.warning("Producer queue full, polling to drain", error=str(e))
            self._maybe_poll(force=True)
            self.producer.produce(topic, key=key, value=value_bytes, on_delivery=self._delivery_report)
        except KafkaException as e:
            logger.error("Kafka exception while sending message", error=str(e))
            raise
        except Exception as e:
            logger.error("Failed to send message to Kafka", error=str(e))
            raise

    def send_message_bytes(self, topic: str, key: bytes, value: bytes):
        """Send a message where key/value are already bytes (avoids re-serialization)."""
        try:
            if not self.producer:
                self._initialize_producer()
            self.producer.produce(topic, key=key, value=value, on_delivery=self._delivery_report)
            self._maybe_poll()
        except BufferError as e:
            logger.warning("Producer queue full, polling to drain", error=str(e))
            self._maybe_poll(force=True)
            self.producer.produce(topic, key=key, value=value, on_delivery=self._delivery_report)
        except KafkaException as e:
            logger.error("Kafka exception while sending message", error=str(e))
            raise
        except Exception as e:
            logger.error("Failed to send message bytes to Kafka", error=str(e))
            raise
    
    def flush(self):
        if self.producer:
            self.producer.flush()
    
    def close(self):
        if self.producer:
            try:
                self.producer.flush(5.0)
            finally:
                self.producer = None
                logger.info("Confluent Kafka producer closed")

