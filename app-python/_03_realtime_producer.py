"""
Realtime user producer
Generates user documents and publishes directly to Kafka
"""

import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timezone
import time

import structlog
from _01_data_generator import UserDataGenerator
from _02_kafka_producer import KafkaManager

logger = structlog.get_logger()


class RealtimeDataProducer:
    def __init__(self, kafka_bootstrap_servers: str, sleep_rate: int, users_topic: str, num_user: int, num_app: int):
        self.kafka_manager = KafkaManager(kafka_bootstrap_servers) if kafka_bootstrap_servers else None
        self.data_generator = UserDataGenerator(num_user=num_user, num_app=num_app)
        self.sleep_rate = sleep_rate
        self.users_topic = users_topic
        self.is_running = False
        self.stats = { 'events_sent': 0, 'errors': 0, 'start_time': None }
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info("Received shutdown signal, stopping producer...", signal=signum)
        self.stop()

    async def initialize(self):
        logger.info("Initializing realtime user producer...")
        if self.kafka_manager:
            try:
                self.kafka_manager.ensure_topic(self.users_topic, num_partitions=4)
            except Exception as e:
                logger.warning("Failed to ensure Kafka topic during init", error=str(e))

    async def _generate_and_send_user(self):
        try:
            user_doc = self.data_generator.generate_user_submission()
            if self.kafka_manager:
                self.kafka_manager.send_message(user_doc, self.users_topic)
            self.stats['events_sent'] += 1
        except Exception as e:
            logger.error("Failed to generate user", error=str(e), error_type=type(e).__name__)
            self.stats['errors'] += 1

    async def start(self):
        if self.is_running:
            logger.warning("Producer is already running")
            return
        logger.info("Starting realtime user producer", rate=self.sleep_rate, topic=self.users_topic)
        self.is_running = True
        self.stats['start_time'] = datetime.now(timezone.utc)
        tasks = [asyncio.create_task(self._loop())]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Producer tasks cancelled")
        except Exception as e:
            logger.error("Producer error", error=str(e))
            raise
        finally:
            self.is_running = False

    async def _loop(self):
        last_log_time = time.time()
        while self.is_running:
            try:
                current_rate = self.sleep_rate
                interval = 1.0 / current_rate if current_rate > 0 else 1.0
                await self._generate_and_send_user()
                await asyncio.sleep(interval)
                current_time = time.time()
                if current_time - last_log_time >= 5.0:
                    logger.info("Events sent (last 5s)", total_events=self.stats['events_sent'])
                    last_log_time = current_time
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in user generator loop", error=str(e))
                await asyncio.sleep(1)

    def stop(self):
        logger.info("Stopping realtime user producer...")
        self.is_running = False
        if self.kafka_manager:
            self.kafka_manager.close()
        logger.info("Producer stopped", total_events=self.stats['events_sent'])


