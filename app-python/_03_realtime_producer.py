"""
Realtime user producer
Generates user documents and writes to MongoDB collection.
Debezium MongoDB connector should capture changes to publish to Kafka topic `leaderboard_update`.
"""

import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timezone
import time

import structlog
from pymongo import MongoClient, errors as pymongo_errors
from _01_data_generator import UserDataGenerator

logger = structlog.get_logger()


class RealtimeDataProducer:
    def __init__(self, mongo_uri: str, mongo_db: str, mongo_collection: str, sleep_rate: int, num_user: int, num_app: int):
        self.data_generator = UserDataGenerator(num_user=num_user, num_app=num_app)
        self.sleep_rate = sleep_rate
        self.is_running = False
        self.stats = { 'events_sent': 0, 'errors': 0, 'start_time': None }
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.client: MongoClient | None = None
        self.collection = None
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info("Received shutdown signal, stopping producer...", signal=signum)
        self.stop()

    async def initialize(self):
        logger.info("Initializing realtime user producer (MongoDB mode)...")
        try:
            self.client = MongoClient(self.mongo_uri, uuidRepresentation='standard')
            db = self.client[self.mongo_db]
            self.collection = db[self.mongo_collection]
            # Ensure useful indexes for Debezium and query patterns
            self.collection.create_index("uid")
            self.collection.create_index("updated_at")
            # Enable pre/post images so Debezium can emit `before` (MongoDB >= 6.0)
            try:
                # Ensure collection exists before collMod (no-op if exists)
                if self.mongo_collection not in db.list_collection_names():
                    db.create_collection(self.mongo_collection)
                db.command({
                    "collMod": self.mongo_collection,
                    "changeStreamPreAndPostImages": {"enabled": True}
                })
                logger.info("Enabled changeStream pre/post images on collection", collection=self.mongo_collection)
            except Exception as e:
                # Log but continue; app still functions without pre/post images
                logger.warning("Could not enable pre/post images on collection", collection=self.mongo_collection, error=str(e))
        except Exception as e:
            logger.error("Failed to initialize MongoDB client", error=str(e))
            raise

    async def _generate_and_send_user(self):
        try:
            user_doc = self.data_generator.generate_user_submission()
            if self.collection is None:
                raise RuntimeError("MongoDB collection is not initialized")
            # Use upsert on uid to simulate user updates over time
            uid = user_doc["uid"]
            self.collection.update_one({"uid": uid}, {"$set": user_doc}, upsert=True)
            self.stats['events_sent'] += 1
        except Exception as e:
            logger.error("Failed to generate user", error=str(e), error_type=type(e).__name__)
            self.stats['errors'] += 1

    async def start(self):
        if self.is_running:
            logger.warning("Producer is already running")
            return
        logger.info("Starting realtime user producer", rate=self.sleep_rate, collection=self.mongo_collection)
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
        if self.client is not None:
            try:
                self.client.close()
            except Exception:
                pass
        logger.info("Producer stopped", total_events=self.stats['events_sent'])


