"""
Main entry point for the realtime user producer (MongoDB -> Debezium CDC)
"""

import asyncio
import argparse
import logging
import sys
import os
import json
from datetime import datetime, timezone
import structlog

from _03_realtime_producer import RealtimeDataProducer
from _00_config import config


def setup_logging(level: str = "INFO"):
    """Setup structured logging"""
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    logging.basicConfig(level=getattr(logging, level.upper()))


# MongoDB is used as the sink; Debezium should capture CDC to Kafka



async def start_producer(rate: int, num_user: int, num_app: int):
    """Start the realtime user producer writing to MongoDB"""
    logger = structlog.get_logger()
    logger.info("Starting realtime user producer...", rate=rate, mongo_uri=config.MONGO_URI, db=config.MONGO_DB, collection=config.MONGO_COLLECTION)
    producer = RealtimeDataProducer(mongo_uri=config.MONGO_URI, mongo_db=config.MONGO_DB, mongo_collection=config.MONGO_COLLECTION, sleep_rate=rate, num_user=num_user, num_app=num_app)
    
    try:
        await producer.initialize()
        await producer.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, stopping producer...")
    except Exception as e:
        logger.error("Producer failed", error=str(e))
        raise
    finally:
        producer.stop()


def _serialize_doc_for_json(doc: dict) -> dict:
    serializable = {}
    for k, v in doc.items():
        if isinstance(v, datetime):
            serializable[k] = v.astimezone(timezone.utc).isoformat()
        else:
            serializable[k] = v
    return serializable


def _parse_doc_from_json(doc: dict) -> dict:
    # Convert ISO datetime strings back to datetime for known fields
    def _maybe_parse(value):
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except Exception:
                return value
        return value

    for key in ("created_at", "updated_at", "updatedAt", "lastLoginAt"):
        if key in doc:
            doc[key] = _maybe_parse(doc[key])
    return doc


async def generate_json_file(count: int, output_path: str, num_user: int, num_app: int):
    logger = structlog.get_logger()
    logger.info("Generating fixed dataset JSONL", count=count, output=output_path)
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    gen = RealtimeDataProducer(
        mongo_uri=config.MONGO_URI,
        mongo_db=config.MONGO_DB,
        mongo_collection=config.MONGO_COLLECTION,
        sleep_rate=config.SLEEP_RATE,
        num_user=num_user,
        num_app=num_app,
    ).data_generator
    # Write JSON Lines for easy replay
    with open(output_path, "w", encoding="utf-8") as f:
        for _ in range(max(0, int(count))):
            doc = gen.generate_user_submission()
            f.write(json.dumps(_serialize_doc_for_json(doc), ensure_ascii=False))
            f.write("\n")
    logger.info("Dataset written", path=output_path)


async def replay_json_file(input_path: str, rate: int):
    logger = structlog.get_logger()
    if not os.path.isfile(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    logger.info("Replaying JSONL into MongoDB", path=input_path, rate=rate)
    producer = RealtimeDataProducer(
        mongo_uri=config.MONGO_URI,
        mongo_db=config.MONGO_DB,
        mongo_collection=config.MONGO_COLLECTION,
        sleep_rate=rate,
        num_user=config.NUM_USER,
        num_app=config.NUM_APP,
    )
    await producer.initialize()
    try:
        interval = 1.0 / rate if rate > 0 else 1.0
        sent = 0
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                doc = json.loads(line)
                doc = _parse_doc_from_json(doc)
                if producer.collection is None:
                    raise RuntimeError("MongoDB collection is not initialized")
                uid = doc.get("uid")
                producer.collection.update_one({"uid": uid}, {"$set": doc}, upsert=True)
                sent += 1
                if sent % 20 == 0:
                    logger.info("Replayed records", count=sent)
                await asyncio.sleep(interval)
        logger.info("Replay finished", total_records=sent)
    finally:
        producer.stop()

def create_parser():
    """Create command line argument parser"""
    parser = argparse.ArgumentParser(description='Realtime User Producer (MongoDB -> Debezium)')
    
    # Global options
    parser.add_argument('--log-level', 
                       default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Start producer command
    producer_parser = subparsers.add_parser('start-producer', help='Start realtime user producer')
    producer_parser.add_argument('--rate', type=int, help='Events per second')
    producer_parser.add_argument('--num-user', type=int, help='Number of unique users to pre-initialize')
    producer_parser.add_argument('--num-app', type=int, help='Number of apps to randomly assign per user')
    # Mongo connection overrides
    producer_parser.add_argument('--mongo-uri', type=str, help='MongoDB connection string')
    producer_parser.add_argument('--mongo-db', type=str, help='MongoDB database name')
    producer_parser.add_argument('--mongo-collection', type=str, help='MongoDB collection name')
    
    # Generate fixed dataset to JSONL
    gen_parser = subparsers.add_parser('generate-json', help='Generate N records to a JSONL file')
    gen_parser.add_argument('--count', type=int, default=100, help='Number of records to generate')
    gen_parser.add_argument('--output', type=str, default='fixed-dataset.jsonl', help='Output JSONL path')
    gen_parser.add_argument('--num-user', type=int, help='Number of unique users to pre-initialize')
    gen_parser.add_argument('--num-app', type=int, help='Number of apps to randomly assign per user')

    # Replay JSONL into MongoDB at a rate
    replay_parser = subparsers.add_parser('replay-json', help='Replay records from a JSONL file into MongoDB at given rate')
    replay_parser.add_argument('--input', type=str, default='fixed-dataset.jsonl', help='Input JSONL path')
    replay_parser.add_argument('--rate', type=int, help='Events per second')
    replay_parser.add_argument('--mongo-uri', type=str, help='MongoDB connection string')
    replay_parser.add_argument('--mongo-db', type=str, help='MongoDB database name')
    replay_parser.add_argument('--mongo-collection', type=str, help='MongoDB collection name')

    return parser


async def main():
    """Main entry point"""
    parser = create_parser()
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = structlog.get_logger()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == 'start-producer':
            # Override config with CLI if provided
            if args.mongo_uri:
                config.MONGO_URI = args.mongo_uri
            if args.mongo_db:
                config.MONGO_DB = args.mongo_db
            if args.mongo_collection:
                config.MONGO_COLLECTION = args.mongo_collection
            await start_producer(
                rate=(args.rate or config.SLEEP_RATE),
                num_user=args.num_user or config.NUM_USER,
                num_app=args.num_app or config.NUM_APP,
            )
        elif args.command == 'generate-json':
            await generate_json_file(
                count=args.count,
                output_path=args.output,
                num_user=args.num_user or config.NUM_USER,
                num_app=args.num_app or config.NUM_APP,
            )
        elif args.command == 'replay-json':
            # Override config with CLI if provided
            if args.mongo_uri:
                config.MONGO_URI = args.mongo_uri
            if args.mongo_db:
                config.MONGO_DB = args.mongo_db
            if args.mongo_collection:
                config.MONGO_COLLECTION = args.mongo_collection
            await replay_json_file(
                input_path=args.input,
                rate=(args.rate or config.SLEEP_RATE),
            )
        else:
            parser.print_help()
    
    except Exception as e:
        logger.error("Command failed", command=args.command, error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
