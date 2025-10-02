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
import pandas as pd
import time
import orjson
import pyarrow.parquet as pq

from _03_realtime_producer import RealtimeDataProducer
from _02_kafka_producer import KafkaManager
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


async def generate_parquet_file(count: int, output_path: str, num_user: int, num_app: int):
    """Generate Parquet file for better performance with large datasets"""
    logger = structlog.get_logger()
    logger.info("Generating fixed dataset Parquet", count=count, output=output_path)
    
    try:
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        logger.error("Required packages not available. Please install: pip install pandas pyarrow")
        return
    
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    gen = RealtimeDataProducer(
        mongo_uri=config.MONGO_URI,
        mongo_db=config.MONGO_DB,
        mongo_collection=config.MONGO_COLLECTION,
        sleep_rate=config.SLEEP_RATE,
        num_user=num_user,
        num_app=num_app,
    ).data_generator
    
    # Define explicit schema for consistent data types
    schema = pa.schema([
        ('uid', pa.string()),
        ('email', pa.string()),
        ('authProvider', pa.string()),
        ('appId', pa.string()),
        ('avatar', pa.string()),
        ('geo', pa.string()),
        ('role', pa.string()),
        ('lastLoginAt', pa.string()),
        ('name', pa.string()),
        ('devices', pa.list_(pa.struct([
            ('fb_analytics_instance_id', pa.string()),
            ('fb_instance_id', pa.string()),
            ('fcmToken', pa.string())
        ]))),
        ('resources', pa.list_(pa.string())),
        ('created_at', pa.string()),
        ('updated_at', pa.string()),
        ('level', pa.int64()),
        ('previousLevel', pa.int64()),
        ('updatedAt', pa.string()),
        ('team', pa.int64())
    ])
    
    def normalize_for_parquet(doc):
        """Normalize data structure for consistent parquet storage"""
        normalized = {}
        for key, value in doc.items():
            if key in ['devices', 'resources']:
                # Ensure these are always lists
                normalized[key] = list(value) if value else []
            elif isinstance(value, datetime):
                normalized[key] = value.astimezone(timezone.utc).isoformat()
            else:
                normalized[key] = value
        return normalized
    
    # Generate data in batches for memory efficiency
    batch_size = 10000
    total_batches = (count + batch_size - 1) // batch_size
    
    logger.info(f"Generating {count} records in {total_batches} batches of {batch_size}")
    
    # Collect all data first
    all_data = []
    for batch_num in range(total_batches):
        batch_count = min(batch_size, count - batch_num * batch_size)
        batch_data = []
        
        for _ in range(batch_count):
            doc = gen.generate_user_submission()
            # Normalize data structure for consistent parquet storage
            normalized_doc = normalize_for_parquet(doc)
            batch_data.append(normalized_doc)
        
        all_data.extend(batch_data)
        
        if (batch_num + 1) % 10 == 0:  # Log every 10 batches
            logger.info(f"Generated batch {batch_num + 1}/{total_batches}")
    
    # Create DataFrame and write as Parquet with explicit schema
    logger.info("Creating DataFrame and writing Parquet with explicit schema...")
    df = pd.DataFrame(all_data)
    
    # Convert to PyArrow table with explicit schema
    table = pa.Table.from_pandas(df, schema=schema)
    
    # Write with compression for better performance
    pq.write_table(
        table,
        output_path,
        compression='snappy',  # Fast compression
        use_deprecated_int96_timestamps=False
    )
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    
    logger.info("Parquet dataset written with explicit schema", 
                path=output_path, 
                records=len(all_data),
                file_size_mb=f"{file_size_mb:.2f}MB")


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


async def replay_parquet_to_kafka(input_path: str, rate: int, kafka_servers: str, kafka_topic: str):
    """Replay records from a Parquet file into Kafka at given rate"""
    logger = structlog.get_logger()
    
    if not os.path.isfile(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    logger.info("Replaying Parquet into Kafka", 
                path=input_path, 
                rate=rate, 
                kafka_servers=kafka_servers, 
                kafka_topic=kafka_topic)
    
    # Initialize Kafka manager
    kafka_manager = KafkaManager(bootstrap_servers=kafka_servers)

    
    try:
        # Ensure topic exists
        kafka_manager.ensure_topic(kafka_topic, num_partitions=1, replication_factor=1)
        
        # Read parquet file via PyArrow for faster iteration
        logger.info("Reading Parquet file...")
        table = pq.read_table(input_path)
        total_records = table.num_rows
        logger.info("Parquet file loaded", records=total_records)
        
        # Calculate interval between messages
        interval = 1.0 / rate if rate > 0 else 1.0
        
        sent_count = 0
        start_perf = time.perf_counter()
        start_time_iso = datetime.now(timezone.utc).isoformat()
        
        # Process each row in batches while preserving single-thread order
        batch_size = 10000
        batch_index = 0
        for batch in table.to_batches(max_chunksize=batch_size):
            rows = batch.to_pylist()  # list[dict] with native Python types
            for doc in rows:
                try:
                    key_bytes = (doc.get('uid') or '').encode('utf-8')
                    value_bytes = orjson.dumps(doc)
                    kafka_manager.send_message_bytes(topic=kafka_topic, key=key_bytes, value=value_bytes)
                    sent_count += 1
                    if rate > 0:
                        await asyncio.sleep(interval)
                except Exception as e:
                    logger.error("Failed to process record", record_index=sent_count, error=str(e))
                    continue
            batch_index += 1
        
        # Flush remaining messages
        kafka_manager.flush()

        end_perf = time.perf_counter()
        end_time_iso = datetime.now(timezone.utc).isoformat()
        duration_seconds = max(end_perf - start_perf, 1e-9)
        avg_msgs_per_sec = sent_count / duration_seconds

        logger.info(
            "Replay to Kafka completed",
            total_records=total_records,
            sent_records=sent_count,
            start_time=start_time_iso,
            end_time=end_time_iso,
            duration_seconds=f"{duration_seconds:.3f}",
            average_msgs_per_sec=f"{avg_msgs_per_sec:.2f}"
        )
        
    
                   
    except Exception as e:
        logger.error("Failed to replay parquet to Kafka", error=str(e))
        raise
    finally:
        kafka_manager.close()


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

    # Generate fixed dataset to Parquet
    parquet_parser = subparsers.add_parser('generate-parquet', help='Generate N records to a Parquet file')
    parquet_parser.add_argument('--count', type=int, default=100, help='Number of records to generate')
    parquet_parser.add_argument('--output', type=str, default='fixed-dataset.parquet', help='Output Parquet path')
    parquet_parser.add_argument('--num-user', type=int, help='Number of unique users to pre-initialize')
    parquet_parser.add_argument('--num-app', type=int, help='Number of apps to randomly assign per user')

    # Replay JSONL into MongoDB at a rate
    replay_parser = subparsers.add_parser('replay-json', help='Replay records from a JSONL file into MongoDB at given rate')
    replay_parser.add_argument('--input', type=str, default='fixed-dataset.jsonl', help='Input JSONL path')
    replay_parser.add_argument('--rate', type=int, help='Events per second')
    replay_parser.add_argument('--mongo-uri', type=str, help='MongoDB connection string')
    replay_parser.add_argument('--mongo-db', type=str, help='MongoDB database name')
    replay_parser.add_argument('--mongo-collection', type=str, help='MongoDB collection name')

    # Replay Parquet into Kafka at a rate
    replay_kafka_parser = subparsers.add_parser('replay-parquet-kafka', help='Replay records from a Parquet file into Kafka at given rate')
    replay_kafka_parser.add_argument('--input', type=str, default='fixed-dataset.parquet', help='Input Parquet path')
    replay_kafka_parser.add_argument('--rate', type=int, help='Events per second', default=0)
    replay_kafka_parser.add_argument('--kafka-servers', type=str, help='Kafka bootstrap servers')
    replay_kafka_parser.add_argument('--kafka-topic', type=str, help='Kafka topic name')

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
                rate = args.rate if args.rate is not None else config.SLEEP_RATE,
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
        elif args.command == 'generate-parquet':
            await generate_parquet_file(
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
                rate = args.rate if args.rate is not None else config.SLEEP_RATE,
            )
        elif args.command == 'replay-parquet-kafka':
            # Override config with CLI if provided
            kafka_servers = args.kafka_servers or config.KAFKA_BOOTSTRAP_SERVERS
            kafka_topic = args.kafka_topic or config.KAFKA_TOPIC
            await replay_parquet_to_kafka(
                input_path=args.input,
                rate = args.rate if args.rate is not None else config.SLEEP_RATE,
                kafka_servers=kafka_servers,
                kafka_topic=kafka_topic,
            )
        else:
            parser.print_help()
    
    except Exception as e:
        logger.error("Command failed", command=args.command, error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
