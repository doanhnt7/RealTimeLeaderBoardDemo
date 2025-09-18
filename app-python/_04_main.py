"""
Main entry point for the realtime user producer (MongoDB -> Debezium CDC)
"""

import asyncio
import argparse
import logging
import sys
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
        else:
            parser.print_help()
    
    except Exception as e:
        logger.error("Command failed", command=args.command, error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
