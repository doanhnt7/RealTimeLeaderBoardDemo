"""
Main entry point for the realtime user producer (Kafka only)
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


# No database or Debezium setup needed



async def start_producer(kafka_bootstrap_servers: str, rate: int, users_topic: str, num_user: int, num_app: int):
    """Start the realtime user producer publishing directly to Kafka"""
    logger = structlog.get_logger()
    logger.info("Starting realtime user producer...", rate=rate, topic=users_topic)
    producer = RealtimeDataProducer(kafka_bootstrap_servers=kafka_bootstrap_servers, sleep_rate=rate, users_topic=users_topic, num_user=num_user, num_app=num_app)
    
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
    parser = argparse.ArgumentParser(description='Realtime User Producer (Kafka)')
    
    # Global options
    parser.add_argument('--log-level', 
                       default='INFO',
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Start producer command
    producer_parser = subparsers.add_parser('start-producer', help='Start realtime user producer')
    producer_parser.add_argument('--rate', type=int, help='Events per second')
    producer_parser.add_argument('--num-user', type=int, default=100, help='Number of unique users to pre-initialize')
    producer_parser.add_argument('--num-app', type=int, default=1, help='Number of apps to randomly assign per user')
    producer_parser.add_argument('--topic', type=str, default='users', help='Kafka topic name')
    
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
            await start_producer(
                kafka_bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                rate=(args.rate or config.SLEEP_RATE),
                users_topic=(args.topic or config.USERS_TOPIC),
                num_user=args.num_user,
                num_app=args.num_app,
            )
        else:
            parser.print_help()
    
    except Exception as e:
        logger.error("Command failed", command=args.command, error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
