"""
Configuration settings for the realtime user producer (Kafka only)
"""

import os


class Config:
    """Configuration class with environment variable support"""
    
    # Kafka settings for direct streaming
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    
    # Producer settings
    SLEEP_RATE = int(os.getenv('SLEEP_RATE', '10'))  # events per second
    USERS_TOPIC = os.getenv('USERS_TOPIC', 'users')
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')


# Global config instance
config = Config()
