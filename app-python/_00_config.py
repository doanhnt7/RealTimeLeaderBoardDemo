"""
Configuration settings for the realtime user producer (MongoDB -> Debezium CDC)
"""

import os


class Config:
    """Configuration class with environment variable support"""
    
    # Producer settings
    SLEEP_RATE = int(os.getenv('SLEEP_RATE', '10'))  # events per second
    
    # MongoDB settings (Debezium watches this collection)
    MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongo:27017')
    MONGO_DB = os.getenv('MONGO_DB', 'leaderboard')
    MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'user_submissions')
    NUM_USER = 100
    NUM_APP = 1
    
    # Redis cache settings (for user profile cache)
    REDIS_HOST = 'redis'
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD = ''
    REDIS_USER_PROFILE_PREFIX = 'user:profile:'
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')


# Global config instance
config = Config()
