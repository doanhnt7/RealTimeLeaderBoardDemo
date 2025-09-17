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
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')


# Global config instance
config = Config()
