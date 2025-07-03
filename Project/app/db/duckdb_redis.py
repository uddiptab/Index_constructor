import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))



import duckdb
import redis
import os
from app.core.base import SingletonMeta, logger

class DBManager(metaclass=SingletonMeta):
    def __init__(self):
        self.conn = duckdb.connect("stock_data.duckdb")
        self.redis = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=0
        )
        logger.info("Connected to DuckDB and Redis")
