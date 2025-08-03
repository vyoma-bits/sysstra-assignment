#In this file all the utils function of db and reddis related are written

import redis
import json
import logging
from datetime import datetime
from pymongo import MongoClient
from config import *

redis_client = None
mongo_client = None
aggregated_collection = None

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )

def get_redis_client():
    return redis_client

def initaiteRedisConnection(host=REDIS_HOST, port=REDIS_PORT):
    global redis_client
    try:
        redis_client = redis.Redis(host=host, port=port, decode_responses=True)
        redis_client.ping()
        logging.info(f"Connected to Redis at {host}:{port}")
        return True
    except Exception as e:
        logging.error(f"Failed to connect to Redis: {e}")
        return False

def initiateMongoDbConnection(mongo_uri=MONGO_URI, db_name=MONGO_DB):
    global mongo_client, aggregated_collection
    try:
        mongo_client = MongoClient(mongo_uri)
        mongo_client.admin.command('ping')
        logging.info(f"Connected to MongoDB")
        db = mongo_client[db_name]
        aggregated_collection = db['aggregated_data']
        aggregated_collection.create_index([("symbol", 1), ("minute_start", 1)])
        aggregated_collection.create_index([("created_at", -1)])
        logging.info(f"Database '{db_name}' ready")
        return True
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        return False

def save_to_mongodb(symbol, minute_start, ohlcv_data):
    try:
        document = {
            'symbol': symbol,
            'minute_start': minute_start,
            'timestamp': minute_start.isoformat(),
            'open': ohlcv_data['open'],
            'high': ohlcv_data['high'],
            'low': ohlcv_data['low'],
            'close': ohlcv_data['close'],
            'volume': ohlcv_data['volume'],
            'tick_count': ohlcv_data['tick_count'],
            'created_at': datetime.utcnow()
        }
        
        result = aggregated_collection.insert_one(document)
        logging.info(f"Saved {symbol} to MongoDB: {result.inserted_id}")
    except Exception as e:
        logging.error(f"Failed to save to MongoDB: {e}")

def redisPublishMessage(symbol, minute_start, ohlcv_data):
    try:
        redis_key = f"{symbol}:{minute_start.strftime('%Y%m%d_%H%M')}"
        data = {
            'symbol': symbol,
            'timestamp': minute_start.isoformat(),
            'open': ohlcv_data['open'],
            'high': ohlcv_data['high'],
            'low': ohlcv_data['low'],
            'close': ohlcv_data['close'],
            'volume': ohlcv_data['volume'],
            'tick_count': ohlcv_data['tick_count']
        }
        redis_client.set(redis_key, json.dumps(data), ex=86400)
        redis_client.publish(f"{symbol}:aggregated", json.dumps(data))
        logging.info(f"Published {symbol} to Redis: {redis_key}")
    except Exception as e:
        logging.error(f"Failed to publish to Redis: {e}")

def fileSave(symbol, minute_start, ohlcv_data):
    try:
        filename = f"aggregated_data_{symbol}.jsonl"
        data = {
            'symbol': symbol,
            'timestamp': minute_start.isoformat(),
            'open': ohlcv_data['open'],
            'high': ohlcv_data['high'],
            'low': ohlcv_data['low'],
            'close': ohlcv_data['close'],
            'volume': ohlcv_data['volume'],
            'tick_count': ohlcv_data['tick_count']
        }
        
        with open(filename, 'a') as f:
            f.write(json.dumps(data) + '\n')
        
        logging.info(f"Saved {symbol} to file: {filename}")
    except Exception as e:
        logging.error(f"Failed to save to file: {e}")

def close_connections():
    if mongo_client:
        mongo_client.close()
        logging.info("MongoDB connection closed")
