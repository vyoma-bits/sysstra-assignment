
import argparse
import signal
import sys
import json
import time
import redis
import logging
from datetime import datetime
from collections import defaultdict


from config import *
from db_utils import *


running = True
current_minute_data = defaultdict(lambda: {'ticks': [], 'minute_start': None})


def handle_shutdown(signum, frame):
    global running
    logging.info("Stopping the script")
    running = False


def parse_tick_message(raw_message):
    try:
        tick = json.loads(raw_message)
        if not all(field in tick for field in REQUIRED_FIELDS):
            logging.warning("Missing required fields in tick")
            return None
        if isinstance(tick['timestamp'], str):
            tick['timestamp'] = datetime.fromisoformat(tick['timestamp'].replace('Z', '+00:00'))
        elif isinstance(tick['timestamp'], (int, float)):
            tick['timestamp'] = datetime.fromtimestamp(tick['timestamp'])
        
        return tick
    except Exception as e:
        logging.error(f"Failed to parse tick: {e}")
        return None


def get_minute_start(timestamp):
    return timestamp.replace(second=0, microsecond=0)


def calculate_ohlcv_data(ticks):
    if not ticks:
        return None
    ticks.sort(key=lambda x: x['timestamp'])
    prices = [float(tick['price']) for tick in ticks]
    volumes = [float(tick['volume']) for tick in ticks]
    
    return {
        'open': prices[0],
        'high': max(prices),
        'low': min(prices),
        'close': prices[-1],
        'volume': sum(volumes),
        'tick_count': len(ticks)
    }


def process_completed_minute(symbol, ticks):
    if not ticks:
        return
    minute_start = get_minute_start(ticks[0]['timestamp'])
    ohlcv_data = calculate_ohlcv_data(ticks)
    if ohlcv_data:
        logging.info(f"Minute complete for {symbol}: "
              f"O={ohlcv_data['open']:.4f} H={ohlcv_data['high']:.4f} "
              f"L={ohlcv_data['low']:.4f} C={ohlcv_data['close']:.4f} "
              f"V={ohlcv_data['volume']:.2f} ({ohlcv_data['tick_count']} ticks)")
        save_to_mongodb(symbol, minute_start, ohlcv_data)
        redisPublishMessage(symbol, minute_start, ohlcv_data)
        fileSave(symbol, minute_start, ohlcv_data)


def handle_tick(symbol, tick):
    current_minute = get_minute_start(tick['timestamp'])
    if symbol not in current_minute_data:
        current_minute_data[symbol]['minute_start'] = current_minute
    stored_minute = current_minute_data[symbol]['minute_start']
    if stored_minute and current_minute > stored_minute:
        process_completed_minute(symbol, current_minute_data[symbol]['ticks'])


        current_minute_data[symbol] = {
            'ticks': [],
            'minute_start': current_minute
        }
    
    current_minute_data[symbol]['ticks'].append(tick)
    current_minute_data[symbol]['minute_start'] = current_minute


def listen_for_ticks(symbols):
    redis_client = get_redis_client()
    if redis_client is None:
        logging.error("Redis client not connected")
        return
    pubsub = redis_client.pubsub()
    for symbol in symbols:
        pubsub.subscribe(symbol)
        logging.info(f"Subscribed to {symbol}")
    logging.info("Listening for Data")
    try:
        while running:
            try:
                message = pubsub.get_message(timeout=1)
                if message is None:
                    continue
                if message['type'] == 'message':
                    channel = message['channel']
                    raw_data = message['data']
                    tick = parse_tick_message(raw_data)
                    if tick:
                        handle_tick(channel, tick)
                
            except redis.ConnectionError:
                logging.error("Redis connection lost, Trying to reconnect")
                time.sleep(5)
                continue
            except Exception as e:
                logging.error(f"Error processing message: {e}")
                continue
    finally:
        logging.info("Processing remaining ticks after User interrupted")
        for symbol in current_minute_data:
            if current_minute_data[symbol]['ticks']:
                process_completed_minute(symbol, current_minute_data[symbol]['ticks'])
        pubsub.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbols', nargs='+', default=['BTCUSDT'])
    parser.add_argument('--redis-host', default=REDIS_HOST)
    parser.add_argument('--redis-port', type=int, default=REDIS_PORT)
    parser.add_argument('--mongo-uri', default=MONGO_URI)
    parser.add_argument('--mongo-db', default=MONGO_DB)
    
    args = parser.parse_args()
    setup_logging()
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    logging.info("Starting Listening")
    logging.info(f"Symbols: {args.symbols}")
    logging.info(f"Redis: {args.redis_host}:{args.redis_port}")
    logging.info(f"MongoDB: {args.mongo_uri}")
    if not initaiteRedisConnection(args.redis_host, args.redis_port):
        logging.error("Cannot connect to Redis. Exiting.")
        return
    if not initiateMongoDbConnection(args.mongo_uri, args.mongo_db):
        logging.error("Cannot connect to MongoDB. Exiting.")
        return
    try:
        listen_for_ticks(args.symbols)
    except Exception as e:
        logging.error(f"error: {e}")
    finally:
        close_connections()
        logging.info("Shutdown complete")
    
if __name__ == "__main__":
    main()
