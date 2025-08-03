#Generates the dummy data

import json
import redis
import time
import random
import argparse
import logging
from datetime import datetime
import sys
from threading import Thread

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TickDataGenerator:
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0):
        try:
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                decode_responses=True
            )
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {redis_host}:{redis_port}")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
            
        self.running = True
        
    def generate_realistic_price(self, base_price: float, volatility: float = 0.001) -> float:
        change = random.gauss(0, volatility) * base_price
        return max(base_price + change, 0.01)  
        
    def generate_tick(self, symbol: str, base_price: float) -> dict:
        price = self.generate_realistic_price(base_price)
        volume = random.lognormvariate(0, 1) * 10 
        
        tick = {
            'symbol': symbol,
            'price': round(price, 2),
            'volume': round(volume, 4),
            'timestamp': datetime.now().isoformat(),
            'trade_id': random.randint(100000, 999999)
        }
        
        return tick
        
    def generate_batch_ticks(self, symbol: str, base_price: float, count: int) -> list:
        ticks = []
        current_price = base_price
        
        for _ in range(count):
            current_price = self.generate_realistic_price(current_price, 0.0005)
            tick = {
                'symbol': symbol,
                'price': round(current_price, 2),
                'volume': random.lognormvariate(0, 0.5) * 5,
                'timestamp': datetime.now().isoformat(),
                'trade_id': random.randint(100000, 999999)
            }
            ticks.append(tick)
            time.sleep(0.01)  
            
        return ticks, current_price
        
    def publish_tick(self, channel: str, tick: dict):
        try:
            tick_json = json.dumps(tick)
            self.redis_client.publish(channel, tick_json)
            logger.debug(f"Published tick for {tick['symbol']}: ${tick['price']} vol:{tick['volume']}")
        except Exception as e:
            logger.error(f"Failed to publish tick: {e}")
            
    def simulate_trading_day(self, symbol: str, base_price: float, duration_minutes: int = 60):
        logger.info(f"Starting {duration_minutes}-minute simulation for {symbol} at base price ${base_price}")
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        current_price = base_price
        
        while self.running and time.time() < end_time:
            current_minute = int((time.time() - start_time) / 60)
            if current_minute % 15 < 5:
                tick_frequency = random.uniform(0.1, 0.5)  
                batch_probability = 0.3 
            else:  
                tick_frequency = random.uniform(0.5, 2.0) 
                batch_probability = 0.1  
            if random.random() < batch_probability:
                batch_size = random.randint(2, 8)
                ticks, current_price = self.generate_batch_ticks(symbol, current_price, batch_size)
                
                for tick in ticks:
                    self.publish_tick(symbol, tick)
                    
                logger.info(f"Published batch of {batch_size} ticks for {symbol}, "
                           f"price now: ${current_price:.2f}")
            else:
                tick = self.generate_tick(symbol, current_price)
                current_price = tick['price']
                self.publish_tick(symbol, tick)
            time.sleep(tick_frequency)
            
        logger.info(f"Simulation completed for {symbol}")
        
    def simulate_multiple_symbols(self, symbols_prices: dict, duration_minutes: int = 60):
        threads = []
        
        for symbol, base_price in symbols_prices.items():
            thread = Thread(
                target=self.simulate_trading_day,
                args=(symbol, base_price, duration_minutes)
            )
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
            
    def stop(self):
        self.running = False
        logger.info("Stopping data generation...")


def main():
    parser = argparse.ArgumentParser(description='Generate dummy tick data for testing')
    parser.add_argument('--symbol', default='BTCUSDT', help='Trading symbol')
    parser.add_argument('--base-price', type=float, default=45000.0, help='Base price for the symbol')
    parser.add_argument('--duration', type=int, default=10, help='Duration in minutes')
    parser.add_argument('--redis-host', default='localhost', help='Redis host')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis port')
    parser.add_argument('--redis-db', type=int, default=0, help='Redis database')
    parser.add_argument('--multi-symbol', action='store_true', help='Generate data for multiple symbols')
    
    args = parser.parse_args()
    
    try:
        generator = TickDataGenerator(
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            redis_db=args.redis_db
        )
        
        if args.multi_symbol:
            symbols_prices = {
                'BTCUSDT': 45000.0,
                'ETHUSDT': 2800.0,
                'ADAUSDT': 0.45,
                'DOTUSDT': 6.50
            }
            logger.info(f"Starting multi-symbol simulation for {args.duration} minutes")
            generator.simulate_multiple_symbols(symbols_prices, args.duration)
        else:
            logger.info(f"Starting simulation for {args.symbol} at ${args.base_price} for {args.duration} minutes")
            generator.simulate_trading_day(args.symbol, args.base_price, args.duration)
            
    except KeyboardInterrupt:
        logger.info("Received interrupt, stopping...")
        generator.stop()
    except Exception as e:
        logger.error(f"Error in data generation: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()