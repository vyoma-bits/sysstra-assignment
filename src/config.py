#Config file for default values

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
MONGO_URI = 'mongodb://localhost:27017/'
MONGO_DB = 'tickdata'

REQUIRED_FIELDS = ['symbol', 'price', 'volume', 'timestamp']

LOG_FILE = 'tick_aggregator.log'
