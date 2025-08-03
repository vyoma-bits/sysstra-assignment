# Setup in local

## Prerequisites

1. **Redis**
   ```powershell
   docker run -d -p 6379:6379 --name redis-tick-data redis:7-alpine
   ```

2. **MongoDB**
   - Local MongoDB installation or
   - MongoDB URL

## Setup

1. **Create Virtual Environment**
   ```powershell
   python -m venv env
   .\env\Scripts\activate
   pip install -r requirements.txt
   ```

## Run Application

1. **Generate Demo Data**
   ```powershell
   cd src
   python dummy_data_generator.py --symbol BTCUSDT --duration 5
   ```

2. **Start Main Script**
   ```powershell
   cd src
   python main.py --symbols BTCUSDT --mongo-uri "url"
   ```

## Data Storage

The application uses a dual storage system:
- **Redis**: For real-time data processing and caching
- **MongoDB**: For persistent long-term storage

## Logging

- Application logs are generated in `tick_aggregator.log`
- In furture these Logs can be shifted to AWS CloudWatch for better management

