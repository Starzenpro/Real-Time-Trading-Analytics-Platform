"""
Real-time market data ingestion from multiple sources
"""

import asyncio
import aiohttp
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
import logging
from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.aio import EventHubProducerClient as AsyncProducer
from azure.identity import DefaultAzureCredential
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MarketDataIngestor:
    """
    Ingest real-time market data from various exchanges
    """
    
    def __init__(self):
        self.symbols = os.getenv('SYMBOLS', 'AAPL,MSFT,GOOGL').split(',')
        self.event_hub_conn_str = os.getenv('AZURE_EVENT_HUB_CONNECTION')
        self.event_hub_name = os.getenv('AZURE_EVENT_HUB_NAME', 'market-data')
        
        # Initialize Azure Event Hub producer
        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=self.event_hub_conn_str,
            eventhub_name=self.event_hub_name
        )
        
    async def fetch_polygon_data(self, symbol: str) -> Dict:
        """Fetch real-time data from Polygon.io"""
        api_key = os.getenv('POLYGON_API_KEY')
        url = f"https://api.polygon.io/v2/last/trade/{symbol}?apiKey={api_key}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'symbol': symbol,
                        'price': data['results']['p'],
                        'size': data['results']['s'],
                        'timestamp': data['results']['t'],
                        'exchange': data['results']['x'],
                        'source': 'polygon'
                    }
                else:
                    logger.error(f"Failed to fetch {symbol}: {response.status}")
                    return None
    
    async def fetch_alphavantage_data(self, symbol: str) -> Dict:
        """Fetch data from Alpha Vantage"""
        api_key = os.getenv('ALPHA_VANTAGE_KEY')
        url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={api_key}"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    quote = data.get('Global Quote', {})
                    return {
                        'symbol': symbol,
                        'price': float(quote.get('05. price', 0)),
                        'change': float(quote.get('09. change', 0)),
                        'volume': int(quote.get('06. volume', 0)),
                        'timestamp': datetime.now().isoformat(),
                        'source': 'alphavantage'
                    }
                return None
    
    async def fetch_iex_data(self, symbol: str) -> Dict:
        """Fetch data from IEX Cloud"""
        # IEX implementation
        pass
    
    async def ingest_market_data(self):
        """Main ingestion loop"""
        tasks = []
        for symbol in self.symbols:
            tasks.append(self.fetch_polygon_data(symbol))
            tasks.append(self.fetch_alphavantage_data(symbol))
        
        results = await asyncio.gather(*tasks)
        
        # Filter out None results
        valid_results = [r for r in results if r is not None]
        
        # Send to Azure Event Hub
        await self.send_to_event_hub(valid_results)
        
        return valid_results
    
    async def send_to_event_hub(self, data: List[Dict]):
        """Send data to Azure Event Hub"""
        event_data_batch = await self.producer.create_batch()
        
        for item in data:
            event_data_batch.add(EventData(json.dumps(item)))
        
        await self.producer.send_batch(event_data_batch)
        logger.info(f"Sent {len(data)} events to Event Hub")
    
    async def run_continuously(self, interval_seconds=1):
        """Run ingestion continuously"""
        while True:
            try:
                data = await self.ingest_market_data()
                logger.info(f"Ingested {len(data)} market data points")
                await asyncio.sleep(interval_seconds)
            except Exception as e:
                logger.error(f"Ingestion error: {e}")
                await asyncio.sleep(5)
