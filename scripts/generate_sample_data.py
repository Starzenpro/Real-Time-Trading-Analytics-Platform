#!/usr/bin/env python3
"""
Generate sample trading data for testing
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json
import os
from faker import Faker

fake = Faker()

def generate_market_data(days=365, symbols=None):
    """Generate sample market data"""
    if symbols is None:
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'V', 'WMT']
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    all_data = []
    
    for symbol in symbols:
        # Base price
        base_price = random.uniform(50, 500)
        volatility = random.uniform(0.01, 0.03)
        
        dates = pd.date_range(start_date, end_date, freq='1min')
        
        for date in dates:
            # Random walk price
            change = np.random.normal(0, volatility)
            price = base_price * (1 + change)
            base_price = price
            
            data = {
                'symbol': symbol,
                'timestamp': date.isoformat(),
                'open': round(price * (1 + np.random.normal(0, 0.001)), 2),
                'high': round(price * (1 + abs(np.random.normal(0, 0.002))), 2),
                'low': round(price * (1 - abs(np.random.normal(0, 0.002))), 2),
                'close': round(price, 2),
                'volume': int(np.random.exponential(1000000)),
                'source': 'sample_generator'
            }
            all_data.append(data)
    
    return pd.DataFrame(all_data)

def generate_trades(n=10000):
    """Generate sample trade data"""
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    sides = ['BUY', 'SELL']
    
    trades = []
    
    for i in range(n):
        symbol = random.choice(symbols)
        side = random.choice(sides)
        quantity = random.randint(1, 1000)
        price = random.uniform(50, 500)
        
        trade = {
            'trade_id': f"TRADE_{i:06d}",
            'symbol': symbol,
            'side': side,
            'quantity': quantity,
            'price': round(price, 2),
            'value': round(price * quantity, 2),
            'timestamp': (datetime.now() - timedelta(minutes=random.randint(0, 10000))).isoformat(),
            'trader': fake.name(),
            'strategy': random.choice(['Momentum', 'MeanReversion', 'Arbitrage', 'MarketMaking']),
            'broker': random.choice(['IB', 'GS', 'MS', 'JPM', 'CS'])
        }
        trades.append(trade)
    
    return pd.DataFrame(trades)

def generate_positions():
    """Generate sample positions"""
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'V', 'WMT']
    
    positions = []
    for symbol in symbols:
        position = {
            'symbol': symbol,
            'quantity': random.randint(-10000, 10000),
            'avg_price': random.uniform(50, 500),
            'current_price': random.uniform(50, 500),
            'pnl': random.uniform(-100000, 100000),
            'beta': random.uniform(0.5, 1.5),
            'sector': random.choice(['Tech', 'Finance', 'Healthcare', 'Consumer'])
        }
        positions.append(position)
    
    return pd.DataFrame(positions)

def main():
    """Generate all sample data"""
    print("📊 Generating sample trading data...")
    
    # Create data directory
    os.makedirs('data/sample', exist_ok=True)
    
    # Generate market data
    print("   Generating market data...")
    market_df = generate_market_data(days=30)
    market_df.to_parquet('data/sample/market_data.parquet')
    print(f"   ✅ Generated {len(market_df):,} market data points")
    
    # Generate trades
    print("   Generating trade data...")
    trades_df = generate_trades(50000)
    trades_df.to_parquet('data/sample/trades.parquet')
    print(f"   ✅ Generated {len(trades_df):,} trades")
    
    # Generate positions
    print("   Generating positions...")
    positions_df = generate_positions()
    positions_df.to_csv('data/sample/positions.csv', index=False)
    print(f"   ✅ Generated {len(positions_df)} positions")
    
    print("\n✅ Sample data generation complete!")
    print(f"📁 Data saved to: data/sample/")

if __name__ == "__main__":
    main()
