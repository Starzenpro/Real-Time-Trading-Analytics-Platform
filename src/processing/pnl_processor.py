"""
Real-time P&L calculations for trading positions
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class PnLProcessor:
    """
    Calculate real-time Profit & Loss for trading positions
    """
    
    def __init__(self):
        self.positions = {}
        self.trades = []
        
    def calculate_trade_pnl(self, trade: Dict) -> Dict:
        """
        Calculate P&L for a single trade
        """
        # Buy or Sell
        if trade['side'] == 'BUY':
            # For buy trades, P&L is realized when sold
            trade['realized_pnl'] = 0
            trade['unrealized_pnl'] = 0
        else:
            # For sell trades, calculate realized P&L
            buy_price = self.positions.get(trade['symbol'], {}).get('avg_buy_price', trade['price'])
            trade['realized_pnl'] = (trade['price'] - buy_price) * trade['quantity']
            trade['unrealized_pnl'] = 0
        
        # Calculate fees
        trade['commission'] = trade['price'] * trade['quantity'] * 0.001
        trade['net_pnl'] = trade.get('realized_pnl', 0) - trade['commission']
        
        return trade
    
    def update_position(self, trade: Dict):
        """
        Update position after a trade
        """
        symbol = trade['symbol']
        
        if symbol not in self.positions:
            self.positions[symbol] = {
                'quantity': 0,
                'avg_price': 0,
                'realized_pnl': 0
            }
        
        position = self.positions[symbol]
        
        if trade['side'] == 'BUY':
            # Update average buy price
            total_value = position['quantity'] * position['avg_price'] + trade['quantity'] * trade['price']
            position['quantity'] += trade['quantity']
            position['avg_price'] = total_value / position['quantity'] if position['quantity'] > 0 else 0
        else:  # SELL
            # Calculate realized P&L
            realized = (trade['price'] - position['avg_price']) * trade['quantity']
            position['realized_pnl'] += realized
            position['quantity'] -= trade['quantity']
    
    def calculate_unrealized_pnl(self, current_prices: Dict) -> float:
        """
        Calculate unrealized P&L for all open positions
        """
        unrealized = 0
        for symbol, position in self.positions.items():
            if position['quantity'] > 0 and symbol in current_prices:
                unrealized += (current_prices[symbol] - position['avg_price']) * position['quantity']
        
        return unrealized
    
    def calculate_daily_pnl(self, date: datetime = None) -> Dict:
        """
        Calculate aggregated P&L for a specific day
        """
        if date is None:
            date = datetime.now().date()
        
        day_trades = [t for t in self.trades 
                     if datetime.fromisoformat(t['timestamp']).date() == date]
        
        total_realized = sum(t.get('realized_pnl', 0) for t in day_trades)
        total_commission = sum(t.get('commission', 0) for t in day_trades)
        net_pnl = total_realized - total_commission
        
        return {
            'date': date.isoformat(),
            'total_trades': len(day_trades),
            'total_volume': sum(t['price'] * t['quantity'] for t in day_trades),
            'realized_pnl': total_realized,
            'total_commission': total_commission,
            'net_pnl': net_pnl
        }
    
    def calculate_mtd_pnl(self) -> Dict:
        """
        Calculate month-to-date P&L
        """
        current_month = datetime.now().month
        current_year = datetime.now().year
        
        mtd_trades = [t for t in self.trades 
                     if datetime.fromisoformat(t['timestamp']).month == current_month
                     and datetime.fromisoformat(t['timestamp']).year == current_year]
        
        total_realized = sum(t.get('realized_pnl', 0) for t in mtd_trades)
        total_commission = sum(t.get('commission', 0) for t in mtd_trades)
        
        return {
            'month': current_month,
            'year': current_year,
            'total_trades': len(mtd_trades),
            'realized_pnl': total_realized,
            'total_commission': total_commission,
            'net_pnl': total_realized - total_commission
        }
    
    def calculate_ytd_pnl(self) -> Dict:
        """
        Calculate year-to-date P&L
        """
        current_year = datetime.now().year
        
        ytd_trades = [t for t in self.trades 
                     if datetime.fromisoformat(t['timestamp']).year == current_year]
        
        total_realized = sum(t.get('realized_pnl', 0) for t in ytd_trades)
        total_commission = sum(t.get('commission', 0) for t in ytd_trades)
        
        return {
            'year': current_year,
            'total_trades': len(ytd_trades),
            'realized_pnl': total_realized,
            'total_commission': total_commission,
            'net_pnl': total_realized - total_commission
        }
