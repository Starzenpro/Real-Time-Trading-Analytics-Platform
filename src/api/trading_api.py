"""
FastAPI endpoints for real-time trading data
"""

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Optional
import asyncio
import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from src.processing.risk_calculator import RiskCalculator
from src.processing.pnl_processor import PnLProcessor
from src.ingestion.market_data_ingestor import MarketDataIngestor

app = FastAPI(
    title="Trading Analytics API",
    description="Real-time trading data and risk analytics for investment firms",
    version="2.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize components
risk_calculator = RiskCalculator()
pnl_processor = PnLProcessor()
market_ingestor = MarketDataIngestor()

# WebSocket connections
active_connections = []

# Pydantic models
class TradeRequest(BaseModel):
    symbol: str
    side: str = Field(..., regex="^(BUY|SELL)$")
    quantity: int
    price: float
    trade_type: str = Field("MARKET", regex="^(MARKET|LIMIT)$")

class TradeResponse(BaseModel):
    trade_id: str
    symbol: str
    side: str
    quantity: int
    price: float
    timestamp: str
    realized_pnl: Optional[float]
    commission: float
    net_pnl: float

class RiskRequest(BaseModel):
    portfolio_id: str
    confidence_level: float = 0.95
    time_horizon: str = "1D"

class RiskResponse(BaseModel):
    portfolio_id: str
    var_95: float
    var_99: float
    expected_shortfall: float
    volatility: float
    sharpe_ratio: float
    max_drawdown: float

class MarketDataResponse(BaseModel):
    symbol: str
    price: float
    bid: float
    ask: float
    volume: int
    timestamp: str
    change: float
    change_percent: float

@app.get("/")
async def root():
    return {
        "service": "Trading Analytics API",
        "version": "2.0.0",
        "status": "operational",
        "endpoints": {
            "health": "/health",
            "market_data": "/market/{symbol}",
            "trade": "/trade",
            "risk": "/risk/{portfolio_id}",
            "pnl": "/pnl/{portfolio_id}",
            "websocket": "/ws"
        }
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {
            "database": "connected",
            "event_hub": "connected",
            "redis": "connected"
        }
    }

@app.get("/market/{symbol}", response_model=MarketDataResponse)
async def get_market_data(symbol: str):
    """Get real-time market data for a symbol"""
    try:
        # In production, this would fetch from your data source
        data = await market_ingestor.fetch_polygon_data(symbol)
        if not data:
            raise HTTPException(status_code=404, detail="Symbol not found")
        
        return MarketDataResponse(
            symbol=data['symbol'],
            price=data['price'],
            bid=data['price'] * 0.999,  # Simplified bid-ask
            ask=data['price'] * 1.001,
            volume=data.get('size', 0),
            timestamp=data['timestamp'],
            change=np.random.normal(0, 1),  # Placeholder
            change_percent=np.random.normal(0, 0.01)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/trade", response_model=TradeResponse)
async def execute_trade(trade: TradeRequest):
    """Execute a trade and calculate P&L"""
    try:
        # Get current market price
        market_data = await market_ingestor.fetch_polygon_data(trade.symbol)
        if not market_data:
            raise HTTPException(status_code=404, detail="Symbol not found")
        
        # Create trade record
        trade_data = {
            'trade_id': f"TRADE_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            'symbol': trade.symbol,
            'side': trade.side,
            'quantity': trade.quantity,
            'price': market_data['price'],
            'timestamp': datetime.now().isoformat(),
            'trade_type': trade.trade_type
        }
        
        # Calculate P&L
        processed_trade = pnl_processor.calculate_trade_pnl(trade_data)
        pnl_processor.update_position(processed_trade)
        pnl_processor.trades.append(processed_trade)
        
        return TradeResponse(
            trade_id=processed_trade['trade_id'],
            symbol=processed_trade['symbol'],
            side=processed_trade['side'],
            quantity=processed_trade['quantity'],
            price=processed_trade['price'],
            timestamp=processed_trade['timestamp'],
            realized_pnl=processed_trade.get('realized_pnl'),
            commission=processed_trade.get('commission', 0),
            net_pnl=processed_trade.get('net_pnl', 0)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/risk/{portfolio_id}", response_model=RiskResponse)
async def get_portfolio_risk(portfolio_id: str):
    """Calculate risk metrics for a portfolio"""
    try:
        # In production, this would fetch actual portfolio data
        # For demo, generate sample returns
        np.random.seed(42)
        returns = pd.Series(np.random.normal(0.001, 0.02, 1000))
        
        var_results = risk_calculator.calculate_var(returns, 'all')
        es = risk_calculator.calculate_expected_shortfall(returns)
        
        return RiskResponse(
            portfolio_id=portfolio_id,
            var_95=var_results.get('historical_var', 0),
            var_99=np.percentile(returns, 1),
            expected_shortfall=es,
            volatility=returns.std() * np.sqrt(252),  # Annualized
            sharpe_ratio=(returns.mean() * 252) / (returns.std() * np.sqrt(252)),
            max_drawdown=0.15  # Placeholder
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pnl/{portfolio_id}")
async def get_pnl(portfolio_id: str, period: str = "daily"):
    """Get P&L for a portfolio"""
    try:
        if period == "daily":
            pnl = pnl_processor.calculate_daily_pnl()
        elif period == "monthly":
            pnl = pnl_processor.calculate_mtd_pnl()
        elif period == "yearly":
            pnl = pnl_processor.calculate_ytd_pnl()
        else:
            raise HTTPException(status_code=400, detail="Invalid period")
        
        return pnl
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for real-time market data"""
    await websocket.accept()
    active_connections.append(websocket)
    
    try:
        while True:
            # Receive client message
            data = await websocket.receive_text()
            request = json.loads(data)
            
            if request.get('type') == 'subscribe':
                symbols = request.get('symbols', [])
                
                # Send real-time updates
                while True:
                    for symbol in symbols:
                        market_data = await market_ingestor.fetch_polygon_data(symbol)
                        if market_data:
                            await websocket.send_json({
                                'type': 'market_update',
                                'symbol': symbol,
                                'data': market_data,
                                'timestamp': datetime.now().isoformat()
                            })
                    await asyncio.sleep(1)
                    
    except WebSocketDisconnect:
        active_connections.remove(websocket)

@app.on_event("startup")
async def startup_event():
    """Initialize on startup"""
    logger.info("Starting Trading Analytics API...")
    # Start background tasks
    asyncio.create_task(market_ingestor.run_continuously())

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("Shutting down...")
    for connection in active_connections:
        await connection.close()
