#!/usr/bin/env python3
"""
Main ETL pipeline for trading analytics
"""

import asyncio
import logging
from datetime import datetime
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ingestion.market_data_ingestor import MarketDataIngestor
from src.ingestion.trade_ingestor import TradeIngestor
from src.processing.risk_calculator import RiskCalculator
from src.processing.pnl_processor import PnLProcessor
from src.storage.azure_sql_connector import AzureSQLConnector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TradingPipeline:
    """
    Main orchestration pipeline for trading analytics
    """
    
    def __init__(self):
        self.market_ingestor = MarketDataIngestor()
        self.trade_ingestor = TradeIngestor()
        self.risk_calculator = RiskCalculator()
        self.pnl_processor = PnLProcessor()
        self.sql_connector = AzureSQLConnector()
        
        self.pipeline_status = {
            'start_time': None,
            'end_time': None,
            'market_data_processed': 0,
            'trades_processed': 0,
            'risk_calculations': 0,
            'errors': []
        }
    
    async def run_market_ingestion(self):
        """Run market data ingestion"""
        try:
            logger.info("Starting market data ingestion...")
            market_data = await self.market_ingestor.ingest_market_data()
            self.pipeline_status['market_data_processed'] = len(market_data)
            
            # Save to SQL
            if market_data:
                df = pd.DataFrame(market_data)
                self.sql_connector.save_market_data(df)
            
            logger.info(f"✅ Market data ingested: {len(market_data)} records")
        except Exception as e:
            error_msg = f"Market ingestion failed: {e}"
            logger.error(error_msg)
            self.pipeline_status['errors'].append(error_msg)
    
    def run_trade_processing(self):
        """Process trades"""
        try:
            logger.info("Starting trade processing...")
            # Load trades from blob
            trades = self.trade_ingestor.load_trades()
            
            processed_trades = []
            for trade in trades:
                processed = self.trade_ingestor.process_trade(trade)
                self.pnl_processor.update_position(processed)
                processed_trades.append(processed)
            
            self.pipeline_status['trades_processed'] = len(processed_trades)
            
            # Save to SQL
            if processed_trades:
                df = pd.DataFrame(processed_trades)
                self.sql_connector.save_trades(df)
            
            logger.info(f"✅ Trades processed: {len(processed_trades)}")
        except Exception as e:
            error_msg = f"Trade processing failed: {e}"
            logger.error(error_msg)
            self.pipeline_status['errors'].append(error_msg)
    
    def run_risk_calculations(self):
        """Calculate risk metrics"""
        try:
            logger.info("Starting risk calculations...")
            # Load positions from SQL
            positions = self.sql_connector.get_positions()
            
            if not positions.empty:
                # Calculate VaR
                returns = positions['pnl'].values
                var_results = self.risk_calculator.calculate_var(pd.Series(returns))
                
                # Save risk metrics
                risk_df = pd.DataFrame([{
                    'timestamp': datetime.now(),
                    'portfolio_var_95': var_results.get('historical_var'),
                    'portfolio_var_99': np.percentile(returns, 1),
                    'expected_shortfall': self.risk_calculator.calculate_expected_shortfall(pd.Series(returns)),
                    'total_pnl': positions['pnl'].sum()
                }])
                
                self.sql_connector.save_risk_metrics(risk_df)
                self.pipeline_status['risk_calculations'] = 1
                
                logger.info(f"✅ Risk calculations complete")
        except Exception as e:
            error_msg = f"Risk calculations failed: {e}"
            logger.error(error_msg)
            self.pipeline_status['errors'].append(error_msg)
    
    async def run(self):
        """Run the complete pipeline"""
        self.pipeline_status['start_time'] = datetime.now()
        logger.info("="*60)
        logger.info("🚀 STARTING TRADING ANALYTICS PIPELINE")
        logger.info("="*60)
        
        try:
            # Run market ingestion
            await self.run_market_ingestion()
            
            # Run trade processing
            self.run_trade_processing()
            
            # Run risk calculations
            self.run_risk_calculations()
            
            self.pipeline_status['end_time'] = datetime.now()
            duration = (self.pipeline_status['end_time'] - self.pipeline_status['start_time']).total_seconds()
            
            logger.info("="*60)
            logger.info("✅ PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("="*60)
            logger.info(f"📊 Market data processed: {self.pipeline_status['market_data_processed']}")
            logger.info(f"📊 Trades processed: {self.pipeline_status['trades_processed']}")
            logger.info(f"📊 Risk calculations: {self.pipeline_status['risk_calculations']}")
            logger.info(f"⏱️  Duration: {duration:.2f} seconds")
            
            if self.pipeline_status['errors']:
                logger.warning(f"⚠️  Errors encountered: {len(self.pipeline_status['errors'])}")
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            self.pipeline_status['errors'].append(str(e))

async def main():
    pipeline = TradingPipeline()
    await pipeline.run()

if __name__ == "__main__":
    asyncio.run(main())
