"""
Advanced risk calculations for trading portfolios
"""

import numpy as np
import pandas as pd
from scipy import stats
from scipy.optimize import minimize
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)

class RiskCalculator:
    """
    Calculate various risk metrics for trading portfolios
    """
    
    def __init__(self, confidence_level: float = 0.95):
        self.confidence_level = confidence_level
        self.risk_free_rate = float(os.getenv('RISK_FREE_RATE', 0.05))
        
    def calculate_var(self, returns: pd.Series, method: str = 'historical') -> Dict:
        """
        Calculate Value at Risk using different methodologies
        """
        results = {}
        
        if method == 'historical':
            # Historical VaR
            results['historical_var'] = np.percentile(returns, (1 - self.confidence_level) * 100)
            
        elif method == 'parametric':
            # Parametric VaR (assuming normal distribution)
            mu = returns.mean()
            sigma = returns.std()
            z_score = stats.norm.ppf(1 - self.confidence_level)
            results['parametric_var'] = mu + z_score * sigma
            
        elif method == 'monte_carlo':
            # Monte Carlo VaR
            n_simulations = 10000
            simulated_returns = np.random.normal(returns.mean(), returns.std(), n_simulations)
            results['monte_carlo_var'] = np.percentile(simulated_returns, (1 - self.confidence_level) * 100)
            
        elif method == 'all':
            results['historical_var'] = self.calculate_var(returns, 'historical')['historical_var']
            results['parametric_var'] = self.calculate_var(returns, 'parametric')['parametric_var']
            results['monte_carlo_var'] = self.calculate_var(returns, 'monte_carlo')['monte_carlo_var']
            
        return results
    
    def calculate_expected_shortfall(self, returns: pd.Series) -> float:
        """
        Calculate Expected Shortfall (Conditional VaR)
        """
        var = self.calculate_var(returns, 'historical')['historical_var']
        expected_shortfall = returns[returns <= var].mean()
        return expected_shortfall
    
    def calculate_greeks(self, option_data: Dict) -> Dict:
        """
        Calculate option Greeks using Black-Scholes
        """
        from py_vollib.black_scholes import black_scholes as bs
        from py_vollib.black_scholes.greeks.analytical import delta, gamma, theta, vega, rho
        
        S = option_data['spot_price']      # Current asset price
        K = option_data['strike_price']     # Strike price
        T = option_data['time_to_expiry']   # Time to expiry in years
        r = self.risk_free_rate              # Risk-free rate
        sigma = option_data['volatility']    # Volatility
        option_type = option_data['option_type']  # 'c' for call, 'p' for put
        
        greeks = {
            'delta': delta(option_type, S, K, T, r, sigma),
            'gamma': gamma(option_type, S, K, T, r, sigma),
            'theta': theta(option_type, S, K, T, r, sigma),
            'vega': vega(option_type, S, K, T, r, sigma),
            'rho': rho(option_type, S, K, T, r, sigma)
        }
        
        return greeks
    
    def calculate_portfolio_risk(self, positions: List[Dict], returns_data: pd.DataFrame) -> Dict:
        """
        Calculate portfolio-level risk metrics
        """
        # Calculate portfolio returns
        portfolio_returns = self._calculate_portfolio_returns(positions, returns_data)
        
        # Basic statistics
        results = {
            'expected_return': portfolio_returns.mean(),
            'volatility': portfolio_returns.std(),
            'sharpe_ratio': (portfolio_returns.mean() - self.risk_free_rate) / portfolio_returns.std(),
            'max_drawdown': self._calculate_max_drawdown(portfolio_returns),
            'var': self.calculate_var(portfolio_returns, 'all'),
            'expected_shortfall': self.calculate_expected_shortfall(portfolio_returns)
        }
        
        return results
    
    def _calculate_portfolio_returns(self, positions: List[Dict], returns_data: pd.DataFrame) -> pd.Series:
        """
        Calculate weighted portfolio returns
        """
        weights = np.array([p['weight'] for p in positions])
        asset_returns = returns_data[positions]
        
        portfolio_returns = (asset_returns * weights).sum(axis=1)
        return portfolio_returns
    
    def _calculate_max_drawdown(self, returns: pd.Series) -> float:
        """
        Calculate maximum drawdown from return series
        """
        cumulative = (1 + returns).cumprod()
        running_max = cumulative.expanding().max()
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = drawdown.min()
        return max_drawdown
    
    def optimize_portfolio(self, returns: pd.DataFrame, method: str = 'sharpe') -> Dict:
        """
        Portfolio optimization using Modern Portfolio Theory
        """
        n_assets = len(returns.columns)
        
        # Calculate expected returns and covariance
        expected_returns = returns.mean()
        cov_matrix = returns.cov()
        
        def portfolio_performance(weights):
            portfolio_return = np.sum(expected_returns * weights)
            portfolio_volatility = np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights)))
            sharpe_ratio = (portfolio_return - self.risk_free_rate) / portfolio_volatility
            return portfolio_return, portfolio_volatility, sharpe_ratio
        
        # Constraints: weights sum to 1
        constraints = ({'type': 'eq', 'fun': lambda x: np.sum(x) - 1})
        
        # Bounds: weights between 0 and 1 (no shorting)
        bounds = tuple((0, 1) for _ in range(n_assets))
        
        if method == 'sharpe':
            # Maximize Sharpe ratio
            def neg_sharpe(weights):
                return -portfolio_performance(weights)[2]
            
            result = minimize(neg_sharpe, 
                            n_assets * [1./n_assets], 
                            method='SLSQP',
                            bounds=bounds,
                            constraints=constraints)
            
        elif method == 'variance':
            # Minimize variance
            def variance(weights):
                return portfolio_performance(weights)[1]
            
            result = minimize(variance,
                            n_assets * [1./n_assets],
                            method='SLSQP',
                            bounds=bounds,
                            constraints=constraints)
        
        optimal_weights = result['x']
        opt_return, opt_vol, opt_sharpe = portfolio_performance(optimal_weights)
        
        return {
            'weights': dict(zip(returns.columns, optimal_weights)),
            'expected_return': opt_return,
            'volatility': opt_vol,
            'sharpe_ratio': opt_sharpe
        }
