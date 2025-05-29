import time
import math
from typing import Dict, Optional, Any, Union, Tuple
from pydantic import Field
from pydantic_settings import BaseSettings

from pkgs.clients.exchange import ExchangeClient
from pkgs.utils.logging import get_logger_named

################################################################################

PREFIX = "MGR_POSITION_"

class ManagerPositionConfig(BaseSettings):
    SYMBOL: str = Field(
        default="BNB/USDT",
        alias=PREFIX + "SYMBOL",
        description="Trading symbol"
    )
    SAFETY_MARGIN: float = Field(
        default=0.95,
        alias=PREFIX + "SAFETY_MARGIN",
        description="Safety margin for balance calculations"
    )
    MIN_TRADE_AMOUNT: float = Field(
        default=20.0,
        alias=PREFIX + "MIN_TRADE_AMOUNT",
        description="Minimum trade amount in USDT"
    )

################################################################################

class ManagerPosition:
    """
    Position Manager handles all asset-related operations:
    - Balance retrieval from spot and funding accounts
    - Position calculations and ratios
    - Fund transfers between spot and savings
    - Price data retrieval
    - Amount precision adjustments
    """
    
    def __init__(self, config: ManagerPositionConfig, exchange: ExchangeClient):
        self.logger = get_logger_named("ManagerPosition")
        self.cfg = config
        self.exchange = exchange
        
        # Cache for frequently accessed data
        self._price_cache = {'time': 0, 'value': None}
        self._assets_cache = {'time': 0, 'value': 0}
        self._symbol_info = None
        self._last_logged_assets = 0
        
    async def initialize(self) -> bool:
        """Initialize the position manager with market data"""
        try:
            retry_count = 0
            while not self.exchange.markets_loaded and retry_count < 3:
                try:
                    await self.exchange.load_markets()
                    await self._load_symbol_info()
                    return True
                except Exception as e:
                    self.logger.warning(f"Failed to load markets, retrying... ({retry_count + 1}/3)\n{str(e)}")
                    retry_count += 1
                    if retry_count >= 3:
                        raise
                    await await_sleep(2)
            return False
        except Exception as e:
            self.logger.error(f"Failed to initialize position manager: {str(e)}")
            return False
    
    async def _load_symbol_info(self) -> None:
        """Load symbol information from the exchange"""
        if self.exchange.markets_loaded:
            self._symbol_info = self.exchange.exchange.market(self.cfg.SYMBOL)
            self.logger.info(f"Symbol info loaded for {self.cfg.SYMBOL}")
    
    @property
    def symbol_info(self) -> Dict[str, Any]:
        """Get symbol information"""
        if not self._symbol_info:
            self.logger.warning("Symbol info not loaded yet")
        return self._symbol_info or {}
    
    @property
    def base_currency(self) -> str:
        """Get base currency from symbol info"""
        return self._symbol_info.get('base', 'BNB') if self._symbol_info else 'BNB'
    
    @property
    def quote_currency(self) -> str:
        """Get quote currency from symbol info"""
        return self._symbol_info.get('quote', 'USDT') if self._symbol_info else 'USDT'
    
    async def get_latest_price(self) -> float:
        """Get latest price for the configured symbol"""
        try:
            # Check cache first (valid for 1 second)
            current_time = time.time()
            if current_time - self._price_cache['time'] < 1:
                return self._price_cache['value']
            
            ticker = await self.exchange.fetch_ticker(self.cfg.SYMBOL)
            if ticker and 'last' in ticker:
                price = ticker['last']
                self._price_cache = {'time': current_time, 'value': price}
                return price
            
            self.logger.error("Failed to get latest price: invalid ticker data")
            return self._price_cache['value'] if self._price_cache['value'] else 0
        except Exception as e:
            self.logger.error(f"Failed to get latest price: {str(e)}")
            return self._price_cache['value'] if self._price_cache['value'] else 0
    
    async def get_available_balance(self, currency: str) -> float:
        """Get available balance for a specific currency"""
        try:
            balance = await self.exchange.fetch_balance({'type': 'spot'})
            return float(balance.get('free', {}).get(currency, 0))
        except Exception as e:
            self.logger.error(f"Failed to get available balance for {currency}: {str(e)}")
            return 0
    
    async def get_total_assets(self) -> float:
        """Get total assets value in quote currency"""
        try:
            # Use cache to avoid frequent requests
            current_time = time.time()
            if current_time - self._assets_cache['time'] < 60:  # 1 minute cache
                return self._assets_cache['value']
            
            # Set default return value in case of failure
            default_total = self._assets_cache['value'] if self._assets_cache['value'] else 0
            
            balance = await self.exchange.fetch_balance()
            funding_balance = await self.exchange.fetch_funding_balance()
            current_price = await self.get_latest_price()
            
            if not current_price or current_price <= 0:
                self.logger.error("Invalid price, cannot calculate total assets")
                return default_total
            
            if not balance:
                self.logger.error("Failed to get balance, returning default total assets")
                return default_total
            
            # Get spot and funding account balances
            spot_base = float(balance.get('free', {}).get(self.base_currency, 0) or 0)
            spot_base += float(balance.get('used', {}).get(self.base_currency, 0) or 0)
            
            spot_quote = float(balance.get('free', {}).get(self.quote_currency, 0) or 0)
            spot_quote += float(balance.get('used', {}).get(self.quote_currency, 0) or 0)
            
            fund_base = 0
            fund_quote = 0
            if funding_balance:
                fund_base = float(funding_balance.get(self.base_currency, 0) or 0)
                fund_quote = float(funding_balance.get(self.quote_currency, 0) or 0)
            
            # Calculate total value
            spot_value = spot_quote + (spot_base * current_price)
            fund_value = fund_quote + (fund_base * current_price)
            total_assets = spot_value + fund_value
            
            # Update cache
            self._assets_cache = {
                'time': current_time,
                'value': total_assets
            }
            
            # Only log when asset value changes significantly
            if not self._last_logged_assets or \
               abs(total_assets - self._last_logged_assets) / max(self._last_logged_assets, 0.01) > 0.01:
                self.logger.info(
                    f"Total assets: {total_assets:.2f} {self.quote_currency} | "
                    f"Spot: {spot_value:.2f} | "
                    f"Savings: {fund_value:.2f}"
                )
                self._last_logged_assets = total_assets
            
            return total_assets
        except Exception as e:
            self.logger.error(f"Failed to calculate total assets: {str(e)}")
            return self._assets_cache['value'] if self._assets_cache['value'] else 0
    
    async def get_position_value(self) -> float:
        """Get position value in quote currency"""
        try:
            balance = await self.exchange.fetch_balance()
            funding_balance = await self.exchange.fetch_funding_balance()
            
            base_amount = (
                float(balance.get('free', {}).get(self.base_currency, 0) or 0) +
                float(funding_balance.get(self.base_currency, 0) or 0)
            )
            current_price = await self.get_latest_price()
            return base_amount * current_price
        except Exception as e:
            self.logger.error(f"Failed to get position value: {str(e)}")
            return 0
    
    async def get_position_ratio(self) -> float:
        """Get position ratio (position value / total assets)"""
        try:
            position_value = await self.get_position_value()
            total_assets = await self.get_total_assets()
            
            if total_assets <= 0:
                return 0
            
            ratio = position_value / total_assets
            self.logger.debug(
                f"Position calculation | "
                f"Base value: {position_value:.2f} {self.quote_currency} | "
                f"Total assets: {total_assets:.2f} | "
                f"Position ratio: {ratio:.2%}"
            )
            return ratio
        except Exception as e:
            self.logger.error(f"Failed to calculate position ratio: {str(e)}")
            return 0
    
    def adjust_amount_precision(self, amount: float) -> float:
        """Adjust amount precision according to exchange requirements"""
        try:
            precision = 3  # Default precision for BNB is 3
            
            # Use symbol info if available
            if self._symbol_info and 'precision' in self._symbol_info:
                amount_precision = self._symbol_info['precision'].get('amount')
                if amount_precision is not None:
                    precision = amount_precision
            
            factor = 10 ** precision
            return math.floor(amount * factor) / factor
        except Exception as e:
            self.logger.error(f"Failed to adjust amount precision: {str(e)}")
            # Return original amount with default precision as fallback
            return float(f"{amount:.3f}")
    
    async def transfer_to_spot(self, currency: str, amount: float) -> bool:
        """Transfer funds from savings to spot account"""
        try:
            self.logger.info(f"Transferring {amount:.8f} {currency} from savings to spot")
            await self.exchange.transfer_to_spot(currency, amount)
            return True
        except Exception as e:
            self.logger.error(f"Failed to transfer {amount} {currency} to spot: {str(e)}")
            return False
    
    async def transfer_to_savings(self, currency: str, amount: float) -> bool:
        """Transfer funds from spot to savings account"""
        try:
            self.logger.info(f"Transferring {amount:.8f} {currency} from spot to savings")
            await self.exchange.transfer_to_savings(currency, amount)
            return True
        except Exception as e:
            self.logger.error(f"Failed to transfer {amount} {currency} to savings: {str(e)}")
            return False
    
    async def ensure_trading_funds(self, side: str, amount_value: float) -> bool:
        """Ensure sufficient funds for trading"""
        try:
            current_price = await self.get_latest_price()
            
            if side.upper() == 'BUY':
                # Check USDT balance
                usdt_needed = amount_value
                usdt_available = await self.get_available_balance(self.quote_currency)
                
                if usdt_available >= usdt_needed:
                    return True
                
                # Need to transfer from savings
                funding_balance = await self.exchange.fetch_funding_balance()
                funding_usdt = float(funding_balance.get(self.quote_currency, 0) or 0)
                
                if usdt_available + funding_usdt < usdt_needed:
                    self.logger.error(f"Insufficient {self.quote_currency} for buy order: needed {usdt_needed:.2f}, available {usdt_available:.2f}, savings {funding_usdt:.2f}")
                    return False
                
                # Transfer needed amount with buffer
                transfer_amount = (usdt_needed - usdt_available) * 1.05
                return await self.transfer_to_spot(self.quote_currency, transfer_amount)
            
            elif side.upper() == 'SELL':
                # Check base currency balance
                base_needed = amount_value / current_price if current_price > 0 else 0
                base_available = await self.get_available_balance(self.base_currency)
                
                if base_available >= base_needed:
                    return True
                
                # Need to transfer from savings
                funding_balance = await self.exchange.fetch_funding_balance()
                funding_base = float(funding_balance.get(self.base_currency, 0) or 0)
                
                if base_available + funding_base < base_needed:
                    self.logger.error(f"Insufficient {self.base_currency} for sell order: needed {base_needed:.8f}, available {base_available:.8f}, savings {funding_base:.8f}")
                    return False
                
                # Transfer needed amount with buffer
                transfer_amount = (base_needed - base_available) * 1.05
                return await self.transfer_to_spot(self.base_currency, transfer_amount)
            
            else:
                self.logger.error(f"Invalid side: {side}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to ensure trading funds: {str(e)}")
            return False
    
    async def transfer_excess_funds(self) -> bool:
        """Transfer excess funds to savings account"""
        try:
            balance = await self.exchange.fetch_balance()
            current_price = await self.get_latest_price()
            total_assets = await self.get_total_assets()
            
            if not current_price or current_price <= 0 or total_assets <= 0:
                self.logger.warning("Invalid price or total assets, skipping excess funds transfer")
                return False
            
            # Target holding is 16% of total assets
            target_quote_hold = total_assets * 0.16
            target_base_hold_value = total_assets * 0.16
            target_base_hold_amount = target_base_hold_value / current_price
            
            # Get current spot available balance
            spot_quote_balance = float(balance.get('free', {}).get(self.quote_currency, 0) or 0)
            spot_base_balance = float(balance.get('free', {}).get(self.base_currency, 0) or 0)
            
            self.logger.info(
                f"Excess funds check | Total assets: {total_assets:.2f} | "
                f"Target {self.quote_currency} hold: {target_quote_hold:.2f} | Spot {self.quote_currency}: {spot_quote_balance:.2f} | "
                f"Target {self.base_currency} hold: {target_base_hold_amount:.4f} | Spot {self.base_currency}: {spot_base_balance:.4f}"
            )
            
            transfer_executed = False
            
            # Handle quote currency (USDT)
            if spot_quote_balance > target_quote_hold:
                transfer_amount = spot_quote_balance - target_quote_hold
                # Minimum transfer amount check
                if transfer_amount > 1.0:
                    self.logger.info(f"Transferring excess {self.quote_currency} to savings: {transfer_amount:.2f}")
                    try:
                        await self.exchange.transfer_to_savings(self.quote_currency, transfer_amount)
                        transfer_executed = True
                    except Exception as e:
                        self.logger.error(f"Failed to transfer {self.quote_currency} to savings: {str(e)}")
                else:
                    self.logger.info(f"Excess {self.quote_currency} ({transfer_amount:.2f}) too small, not transferring")
            
            # Handle base currency (BNB)
            if spot_base_balance > target_base_hold_amount:
                transfer_amount = spot_base_balance - target_base_hold_amount
                # Minimum transfer amount check (0.01 BNB)
                if transfer_amount >= 0.01:
                    self.logger.info(f"Transferring excess {self.base_currency} to savings: {transfer_amount:.4f}")
                    try:
                        await self.exchange.transfer_to_savings(self.base_currency, transfer_amount)
                        transfer_executed = True
                    except Exception as e:
                        self.logger.error(f"Failed to transfer {self.base_currency} to savings: {str(e)}")
                else:
                    self.logger.info(f"Excess {self.base_currency} ({transfer_amount:.4f}) too small, not transferring")
            
            return transfer_executed
        except Exception as e:
            self.logger.error(f"Failed to transfer excess funds: {str(e)}")
            return False

# Helper function
async def await_sleep(seconds: float) -> None:
    """Async sleep helper"""
    import asyncio
    await asyncio.sleep(seconds)
