import time
import asyncio
from datetime import datetime

import ccxt.async_support as ccxt
from pydantic import Field
from pydantic_settings import BaseSettings

from pkgs.utils.logging import get_logger_named


################################################################################

PREFIX = "EXCHANGE_CLIENT_"


class ExchangeClientConfig(BaseSettings):
    DEBUG_MODE: bool = Field(
        default=False,
        alias=PREFIX + "DEBUG",
        description="Debug mode",
    )
    SYMBOL: str = Field(
        default="BNB/USDT",
        alias=PREFIX + "SYMBOL",
        description="Trading symbol",
    )

    HTTP_PROXY: str | None = Field(
        default=None,
        alias=PREFIX + "HTTP_PROXY",
        description="HTTP proxy for requests, if needed",
    )

    BINANCE_API_KEY: str = Field(
        default="",
        alias=PREFIX + "BINANCE_API_KEY",
        description="Binance API key, set via environment variable",
    )
    BINANCE_API_SECRET: str = Field(
        default="",
        alias=PREFIX + "BINANCE_API_SECRET",
        description="Binance API secret, set via environment variable",
    )


################################################################################


class ExchangeClient:
    def __init__(self, cfg: ExchangeClientConfig):
        self.logger = get_logger_named("ExchangeClient")
        self.symbol = cfg.SYMBOL

        ########################################################################

        self.exchange = ccxt.binance(
            {
                "apiKey": cfg.BINANCE_API_KEY,
                "secret": cfg.BINANCE_API_SECRET,
                "enableRateLimit": True,
                "timeout": 60000,  # 60 seconds timeout
                "options": {
                    "defaultType": "spot",
                    "fetchMarkets": {
                        "spot": True,  # Enable spot market
                        "margin": False,  # Explicitly disable margin
                        "swap": False,  # Disable swap
                        "future": False,  # Disable futures
                    },
                    "fetchCurrencies": False,
                    "recvWindow": 5000,  # Fixed receive window
                    "adjustForTimeDifference": True,  # Enable time adjustment
                    "warnOnFetchOpenOrdersWithoutSymbol": False,
                    "createMarketBuyOrderRequiresPrice": False,
                },
                "aiohttp_proxy": cfg.HTTP_PROXY,  # Use proxy config from environment variable
                "verbose": cfg.DEBUG_MODE,
            }
        )

        if cfg.HTTP_PROXY:
            self.logger.info(f"Proxy configured: {cfg.HTTP_PROXY}")
        self.logger.info("ExchangeClient initialized with Binance API")

        self.markets_loaded = False
        self.time_diff = 0
        self.balance_cache = {"timestamp": 0, "data": None}
        self.funding_balance_cache = {"timestamp": 0, "data": {}}
        self.cache_ttl = 30  # Cache validity period (seconds)

    ############################################################################
    # core methods

    async def load_markets(self) -> bool:
        self.logger.info("Time synchronization in progress...")
        try:
            await self.__sync_time()
        except Exception as e:
            self.logger.error(f"Time synchronization failed: {str(e)}")
            return False

        self.logger.info("Loading market data...")
        max_retries = 3
        for i in range(max_retries):
            try:
                await self.exchange.load_markets()
                self.markets_loaded = True
                market = self.exchange.market(self.symbol)
                self.logger.info(
                    f"Market data loaded successfully for {self.symbol} | Market ID: {market['id']}"
                )
                return True
            except Exception as e:
                if i == max_retries - 1:
                    self.logger.error(
                        f"Failed to load market data after {max_retries} attempts: {str(e)}"
                    )
                    continue
                self.logger.warning(
                    f"Attempt {i + 1} to load market data failed: {str(e)}. Retrying in 2 seconds..."
                )
                await asyncio.sleep(2)

        self.logger.error("Failed to load market data after multiple attempts")
        return False

    async def fetch_ohlcv(self, symbol, timeframe="1h", limit=None):
        try:
            params = {}
            if limit:
                params["limit"] = limit
            return await self.exchange.fetch_ohlcv(symbol, timeframe, params=params)
        except Exception as e:
            self.logger.error(f"Fetching OHLCV data failed: {str(e)}")
            raise

    ############################################################################

    async def create_order(self, symbol, type, side, amount, price):
        try:
            # Resync time before placing order
            await self.sync_time()
            # Add timestamp to request params
            params = {
                "timestamp": int(time.time() * 1000 + self.time_diff),
                "recvWindow": 5000,
                # "test": True,
            }
            return await self.exchange.create_order(
                symbol, type, side, amount, price, params
            )
        except Exception as e:
            self.logger.error(f"Order placement failed: {str(e)}")
            raise

    async def create_market_order(
        self,
        symbol: str,
        side: str,  # must be 'buy' or 'sell'
        amount: float,
        params: dict | None = None,
    ):
        """
        Convenience wrapper for market orders.
        Actually calls ccxt's create_order with type fixed as 'market'.
        """
        # Ensure params is a dict
        params = params or {}

        # Sync time before placing order to avoid -1021 error
        await self.sync_time()
        params.update(
            {
                "timestamp": int(time.time() * 1000 + self.time_diff),
                "recvWindow": 5000,
                # "test": True,
            }
        )

        order = await self.exchange.create_order(
            symbol=symbol,
            type="market",
            side=side.lower(),  # ccxt expects lowercase
            amount=amount,
            price=None,  # price must be None for market orders
            params=params,
        )
        return order

    async def fetch_order(self, order_id, symbol, params=None):
        if params is None:
            params = {}
        params["timestamp"] = int(time.time() * 1000 + self.time_diff)
        params["recvWindow"] = 5000
        return await self.exchange.fetch_order(order_id, symbol, params)

    async def fetch_open_orders(self, symbol):
        """Fetch current open orders"""
        return await self.exchange.fetch_open_orders(symbol)

    async def cancel_order(self, order_id, symbol, params=None):
        """Cancel specified order"""
        if params is None:
            params = {}
        params["timestamp"] = int(time.time() * 1000 + self.time_diff)
        params["recvWindow"] = 5000
        return await self.exchange.cancel_order(order_id, symbol, params)

    async def close(self):
        """Close exchange connection"""
        try:
            if self.exchange:
                await self.exchange.close()
                self.logger.info("Exchange connection closed safely")
        except Exception as e:
            self.logger.error(f"Error occurred while closing connection: {str(e)}")

    async def fetch_order_book(self, symbol, limit=5):
        """Fetch order book data"""
        try:
            market = self.exchange.market(symbol)
            return await self.exchange.fetch_order_book(market["id"], limit=limit)
        except Exception as e:
            self.logger.error(f"Failed to fetch order book: {str(e)}")
            raise

    ############################################################################
    # trader runtime info utils

    async def fetch_my_trades(self, symbol, limit=10):
        self.logger.debug(f"Fetching recent trades for {symbol} with limit {limit}...")
        if not self.markets_loaded:
            await self.load_markets()
        try:
            # Ensure using market ID
            market = self.exchange.market(symbol)
            trades = await self.exchange.fetch_my_trades(market["id"], limit=limit)
            self.logger.info(f"Fetched {len(trades)} trades for {symbol}")
            return trades
        except Exception as e:
            self.logger.error(f"Fetching trades failed: {str(e)}")
            return []

    async def fetch_balance(self, params=None):
        now = time.time()
        if now - self.balance_cache["timestamp"] < self.cache_ttl:
            return self.balance_cache["data"]

        try:
            params = params or {}
            params["timestamp"] = int(time.time() * 1000) + self.time_diff
            balance = await self.exchange.fetch_balance(params)

            # Get funding account balance
            funding_balance = await self.fetch_funding_balance()

            # Merge spot and funding balances
            for asset, amount in funding_balance.items():
                if asset not in balance["total"]:
                    balance["total"][asset] = 0
                if asset not in balance["free"]:
                    balance["free"][asset] = 0
                balance["total"][asset] += amount

            self.logger.debug(f"Account balance summary: {balance['total']}")
            self.balance_cache = {"timestamp": now, "data": balance}
            return balance
        except Exception as e:
            self.logger.error(f"Failed to fetch balance: {str(e)}")
            # On error, return an empty but structurally complete balance dict
            return {"free": {}, "used": {}, "total": {}}

    async def fetch_funding_balance(self):
        now = time.time()

        # If cache is valid, return cached data
        if now - self.funding_balance_cache["timestamp"] < self.cache_ttl:
            return self.funding_balance_cache["data"]

        try:
            # Use new Simple Earn API
            result = await self.exchange.sapi_get_simple_earn_flexible_position()
            self.logger.debug(f"Funding account raw data: {result}")
            balances = {}

            # Handle returned data structure
            data = result.get("rows", []) if isinstance(result, dict) else result

            for item in data:
                asset = item["asset"]
                amount = float(item.get("totalAmount", 0) or item.get("amount", 0))
                balances[asset] = amount

            # Only log when balance changes significantly
            if not self.funding_balance_cache.get("data"):
                self.logger.info(f"Funding account balance: {balances}")
            else:
                # Check for significant change (>0.1%)
                old_balances = self.funding_balance_cache["data"]
                significant_change = False
                for asset, amount in balances.items():
                    old_amount = old_balances.get(asset, 0)
                    if old_amount == 0:
                        if amount != 0:
                            significant_change = True
                            break
                    elif abs((amount - old_amount) / old_amount) > 0.001:
                        significant_change = True
                        break

                if significant_change:
                    self.logger.info(f"Funding account balance updated: {balances}")

            # Update cache
            self.funding_balance_cache = {"timestamp": now, "data": balances}

            return balances
        except Exception as e:
            self.logger.error(f"Failed to fetch funding account balance: {str(e)}")
            return {}

    async def fetch_ticker(self, symbol):
        self.logger.debug(f"Fetching ticker for {symbol}...")
        start = datetime.now()
        try:
            # Use market ID for request
            market = self.exchange.market(symbol)
            ticker = await self.exchange.fetch_ticker(market["id"])
            latency = (datetime.now() - start).total_seconds()
            self.logger.debug(
                f"Fetched ticker successfully | Latency: {latency:.3f}s | Last price: {ticker['last']}"
            )
            return ticker
        except Exception as e:
            self.logger.error(f"Failed to fetch ticker: {str(e)}")
            self.logger.debug(f"Request params: symbol={symbol}")
            raise

    ############################################################################
    # trader runtime transfer utils

    async def transfer_to_savings(self, asset: str, amount):
        try:
            # Get product ID
            product_id = await self.__get_flexible_product_id(asset)

            # Format amount with correct precision
            if asset == "USDT":
                formatted_amount = "{:.2f}".format(float(amount))
            elif asset == "BNB":
                formatted_amount = "{:.8f}".format(float(amount))
            else:
                formatted_amount = str(amount)

            params = {
                "asset": asset,
                "amount": formatted_amount,
                "productId": product_id,
                "timestamp": int(time.time() * 1000 + self.time_diff),
            }
            self.logger.info(f"Starting subscription: {formatted_amount} {asset} to flexible savings")
            result = await self.exchange.sapi_post_simple_earn_flexible_subscribe(
                params
            )
            self.logger.info(f"Transfer successful: {result}")

            # Clear balance cache after subscription to ensure next fetch is up to date
            self.balance_cache = {"timestamp": 0, "data": None}
            self.funding_balance_cache = {"timestamp": 0, "data": {}}

            return result
        except Exception as e:
            self.logger.error(f"Subscription failed: {str(e)}")
            raise

    async def transfer_to_spot(self, asset: str, amount):
        """Redeem from flexible savings to spot account"""
        try:
            # Get product ID
            product_id = await self.get_flexible_product_id(asset)

            # Format amount with correct precision
            if asset == "USDT":
                formatted_amount = "{:.2f}".format(float(amount))
            elif asset == "BNB":
                formatted_amount = "{:.8f}".format(float(amount))
            else:
                formatted_amount = str(amount)

            params = {
                "asset": asset,
                "amount": formatted_amount,
                "productId": product_id,
                "timestamp": int(time.time() * 1000 + self.time_diff),
                "redeemType": "FAST",  # Fast redemption
            }
            self.logger.info(f"Starting redemption: {formatted_amount} {asset} to spot")
            result = await self.exchange.sapi_post_simple_earn_flexible_redeem(params)
            self.logger.info(f"Transfer successful: {result}")

            # Clear balance cache after redemption to ensure next fetch is up to date
            self.balance_cache = {"timestamp": 0, "data": None}
            self.funding_balance_cache = {"timestamp": 0, "data": {}}

            return result
        except Exception as e:
            self.logger.error(f"Redemption failed: {str(e)}")
            raise

    ############################################################################
    # helper methods

    async def __sync_time(self):
        try:
            server_time = await self.exchange.fetch_time()
            local_time = int(time.time() * 1000)
            self.time_diff = server_time - local_time
            self.logger.info(
                f"Time synchronized successfully | Server time: {server_time}, Local time: {local_time}, Time diff: {self.time_diff} ms"
            )
        except Exception as e:
            self.logger.error(f"Time synchronization failed: {str(e)}")

    async def __get_flexible_product_id(self, asset):
        """Get flexible savings product ID for the specified asset"""
        try:
            params = {
                "asset": asset,
                "timestamp": int(time.time() * 1000 + self.time_diff),
                "current": 1,  # Current page
                "size": 100,  # Items per page
            }
            result = await self.exchange.sapi_get_simple_earn_flexible_list(params)
            products = result.get("rows", [])

            # Find the flexible savings product for the asset
            for product in products:
                if product["asset"] == asset and product["status"] == "PURCHASING":
                    self.logger.info(f"Found flexible savings product for {asset}: {product['productId']}")
                    return product["productId"]

            raise ValueError(f"No available flexible savings product found for {asset}")
        except Exception as e:
            self.logger.error(f"Failed to get flexible savings product: {str(e)}")
            raise
