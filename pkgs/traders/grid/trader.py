import asyncio
import time
import numpy as np

from pydantic import Field
from pydantic_settings import BaseSettings

from pkgs.actioners.s1.actioner import ActionerS1
from pkgs.managers.advancerisk.manager import ManagerAdvancedRisk
from pkgs.managers.position.manager import ManagerPosition
from pkgs.utils.logging import get_logger_named
from pkgs.clients.exchange import ExchangeClient
from pkgs.managers.order.manager import ManagerOrder

################################################################################

PREFIX = "TRADER_GRID_"


class TraderGridConfig(BaseSettings):
    SYMBOL: str = Field(
        default="BNB/USDT", alias=PREFIX + "SYMBOL", description="Trading symbol"
    )
    INITIAL_BASE_PRICE: float = Field(
        default=0.0,
        alias=PREFIX + "INITIAL_BASE_PRICE",
        description="Initial base price for grid trading",
    )
    INITIAL_GRID: float = Field(
        default=2.0,
        alias=PREFIX + "INITIAL_GRID",
        description="Initial grid size in percentage",
    )

    AUTO_ADJUST_BASE_PRICE: bool = Field(
        default=False,
        alias=PREFIX + "AUTO_ADJUST_BASE_PRICE",
        description="Automatically adjust base price based on market conditions",
    )
    MIN_POSITION_RATIO: float = Field(
        default=0.1,
        alias=PREFIX + "MIN_POSITION_RATIO",
        description="Minimum position ratio to trigger base price adjustment",
    )
    MAX_POSITION_RATIO: float = Field(
        default=0.9,
        alias=PREFIX + "MAX_POSITION_RATIO",
        description="Maximum position ratio to trigger base price adjustment",
    )

    BASE_AMOUNT: float = Field(
        default=50.0,
        alias=PREFIX + "BASE_AMOUNT",
        description="Base amount for each grid trade in BNB",
    )
    MIN_TRADE_AMOUNT: float = Field(
        default=20.0,
        alias=PREFIX + "MIN_TRADE_AMOUNT",
        description="Minimum trade amount in USDT",
    )

    VOLATILITY_WINDOW: int = Field(
        default=24,
        alias=PREFIX + "VOLATILITY_WINDOW",
        description="Volatility calculation window in hours",
    )

    RISK_FACTOR: float = Field(
        default=0.1,
        alias=PREFIX + "RISK_FACTOR",
        description="Risk factor for position sizing",
    )
    SAFETY_MARGIN: float = Field(
        default=0.95,
        alias=PREFIX + "SAFETY_MARGIN",
        description="Safety margin for balance calculations",
    )

    @property
    def DYNAMIC_INTERVAL_PARAMS(self):
        return {
            # Define mapping of volatility ranges to corresponding adjustment intervals (hours)
            "volatility_to_interval_hours": [
                # Format: {'range': [min_volatility (inclusive), max_volatility (exclusive)], 'interval_hours': corresponding_interval_hours}
                {
                    "range": [0, 0.20],
                    "interval_hours": 1.0,
                },  # Volatility < 0.20, interval 1 hour
                {
                    "range": [0.20, 0.40],
                    "interval_hours": 0.5,
                },  # Volatility 0.20 to 0.40, interval 30 minutes
                {
                    "range": [0.40, 0.80],
                    "interval_hours": 0.25,
                },  # Volatility 0.40 to 0.80, interval 15 minutes
                {
                    "range": [0.80, 999],
                    "interval_hours": 0.125,
                },  # Volatility >= 0.80, interval 7.5 minutes
            ],
            # Define a default interval in case volatility calculation fails or no range is matched
            "default_interval_hours": 1.0,
        }

    @property
    def GRID_PARAMS(self):
        return {
            "initial": self.INITIAL_GRID,
            "min": 1.0,
            "max": 4.0,
            "volatility_threshold": {
                "ranges": [
                    {"range": [0, 0.20], "grid": 1.0},  # Volatility 0-20%, grid 1.0%
                    {"range": [0.20, 0.40], "grid": 1.5},  # Volatility 20-40%, grid 1.5%
                    {"range": [0.40, 0.60], "grid": 2.0},  # Volatility 40-60%, grid 2.0%
                    {"range": [0.60, 0.80], "grid": 2.5},  # Volatility 60-80%, grid 2.5%
                    {"range": [0.80, 1.00], "grid": 3.0},  # Volatility 80-100%, grid 3.0%
                    {"range": [1.00, 1.20], "grid": 3.5},  # Volatility 100-120%, grid 3.5%
                    {"range": [1.20, 999], "grid": 4.0},  # Volatility >120%, grid 4.0%
                ]
            },
        }

    @staticmethod
    def flip_threshold(grid_size: float) -> float:
        return (grid_size / 5) / 100


################################################################################


class TraderGrid:
    def __init__(
        self,
        config: TraderGridConfig,
        exchange: ExchangeClient,
        position_manager: ManagerPosition,
        risk_manager: ManagerAdvancedRisk,
        actioner_s1: ActionerS1,
        order_manager: ManagerOrder,
    ):
        self.logger = get_logger_named("TraderGrid")
        self.exchange = exchange
        self.cfg: TraderGridConfig = config

        ############################################################################

        self.base_price = config.INITIAL_BASE_PRICE
        self.grid_size = config.INITIAL_GRID

        self.ORDER_TIMEOUT = 10  # Order timeout in seconds

        # trader states
        self.initialized = False

        # runtime states
        self.highest = None
        self.lowest = None
        self.current_price = None
        self.active_orders = {"buy": None, "sell": None}

        self.pending_orders = {}
        self.order_timestamps = {}

        self.buying_or_selling = False  # Not waiting for buy or sell

        # info
        # self.total_assets = 0 # Will be fetched from position_manager
        self.last_trade_time = None
        self.last_trade_price = None
        self.last_grid_adjust_time = time.time()

        # runtime helpers
        self.position_manager = position_manager
        self.risk_manager = risk_manager
        self.actioner_s1 = actioner_s1
        self.order_manager = order_manager

    ############################################################################
    # initialization and setup

    async def initialize(self):
        if self.initialized:
            return

        self.logger.info("Initializing TraderGrid...")
        try:
            # Initialize position manager first
            if not await self.position_manager.initialize():
                raise RuntimeError("Failed to initialize position manager")

            # Check and transfer initial funds in spot account
            await self._check_and_transfer_initial_funds()

            # Set base price if not provided
            if self.base_price <= 0:
                self.base_price = await self.position_manager.get_latest_price()
                self.logger.info(f"Apply latest price as base price: {self.base_price}")

            if self.base_price is None or self.base_price <= 0:
                raise ValueError(
                    "Failed to fetch initial base price, please check the market data."
                )

            self.logger.info(f"Initial base price set to: {self.base_price} USDT")

            # Fetch and update the latest 10 trade records
            try:
                self.logger.info("Fetching latest 10 trade records...")
                latest_trades = await self.exchange.fetch_my_trades(
                    self.cfg.SYMBOL, limit=10
                )
                if latest_trades:
                    # Convert format to match OrderTracker's expected format (if needed)
                    formatted_trades = []
                    for trade in latest_trades:
                        # Note: The trade structure returned by ccxt may need adjustment
                        # Assume OrderTracker needs timestamp(seconds), side, price, amount, profit, order_id
                        # Profit may need subsequent calculation or default to 0
                        formatted_trade = {
                            "timestamp": trade["timestamp"] / 1000,  # ms to s
                            "side": trade["side"],
                            "price": trade["price"],
                            "amount": trade["amount"],
                            "cost": trade["cost"],  # Keep original cost
                            "fee": trade.get("fee", {}).get("cost", 0),  # Extract fee
                            "order_id": trade.get("order"),  # Associated order ID
                            "profit": 0,  # Set to 0 during initialization, or calculate later
                        }
                        formatted_trades.append(formatted_trade)

                    # Directly replace history in OrderTracker
                    self.order_manager.trade_history = formatted_trades
                    self.order_manager.save_trade_history()  # Save to file
                    self.logger.info(
                        f"History updated with the latest {len(formatted_trades)} trade records."
                    )
                else:
                    self.logger.info("Failed to fetch latest trade records, will use local history.")
            except Exception as trade_fetch_error:
                self.logger.error(f"Error fetching or processing latest trade records: {trade_fetch_error}")

            self.initialized = True
        except Exception as e:
            self.initialized = False
            self.logger.error(f"Initialization failed: {str(e)}")
            raise

    async def _check_and_transfer_initial_funds(self):
        """Check and transfer initial funds"""
        try:
            # Get spot and funding account balances
            balance = (
                await self.exchange.fetch_balance()
            )
            funding_balance = (
                await self.exchange.fetch_funding_balance()
            )
            total_assets = await self.position_manager.get_total_assets()
            current_price = await self.position_manager.get_latest_price()

            # Calculate target position (16% of total assets)
            target_usdt = total_assets * 0.16
            target_bnb = (total_assets * 0.16) / current_price

            # Get spot balances
            usdt_balance = float(balance["free"].get("USDT", 0))
            bnb_balance = float(balance["free"].get("BNB", 0))

            # Calculate total balances (spot + funding)
            total_usdt = usdt_balance + float(funding_balance.get("USDT", 0))
            total_bnb = bnb_balance + float(funding_balance.get("BNB", 0))

            # Adjust USDT balance
            if usdt_balance > target_usdt:
                # Subscribe excess to savings
                transfer_amount = usdt_balance - target_usdt
                self.logger.info(f"Found transferable USDT: {transfer_amount}")
                # --- Add minimum subscription amount check (>= 1 USDT) ---
                if transfer_amount >= 1.0:
                    try:
                        await self.exchange.transfer_to_savings("USDT", transfer_amount)
                        self.logger.info(f"Subscribed {transfer_amount:.2f} USDT to savings")
                    except Exception as e_savings_usdt:
                        self.logger.error(f"Failed to subscribe USDT to savings: {str(e_savings_usdt)}")
                else:
                    self.logger.info(
                        f"Transferable USDT ({transfer_amount:.2f}) is below minimum subscription amount 1.0 USDT, skipping subscription"
                    )
            elif usdt_balance < target_usdt:
                # Redeem shortfall from savings
                transfer_amount = target_usdt - usdt_balance
                self.logger.info(f"Redeeming USDT from savings: {transfer_amount}")
                # Similarly, redeeming USDT might require a minimum amount check, add if errors occur
                try:
                    await self.exchange.transfer_to_spot("USDT", transfer_amount)
                    self.logger.info(f"Redeemed {transfer_amount:.2f} USDT from savings")
                except Exception as e_spot_usdt:
                    self.logger.error(f"Failed to redeem USDT from savings: {str(e_spot_usdt)}")

            # Adjust BNB balance
            if bnb_balance > target_bnb:
                # Subscribe excess to savings
                transfer_amount = bnb_balance - target_bnb
                self.logger.info(f"Found transferable BNB: {transfer_amount}")
                # --- Add minimum subscription amount check ---
                if transfer_amount >= 0.01:
                    try:
                        await self.exchange.transfer_to_savings("BNB", transfer_amount)
                        self.logger.info(f"Subscribed {transfer_amount:.4f} BNB to savings")
                    except Exception as e_savings:
                        self.logger.error(f"Failed to subscribe BNB to savings: {str(e_savings)}")
                else:
                    self.logger.info(
                        f"Transferable BNB ({transfer_amount:.4f}) is below minimum subscription amount 0.01 BNB, skipping subscription"
                    )
            elif bnb_balance < target_bnb:
                # Redeem shortfall from savings
                transfer_amount = target_bnb - bnb_balance
                self.logger.info(f"Redeeming BNB from savings: {transfer_amount}")
                # Redemption operations usually have different minimum limits, or lower limits, no check added for now
                # If redemption also encounters -6005, corresponding minimum redemption amount check needs to be added here
                try:
                    await self.exchange.transfer_to_spot("BNB", transfer_amount)
                    self.logger.info(f"Redeemed {transfer_amount:.4f} BNB from savings")
                except Exception as e_spot:
                    self.logger.error(f"Failed to redeem BNB from savings: {str(e_spot)}")

            self.logger.info(
                f"Fund allocation complete\n" f"USDT: {total_usdt:.2f}\n" f"BNB: {total_bnb:.4f}"
            )
        except Exception as e:
            self.logger.error(f"Initial fund check failed: {str(e)}")

    ############################################################################

    async def _calculate_dynamic_interval_seconds(self):
        """Dynamically calculate grid adjustment interval in seconds based on volatility"""
        try:
            volatility = await self._calculate_volatility()
            if volatility is None:
                raise ValueError("Volatility calculation failed")

            interval_rules = self.cfg.DYNAMIC_INTERVAL_PARAMS[
                "volatility_to_interval_hours"
            ]
            default_interval_hours = self.cfg.DYNAMIC_INTERVAL_PARAMS[
                "default_interval_hours"
            ]

            matched_interval_hours = default_interval_hours

            for rule in interval_rules:
                vol_range = rule["range"]
                if vol_range[0] <= volatility < vol_range[1]:
                    matched_interval_hours = rule["interval_hours"]
                    self.logger.debug(
                        f"Dynamic interval match: Volatility {volatility:.4f} in range {vol_range}, interval {matched_interval_hours} hours"
                    )
                    break

            interval_seconds = matched_interval_hours * 3600
            min_interval_seconds = 5 * 60  # Minimum 5 minutes
            final_interval_seconds = max(interval_seconds, min_interval_seconds)

            self.logger.debug(
                f"Calculated dynamic adjustment interval: {final_interval_seconds:.0f} seconds ({final_interval_seconds/3600:.2f} hours)"
            )
            return final_interval_seconds

        except Exception as e:
            self.logger.error(
                f"Failed to calculate dynamic adjustment interval: {e}, using default interval."
            )
            default_interval_hours = self.cfg.DYNAMIC_INTERVAL_PARAMS.get(
                "default_interval_hours", 1.0
            )
            return default_interval_hours * 3600

    async def adjust_grid_size(self):
        """Adjust grid size based on volatility and market trend"""
        try:
            volatility = await self._calculate_volatility()
            self.logger.info(f"Current volatility: {volatility:.4f}")

            base_grid = None
            for range_config in self.cfg.GRID_PARAMS["volatility_threshold"]["ranges"]:
                if range_config["range"][0] <= volatility < range_config["range"][1]:
                    base_grid = range_config["grid"]
                    break

            if base_grid is None:
                base_grid = self.cfg.INITIAL_GRID

            new_grid = base_grid

            new_grid = max(
                min(new_grid, self.cfg.GRID_PARAMS["max"]), self.cfg.GRID_PARAMS["min"]
            )

            if new_grid != self.grid_size:
                self.logger.info(
                    f"Adjusting grid size | "
                    f"Volatility: {volatility:.2%} | "
                    f"Old grid: {self.grid_size:.2f}% | "
                    f"New grid: {new_grid:.2f}%"
                )
                self.grid_size = new_grid

        except Exception as e:
            self.logger.error(f"Failed to adjust grid size: {str(e)}")

    async def _calculate_volatility(self):
        """Calculate price volatility"""
        try:
            klines = await self.exchange.fetch_ohlcv(
                self.cfg.SYMBOL, timeframe="1h", limit=self.cfg.VOLATILITY_WINDOW
            )

            if not klines:
                return 0

            prices = [float(k[4]) for k in klines]  # Closing prices
            returns = np.diff(np.log(prices))

            volatility = np.std(returns) * np.sqrt(24 * 365)  # Annualized volatility
            return volatility

        except Exception as e:
            self.logger.error(f"Failed to calculate volatility: {str(e)}")
            return 0

    ############################################################################

    async def _calculate_order_amount(self):
        """Calculate target order amount (10% of total assets)"""
        try:
            current_time = time.time()

            # Use cache to avoid frequent calculations and log output
            cache_key = f"order_amount_target"
            if (
                hasattr(self, cache_key)
                and current_time - getattr(self, f"{cache_key}_time") < 60
            ):
                return getattr(self, cache_key)

            total_assets = await self.position_manager.get_total_assets()

            amount = total_assets * 0.1

            # Log only if the amount changes by more than 1%
            if (
                not hasattr(self, f"{cache_key}_last")
                or abs(amount - getattr(self, f"{cache_key}_last", 0))
                / max(getattr(self, f"{cache_key}_last", 0.01), 0.01)
                > 0.01
            ):
                self.logger.info(
                    f"Target order amount calculation | "
                    f"Total assets: {total_assets:.2f} USDT | "
                    f"Calculated amount (10%): {amount:.2f} USDT"
                )
                setattr(self, f"{cache_key}_last", amount)

            setattr(self, cache_key, amount)
            setattr(self, f"{cache_key}_time", current_time)

            return amount

        except Exception as e:
            self.logger.error(f"Failed to calculate target order amount: {str(e)}")
            return getattr(self, cache_key, 0)

    ############################################################################

    async def execute_order(self, side):
        """Execute order with retry mechanism"""
        max_retries = 10
        retry_count = 0
        check_interval = 3  # Time to wait for check after placing order (seconds)

        while retry_count < max_retries:
            try:
                order_book = await self.exchange.fetch_order_book(
                    self.cfg.SYMBOL, limit=5
                )
                if (
                    not order_book
                    or not order_book.get("asks")
                    or not order_book.get("bids")
                ):
                    self.logger.error("Failed to fetch order book data or data is incomplete")
                    retry_count += 1
                    await asyncio.sleep(3)
                    continue

                if side == "buy":
                    order_price = order_book["asks"][0][0]
                else:
                    order_price = order_book["bids"][0][0]

                amount_usdt = await self._calculate_order_amount()
                amount = self.position_manager.adjust_amount_precision(
                    amount_usdt / order_price
                )

                if not await self.position_manager.ensure_trading_funds(
                    side, amount_usdt
                ):
                    self.logger.warning(
                        f"{side.capitalize()} balance insufficient or transfer failed, attempt {retry_count + 1} aborted"
                    )
                    return False

                self.logger.info(
                    f"Attempting {retry_count + 1}/{max_retries} {side} order | "
                    f"Price: {order_price} | "
                    f"Amount: {amount_usdt:.2f} USDT | "
                    f"Quantity: {amount:.8f} BNB"
                )

                order = await self.exchange.create_order(
                    self.cfg.SYMBOL, "limit", side, amount, order_price
                )

                order_id = order["id"]
                self.active_orders[side] = order_id
                self.order_manager.add_order(order)

                self.logger.info(f"Order submitted, waiting {check_interval} seconds to check status")
                await asyncio.sleep(check_interval)

                updated_order = await self.exchange.fetch_order(
                    order_id, self.cfg.SYMBOL
                )

                if updated_order["status"] == "closed":
                    self.logger.info(f"Order filled | ID: {order_id}")
                    self.base_price = float(updated_order["price"])
                    self.active_orders[side] = None

                    trade_info = {
                        "timestamp": time.time(),
                        "side": side,
                        "price": float(updated_order["price"]),
                        "amount": float(updated_order["filled"]),
                        "order_id": updated_order["id"],
                    }
                    self.order_manager.add_trade(trade_info)

                    self.last_trade_time = time.time()
                    self.last_trade_price = float(updated_order["price"])

                    self.logger.info(f"Base price updated: {self.base_price}")

                    await self.position_manager.transfer_excess_funds()

                    return updated_order

                self.logger.warning(
                    f"Order not filled, attempting to cancel | ID: {order_id} | Status: {updated_order['status']}"
                )
                try:
                    await self.exchange.cancel_order(order_id, self.cfg.SYMBOL)
                    self.logger.info(f"Order cancelled, preparing to retry | ID: {order_id}")
                except Exception as e:
                    self.logger.warning(f"Error cancelling order: {str(e)}, checking order status again")
                    try:
                        check_order = await self.exchange.fetch_order(
                            order_id, self.cfg.SYMBOL
                        )
                        if check_order["status"] == "closed":
                            self.logger.info(f"Order was already filled | ID: {order_id}")
                            self.base_price = float(check_order["price"])
                            self.active_orders[side] = None
                            trade_info = {
                                "timestamp": time.time(),
                                "side": side,
                                "price": float(check_order["price"]),
                                "amount": float(check_order["filled"]),
                                "order_id": check_order["id"],
                            }
                            self.order_manager.add_trade(trade_info)
                            self.last_trade_time = time.time()
                            self.last_trade_price = float(check_order["price"])
                            await self.position_manager.transfer_excess_funds()
                            self.logger.info(f"Base price updated: {self.base_price}")

                            await self.position_manager.transfer_excess_funds()

                            return check_order
                    except Exception as check_e:
                        self.logger.error(f"Failed to check order status: {str(check_e)}")

                self.active_orders[side] = None

                retry_count += 1

                if retry_count < max_retries:
                    self.logger.info(f"Waiting 1 second before attempt {retry_count + 1}")
                    await asyncio.sleep(1)

            except Exception as e:
                self.logger.error(f"Failed to execute {side} order: {str(e)}")

                if "order_id" in locals() and self.active_orders.get(side) == order_id:
                    try:
                        await self.exchange.cancel_order(order_id, self.cfg.SYMBOL)
                        self.logger.info(f"Cancelled erroneous order | ID: {order_id}")
                    except Exception as cancel_e:
                        self.logger.error(f"Failed to cancel erroneous order: {str(cancel_e)}")
                    finally:
                        self.active_orders[side] = None

                retry_count += 1

                if "资金不足" in str(e) or "Insufficient" in str(e):
                    self.logger.error("Insufficient balance, stopping retries")
                    error_message = f"""❌ Trade Failed
━━━━━━━━━━━━━━━━━━━━
🔍 Type: {side} failed
📊 Pair: {self.cfg.SYMBOL}
⚠️ Error: Insufficient balance
"""
                    return False

                if retry_count < max_retries:
                    self.logger.info(f"Waiting 2 seconds before attempt {retry_count + 1}")
                    await asyncio.sleep(2)

        if retry_count >= max_retries:
            self.logger.error(f"{side} order execution failed, reached max retries: {max_retries}")
            error_message = f"""❌ Trade Failed
━━━━━━━━━━━━━━━━━━━━
🔍 Type: {side} failed
📊 Pair: {self.cfg.SYMBOL}
⚠️ Error: Reached max retries {max_retries}
"""

        return False

    ############################################################################
    # balance checking

    async def check_buy_balance(self):
        """Check balance before buying, redeem from savings if insufficient"""
        try:
            amount_usdt = await self._calculate_order_amount()

            if await self.position_manager.ensure_trading_funds("BUY", amount_usdt):
                self.logger.info(
                    f"Buy funds confirmed or prepared: {amount_usdt:.2f} {self.position_manager.quote_currency}"
                )
                return True
            else:
                self.logger.error(
                    f"Buy funds insufficient or preparation failed: {amount_usdt:.2f} {self.position_manager.quote_currency}"
                )
                return False

        except Exception as e:
            self.logger.error(f"Failed to check buy balance: {str(e)}")
            return False

    async def check_sell_balance(self):
        """Check balance before selling, redeem from savings if insufficient"""
        try:
            amount_usdt = await self._calculate_order_amount()

            if await self.position_manager.ensure_trading_funds("SELL", amount_usdt):
                self.logger.info(
                    f"Sell funds confirmed or prepared (equivalent to {amount_usdt:.2f} {self.position_manager.quote_currency})"
                )
                return True
            else:
                self.logger.error(
                    f"Sell funds insufficient or preparation failed (equivalent to {amount_usdt:.2f} {self.position_manager.quote_currency})"
                )
                return False

        except Exception as e:
            self.logger.error(f"Failed to check sell balance: {str(e)}")
            return False

    ############################################################################
    # signal checking

    # for grid trading logic
    def _get_upper_band(self):
        return self.base_price * (1 + self.grid_size / 100)

    # for grid trading logic
    def _get_lower_band(self):
        return self.base_price * (1 - self.grid_size / 100)

    async def _check_buy_signal(self):
        current_price = self.current_price
        if current_price <= self._get_lower_band():
            self.buying_or_selling = True  # Enter buy or sell monitoring
            new_lowest = (
                current_price
                if self.lowest is None
                else min(self.lowest, current_price)
            )
            if new_lowest != self.lowest:
                self.lowest = new_lowest
                self.logger.info(
                    f"Buy monitoring | "
                    f"Current price: {current_price:.2f} | "
                    f"Trigger price: {self._get_lower_band():.5f} | "
                    f"Lowest price: {self.lowest:.2f} | "
                    f"Grid lower band: {self._get_lower_band():.2f} | "
                    f"Rebound threshold: {TraderGridConfig.flip_threshold(self.grid_size)*100:.2f}%"
                )
            threshold = TraderGridConfig.flip_threshold(self.grid_size)
            if self.lowest and current_price >= self.lowest * (1 + threshold):
                self.buying_or_selling = False
                self.logger.info(
                    f"Buy signal triggered | Current price: {current_price:.2f} | Rebounded: {(current_price/self.lowest-1)*100:.2f}%"
                )
                if not await self.check_buy_balance():
                    return False
                return True
        else:
            self.buying_or_selling = False
        return False

    async def _check_sell_signal(self):
        current_price = self.current_price
        initial_upper_band = self._get_upper_band()

        position_ratio = await self.position_manager.get_position_ratio()
        if (
            self.cfg.AUTO_ADJUST_BASE_PRICE
            and current_price >= initial_upper_band
            and position_ratio < self.cfg.MIN_POSITION_RATIO
        ):
            old_base_price = self.base_price
            self.base_price = current_price
            self.highest = None

            self.logger.info(
                f"Base price adjusted | "
                f"Reason: Position too low ({position_ratio:.2%} < {self.cfg.MIN_POSITION_RATIO:.2%}) | "
                f"Old base price: {old_base_price:.2f} | "
                f"New base price: {current_price:.2f}"
            )

            return False

        if current_price >= initial_upper_band:
            self.buying_or_selling = True
            new_highest = (
                current_price
                if self.highest is None
                else max(self.highest, current_price)
            )
            threshold = TraderGridConfig.flip_threshold(self.grid_size)

            dynamic_trigger_price = (
                new_highest * (1 - threshold)
                if new_highest is not None
                else initial_upper_band
            )

            if new_highest != self.highest:
                self.highest = new_highest
                dynamic_trigger_price = self.highest * (1 - threshold)

                self.logger.info(
                    f"Sell monitoring | "
                    f"Current price: {current_price:.2f} | "
                    f"Dynamic trigger price: {dynamic_trigger_price:.5f} | "
                    f"Highest price: {self.highest:.2f}"
                )

            if self.highest and current_price <= self.highest * (1 - threshold):
                self.buying_or_selling = False
                self.logger.info(
                    f"Sell signal triggered | Current price: {current_price:.2f} | Target price: {self.highest * (1 - threshold):.5f} | Dropped: {(1-current_price/self.highest)*100:.2f}%"
                )
                if not await self.check_sell_balance():
                    return False
                return True
        else:
            self.buying_or_selling = False
        return False

    ############################################################################

    async def _check_signal_with_retry(
        self, check_func, check_name, max_retries=3, retry_delay=2
    ):
        """Signal check function with retry mechanism

        Args:
            check_func: The check function to execute (_check_buy_signal or _check_sell_signal)
            check_name: Name of the check, for logging
            max_retries: Maximum number of retries
            retry_delay: Retry interval (seconds)

        Returns:
            bool: Check result
        """
        retries = 0
        while retries <= max_retries:
            try:
                return await check_func()
            except Exception as e:
                retries += 1
                if retries <= max_retries:
                    self.logger.warning(
                        f"{check_name} error, retrying attempt {retries} after {retry_delay} seconds: {str(e)}"
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    self.logger.error(
                        f"{check_name} failed, reached max retries ({max_retries}): {str(e)}"
                    )
                    return False
        return False

    async def main_loop(self):
        while True:
            try:
                if not self.initialized:
                    await self.initialize()
                    await self.actioner_s1.update_daily_s1_levels()

                await self.actioner_s1.update_daily_s1_levels()

                current_price = await self.position_manager.get_latest_price()
                if not current_price:
                    await asyncio.sleep(5)
                    continue
                self.current_price = current_price

                sell_signal = await self._check_signal_with_retry(
                    self._check_sell_signal, "Sell check"
                )
                if sell_signal:
                    await self.execute_order("sell")
                else:
                    buy_signal = await self._check_signal_with_retry(
                        self._check_buy_signal, "Buy check"
                    )
                    if buy_signal:
                        await self.execute_order("buy")
                    else:
                        if await self.risk_manager.multi_layer_check():
                            await asyncio.sleep(5)
                            continue

                        await self.actioner_s1.check_and_execute()

                        dynamic_interval_seconds = (
                            await self._calculate_dynamic_interval_seconds()
                        )
                        if (
                            time.time() - self.last_grid_adjust_time
                            > dynamic_interval_seconds
                            and not self.buying_or_selling
                        ):
                            self.logger.info(
                                f"Time to adjust grid size (interval: {dynamic_interval_seconds/3600} hours)."
                            )
                            await self.adjust_grid_size()
                            self.last_grid_adjust_time = time.time()

                await asyncio.sleep(5)

            except Exception as e:
                self.logger.error(f"Main loop error: {e}", exc_info=True)
                await asyncio.sleep(30)

    async def emergency_stop(self):
        try:
            open_orders = await self.exchange.fetch_open_orders(self.cfg.SYMBOL)
            for order in open_orders:
                await self.exchange.cancel_order(
                    order["id"], self.cfg.SYMBOL
                )
            self.logger.critical("All trades stopped, entering review procedure")
        except Exception as e:
            self.logger.error(f"Emergency stop failed: {str(e)}")
        finally:
            await self.exchange.close()
            exit()
