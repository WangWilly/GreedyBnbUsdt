import time

from pydantic import Field
from pydantic_settings import BaseSettings

from pkgs.clients.exchange import ExchangeClient
from pkgs.managers.position.manager import ManagerPosition
from pkgs.managers.advancerisk.manager import ManagerAdvancedRisk
from pkgs.utils.logging import get_logger_named

################################################################################

PREFIX = "ACTIONER_S1_"


class ActionerS1Config(BaseSettings):
    S1_LOOKBACK: int = Field(
        default=52,
        alias=PREFIX + "LOOKBACK",
        description="Lookback period for S1 strategy, in days.",
    )
    S1_SELL_TARGET_PCT: float = Field(
        default=0.50,
        alias=PREFIX + "SELL_TARGET_PCT",
        description="Sell target position ratio for S1 strategy.",
    )
    S1_BUY_TARGET_PCT: float = Field(
        default=0.70,
        alias=PREFIX + "BUY_TARGET_PCT",
        description="Buy target position ratio for S1 strategy.",
    )


################################################################################


class ActionerS1:
    """
    Independent position control strategy (S1).
    Based on daily updated 52-day high/low, frequently checks and adjusts position.
    Runs independently from the main grid strategy, does not modify grid base_price.
    """

    def __init__(
        self,
        cfg: ActionerS1Config,
        exchange: ExchangeClient,
        position_manager: ManagerPosition,
        risk_manager: ManagerAdvancedRisk,
    ):
        self.logger = get_logger_named("ActionerS1")
        self.cfg = cfg

        ########################################################################

        self.exchange = exchange
        self.position_manager = position_manager
        self.risk_manager = risk_manager

        ########################################################################

        # S1 state variables
        self.s1_daily_high = None
        self.s1_daily_low = None
        self.s1_last_data_update_ts = 0
        # Daily update interval (seconds), slightly less than 24h to avoid missing updates
        self.daily_update_interval = 23.9 * 60 * 60

        self.logger.info(
            f"S1 Position Controller initialized. Lookback={self.cfg.S1_LOOKBACK} days, Sell Target={self.cfg.S1_SELL_TARGET_PCT*100}%, Buy Target={self.cfg.S1_BUY_TARGET_PCT*100}%."
        )

    ############################################################################

    async def _fetch_and_calculate_s1_levels(self) -> bool:
        """Fetch daily data and calculate 52-day high/low"""
        try:
            # Fetch slightly more daily candles than lookback (+2 buffer)
            limit = self.cfg.S1_LOOKBACK + 2
            klines = await self.exchange.fetch_ohlcv(
                self.position_manager.cfg.SYMBOL, timeframe="1d", limit=limit
            )

            if not klines or len(klines) < self.cfg.S1_LOOKBACK + 1:
                self.logger.warning(
                    f"S1: Insufficient daily klines received ({len(klines)}), cannot update levels."
                )
                return False

            # Use the previous (closed) daily candles for calculation (exclude current unfinished candle)
            relevant_klines = klines[-(self.cfg.S1_LOOKBACK + 1) : -1]

            if len(relevant_klines) < self.cfg.S1_LOOKBACK:
                self.logger.warning(
                    f"S1: Not enough relevant klines ({len(relevant_klines)}) for lookback {self.cfg.S1_LOOKBACK}."
                )
                return False

            # Calculate high/low (index 2 is high, 3 is low)
            self.s1_daily_high = max(float(k[2]) for k in relevant_klines)
            self.s1_daily_low = min(float(k[3]) for k in relevant_klines)
            self.s1_last_data_update_ts = time.time()
            self.logger.info(
                f"S1 Levels Updated: High={self.s1_daily_high:.4f}, Low={self.s1_daily_low:.4f}"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"S1: Failed to fetch or calculate daily levels: {e}", exc_info=False
            )
            return False

    async def update_daily_s1_levels(self) -> None:
        """Check and update S1 52-day high/low once per day"""
        now = time.time()
        if now - self.s1_last_data_update_ts >= self.daily_update_interval:
            self.logger.info("S1: Time to update daily high/low levels...")
            await self._fetch_and_calculate_s1_levels()
        # else: no update needed

    async def _execute_s1_adjustment(self, side: str, amount_bnb: float) -> bool:
        """
        Execute S1 position adjustment order.
        Uses exchange client to place order directly.
        """
        try:
            # 1. Adjust precision
            adjusted_amount = self.position_manager.adjust_amount_precision(amount_bnb)

            if adjusted_amount <= 0:
                self.logger.warning(
                    f"S1: Adjusted amount is zero or negative ({adjusted_amount}), skipping order."
                )
                return False

            # 2. Get current price (for logs and min notional check)
            current_price = await self.position_manager.get_latest_price()
            if not current_price or current_price <= 0:
                self.logger.error(
                    "S1: Invalid current price, cannot execute adjustment."
                )
                return False

            # 3. Check minimum order limits
            symbol_info = self.position_manager.symbol_info
            min_notional = 10  # Default min notional (USDT)
            min_amount_limit = 0.0001  # Default min amount

            if symbol_info:
                limits = symbol_info.get("limits", {})
                min_notional = limits.get("cost", {}).get("min", min_notional)
                min_amount_limit = limits.get("amount", {}).get("min", min_amount_limit)

            if adjusted_amount < min_amount_limit:
                self.logger.warning(
                    f"S1: Adjusted amount {adjusted_amount:.8f} BNB is below minimum amount limit {min_amount_limit:.8f}."
                )
                return False

            if adjusted_amount * current_price < min_notional:
                self.logger.warning(
                    f"S1: Order value {adjusted_amount * current_price:.2f} USDT is below minimum notional value {min_notional:.2f}."
                )
                return False

            # 4. Ensure sufficient trading funds
            if not await self.position_manager.ensure_trading_funds(
                side, adjusted_amount * current_price
            ):
                self.logger.warning(
                    f"S1: Failed to ensure sufficient trading funds for {side} {adjusted_amount:.8f}"
                )
                return False

            self.logger.info(
                f"S1: Placing {side} order for {adjusted_amount:.8f} BNB at market price (approx {current_price})..."
            )

            # 5. Place order using exchange client
            order = await self.exchange.create_market_order(
                symbol=self.position_manager.cfg.SYMBOL,
                side=side.lower(),
                amount=adjusted_amount,
            )

            self.logger.info(
                f"S1: Adjustment order placed successfully. Order ID: {order.get('id', 'N/A')}"
            )

            # 6. After buy, transfer excess funds to savings
            if side.lower() == "buy":
                await self.position_manager.transfer_excess_funds()
                self.logger.info("S1: Checked for excess funds after trade")

            return True

        except Exception as e:
            self.logger.error(
                f"S1: Failed to execute adjustment order ({side} {amount_bnb:.8f}): {e}",
                exc_info=True,
            )
            return False

    async def check_and_execute(self) -> None:
        """
        Frequently check S1 position control conditions and execute adjustment.
        Should be called frequently in the main trading loop.
        """
        # 0. Ensure we have today's S1 boundaries
        if self.s1_daily_high is None or self.s1_daily_low is None:
            self.logger.debug("S1: Daily high/low levels not available yet.")
            return  # Wait for next data update

        # 1. Get current state
        try:
            current_price = await self.position_manager.get_latest_price()
            if not current_price or current_price <= 0:
                self.logger.warning("S1: Invalid current price.")
                return

            position_pct = await self.position_manager.get_position_ratio()
            position_value = await self.position_manager.get_position_value()
            total_assets = await self.position_manager.get_total_assets()
            base_currency = self.position_manager.base_currency
            bnb_balance = await self.position_manager.get_available_balance(
                base_currency
            )

            if total_assets <= 0:
                self.logger.warning("S1: Invalid total assets value.")
                return

        except Exception as e:
            self.logger.error(f"S1: Failed to get current state: {e}")
            return

        # 2. Check S1 conditions
        s1_action = "NONE"
        s1_trade_amount_bnb = 0

        # High level check
        if (
            current_price > self.s1_daily_high
            and position_pct > self.cfg.S1_SELL_TARGET_PCT
        ):
            s1_action = "SELL"
            target_position_value = total_assets * self.cfg.S1_SELL_TARGET_PCT
            sell_value_needed = position_value - target_position_value
            # Ensure not selling negative or zero
            if sell_value_needed > 0:
                s1_trade_amount_bnb = min(
                    sell_value_needed / current_price, bnb_balance
                )
                self.logger.info(
                    f"S1: High level breached. Need to SELL {s1_trade_amount_bnb:.8f} BNB to reach {self.cfg.S1_SELL_TARGET_PCT*100:.0f}% target."
                )
            else:
                s1_action = "NONE"  # Reset if calculation invalid

        # Low level check (use elif to avoid triggering both)
        elif (
            current_price < self.s1_daily_low
            and position_pct < self.cfg.S1_BUY_TARGET_PCT
        ):
            s1_action = "BUY"
            target_position_value = total_assets * self.cfg.S1_BUY_TARGET_PCT
            buy_value_needed = target_position_value - position_value
            # Ensure not buying negative or zero
            if buy_value_needed > 0:
                s1_trade_amount_bnb = buy_value_needed / current_price
                self.logger.info(
                    f"S1: Low level breached. Need to BUY {s1_trade_amount_bnb:.8f} BNB to reach {self.cfg.S1_BUY_TARGET_PCT*100:.0f}% target."
                )
            else:
                s1_action = "NONE"  # Reset

        # 3. If triggered, execute S1 adjustment
        if s1_action != "NONE" and s1_trade_amount_bnb > 1e-9:  # Tiny value check
            self.logger.info(f"S1: Condition met for {s1_action} adjustment.")
            await self._execute_s1_adjustment(s1_action, s1_trade_amount_bnb)
