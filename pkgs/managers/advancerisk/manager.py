from pydantic import Field
from pydantic_settings import BaseSettings

from pkgs.clients.exchange import ExchangeClient
from pkgs.utils.logging import get_logger_named

################################################################################

PREFIX = "MGR_ADVANCED_RISK_"

class ManagerAdvancedRiskConfig(BaseSettings):
    MIN_POSITION_RATIO: float = Field(
        default=0.1,
        alias=PREFIX + "MIN_POSITION_RATIO",
        description="最小底仓比例，低于此比例将触发底仓保护",
    )
    MAX_POSITION_RATIO: float = Field(
        default=0.9,
        alias=PREFIX + "MAX_POSITION_RATIO",
        description="最大仓位比例，超过此比例将触发风控",
    )
    
    RISK_FACTOR: float = Field(
        default=0.1,
        alias=PREFIX + "RISK_FACTOR",
        description="风险系数，用于调整交易策略的风险承受能力",
    )

################################################################################

class ManagerAdvancedRisk:
    def __init__(self, config: ManagerAdvancedRiskConfig, trader, exchange: ExchangeClient):
        self.logger = get_logger_named("TraderGrid")
        self.cfg = config

        self.trader = trader
        self.exchange = exchange
        
    
    async def multi_layer_check(self):
        try:
            position_ratio = await self._get_position_ratio()
            
            # 保存上次的仓位比例
            if not hasattr(self, 'last_position_ratio'):
                self.last_position_ratio = position_ratio
            
            # 只在仓位比例变化超过0.1%时打印日志
            if abs(position_ratio - self.last_position_ratio) > 0.001:
                self.logger.info(
                    f"风控检查 | "
                    f"当前仓位比例: {position_ratio:.2%} | "
                    f"最大允许比例: {self.cfg.MAX_POSITION_RATIO:.2%} | "
                    f"最小底仓比例: {self.cfg.MIN_POSITION_RATIO:.2%}"
                )
                self.last_position_ratio = position_ratio
            
            if position_ratio < self.cfg.MIN_POSITION_RATIO:
                self.logger.warning(f"底仓保护触发 | 当前: {position_ratio:.2%}")
                return True
            
            if position_ratio > self.cfg.MAX_POSITION_RATIO:
                self.logger.warning(f"仓位超限 | 当前: {position_ratio:.2%}")
                return True
        except Exception as e:
            self.logger.error(f"风控检查失败: {str(e)}")
            return False

    async def _get_position_value(self):
        balance = await self.exchange.fetch_balance()
        funding_balance = await self.exchange.fetch_funding_balance()
        if not self.trader.symbol_info:
            self.trader.trade_log.error("交易对信息未初始化")
            return 0
        base_amount = (
            float(balance.get('free', {}).get(self.trader.symbol_info['base'], 0)) +
            float(funding_balance.get(self.trader.symbol_info['base'], 0))
        )
        current_price = await self.trader._get_latest_price()
        return base_amount * current_price

    async def _get_position_ratio(self):
        """获取当前仓位占总资产比例"""
        try:
            position_value = await self._get_position_value()
            balance = await self.exchange.fetch_balance()
            funding_balance = await self.exchange.fetch_funding_balance()
            
            usdt_balance = (
                float(balance.get('free', {}).get('USDT', 0)) +
                float(funding_balance.get('USDT', 0))
            )
            
            total_assets = position_value + usdt_balance
            if total_assets == 0:
                return 0
                
            ratio = position_value / total_assets
            self.logger.debug(
                f"仓位计算 | "
                f"BNB价值: {position_value:.2f} USDT | "
                f"USDT余额: {usdt_balance:.2f} | "
                f"总资产: {total_assets:.2f} | "
                f"仓位比例: {ratio:.2%}"
            )
            return ratio
        except Exception as e:
            self.logger.error(f"计算仓位比例失败: {str(e)}")
            return 0
