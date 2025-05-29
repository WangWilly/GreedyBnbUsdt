from pydantic import Field
from pydantic_settings import BaseSettings

from pkgs.managers.position.manager import ManagerPosition
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

################################################################################

class ManagerAdvancedRisk:
    def __init__(self, config: ManagerAdvancedRiskConfig, position_manager: ManagerPosition):
        self.logger = get_logger_named("ManagerAdvancedRisk")
        self.cfg = config
        self.position_manager = position_manager
        self.last_position_ratio = 0
    
    async def multi_layer_check(self) -> bool:
        try:
            position_ratio = await self.position_manager.get_position_ratio()
            
            # 只在仓位比例变化超过0.1%时打印日志
            if self.last_position_ratio == 0 or abs(position_ratio - self.last_position_ratio) > 0.001:
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
                
            return False
        except Exception as e:
            self.logger.error(f"风控检查失败: {str(e)}")
            return False
