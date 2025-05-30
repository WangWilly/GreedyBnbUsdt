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
        description="Minimum base position ratio; triggers base position protection if below this value",
    )
    MAX_POSITION_RATIO: float = Field(
        default=0.9,
        alias=PREFIX + "MAX_POSITION_RATIO",
        description="Maximum position ratio; triggers risk control if exceeded",
    )


################################################################################


class ManagerAdvancedRisk:
    def __init__(
        self, config: ManagerAdvancedRiskConfig, position_manager: ManagerPosition
    ):
        self.logger = get_logger_named("ManagerAdvancedRisk")
        self.cfg = config
        self.position_manager = position_manager
        self.last_position_ratio = 0

    async def multi_layer_check(self) -> bool:
        try:
            position_ratio = await self.position_manager.get_position_ratio()

            # Only log when position ratio changes by more than 0.1%
            if (
                self.last_position_ratio == 0
                or abs(position_ratio - self.last_position_ratio) > 0.001
            ):
                self.logger.info(
                    f"Risk check | "
                    f"Current position ratio: {position_ratio:.2%} | "
                    f"Max allowed: {self.cfg.MAX_POSITION_RATIO:.2%} | "
                    f"Min base ratio: {self.cfg.MIN_POSITION_RATIO:.2%}"
                )
                self.last_position_ratio = position_ratio

            if position_ratio < self.cfg.MIN_POSITION_RATIO:
                self.logger.warning(f"Base position protection triggered | Current: {position_ratio:.2%}")
                return True

            if position_ratio > self.cfg.MAX_POSITION_RATIO:
                self.logger.warning(f"Position limit exceeded | Current: {position_ratio:.2%}")
                return True

            return False
        except Exception as e:
            self.logger.error(f"Risk check failed: {str(e)}")
            return False
