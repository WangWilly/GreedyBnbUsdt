import asyncio
import traceback

from pkgs.actioners.s1.actioner import ActionerS1, ActionerS1Config
from pkgs.clients.exchange import ExchangeClient, ExchangeClientConfig
from pkgs.managers.advancerisk.manager import (
    ManagerAdvancedRisk,
    ManagerAdvancedRiskConfig,
)
from pkgs.managers.order.manager import ManagerOrder
from pkgs.managers.position.manager import ManagerPosition, ManagerPositionConfig
from pkgs.traders.grid.trader import TraderGrid, TraderGridConfig
from pkgs.utils.logging import get_logger_named, set_default_level

from pkgs.utils.webserver import start_web_server

################################################################################


async def main():
    set_default_level(True)
    logger = get_logger_named("main")
    logger.info("Service is starting...")

    ############################################################################

    try:
        exchange_cfg = ExchangeClientConfig()
        exchange = ExchangeClient(exchange_cfg)
    except Exception as e:
        error_msg = (
            f"Exchange client initialization failed: {str(e)}\n{traceback.format_exc()}"
        )
        logger.error(error_msg)
        return

    try:
        position_cfg = ManagerPositionConfig()
        position_manager = ManagerPosition(position_cfg, exchange)

        risk_manager_cfg = ManagerAdvancedRiskConfig()
        risk_manager = ManagerAdvancedRisk(risk_manager_cfg, position_manager)

        actioner_s1_cfg = ActionerS1Config()
        actioner_s1 = ActionerS1(
            actioner_s1_cfg, exchange, position_manager, risk_manager
        )

        order_manager = ManagerOrder()

        trader_cfg = TraderGridConfig()
        trader = TraderGrid(
            trader_cfg,
            exchange,
            position_manager,
            risk_manager,
            actioner_s1,
            order_manager,
        )
        await trader.initialize()

        web_server_task = asyncio.create_task(
            start_web_server(
                trader, exchange, position_manager, actioner_s1, order_manager
            )
        )
        trading_task = asyncio.create_task(trader.main_loop())

        await asyncio.gather(web_server_task, trading_task)

    except Exception as e:
        error_msg = f"Startup failed: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_msg)

    finally:
        if "trader" in locals():
            try:
                await trader.exchange.close()
                logger.info("Exchange connection closed")
            except Exception as e:
                logger.error(f"Error occurred while closing connection: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())
